from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shlex
import socket
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .config import Settings
from .db import connect, write_transaction
from .git_utils import infer_project_identity, resolve_git_info, resolve_project_root
from .projects import project_is_ignored
from .session_rollups import (
    compute_session_rollups,
    replace_session_turn_activity_daily,
)
from .session_insights import extract_agent_metadata, parse_raw_meta_json
from .session_artifacts import load_session_artifact_text, read_session_source_text, store_session_artifact
from .turn_index import replace_session_turn_search, replace_session_turns
from .text_utils import shorten, strip_codex_wrappers

logger = logging.getLogger("codex_session_viewer.importer")


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def current_source_host() -> str:
    return (os.getenv("CODEX_VIEWER_SOURCE_HOST", socket.gethostname()) or "").strip().lower()


def can_probe_git_for_source(source_host: str | None) -> bool:
    candidate = str(source_host or "").strip().lower()
    if not candidate:
        return True
    return candidate == current_source_host()


def first_text_from_content(content: object) -> str:
    if not isinstance(content, list):
        return ""

    fragments: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if isinstance(item.get("text"), str):
            fragments.append(item["text"])
            continue
        if isinstance(item.get("input"), str):
            fragments.append(item["input"])
            continue
        if isinstance(item.get("output"), str):
            fragments.append(item["output"])
            continue
        if isinstance(item.get("content"), str):
            fragments.append(item["content"])

    return "\n\n".join(fragment for fragment in fragments if fragment).strip()


def safe_json(value: object) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True)


def format_command(command: object) -> str:
    if isinstance(command, list):
        return " ".join(shlex.quote(str(part)) for part in command)
    if isinstance(command, str):
        return command
    return ""


def parse_jsonish(value: object) -> object | None:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text[0] not in "[{":
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def friendly_tool_title(tool_name: str | None) -> str:
    mapping = {
        "exec_command": "Shell Command",
        "write_stdin": "Command Session",
        "apply_patch": "Patch",
        "update_plan": "Plan Update",
        "tool_search": "Tool Search",
        "image_generation": "Image Generation",
        "view_image": "View Image",
    }
    return mapping.get((tool_name or "").strip(), tool_name or "Tool Call")


def summarize_tool_call_input(tool_name: str | None, tool_input: object) -> tuple[str, str | None]:
    raw_text = tool_input if isinstance(tool_input, str) else safe_json(tool_input)

    if tool_name == "update_plan":
        parsed = parse_jsonish(tool_input)
        if isinstance(parsed, dict):
            steps = parsed.get("plan")
            explanation = parsed.get("explanation")
            plan_items = [item for item in steps if isinstance(item, dict)] if isinstance(steps, list) else []
            counts: dict[str, int] = {}
            for item in plan_items:
                status = str(item.get("status") or "").strip().lower() or "unknown"
                counts[status] = counts.get(status, 0) + 1
            summary_parts: list[str] = []
            if plan_items:
                summary_parts.append(f"{len(plan_items)} step{'s' if len(plan_items) != 1 else ''}")
            for status in ("in_progress", "completed", "pending"):
                count = counts.get(status)
                if count:
                    summary_parts.append(f"{count} {status.replace('_', ' ')}")
            for status, count in sorted(counts.items()):
                if status not in {"in_progress", "completed", "pending"}:
                    summary_parts.append(f"{count} {status.replace('_', ' ')}")

            current_step = ""
            for item in plan_items:
                if str(item.get("status") or "").strip().lower() == "in_progress":
                    current_step = str(item.get("step") or "").strip()
                    if current_step:
                        break

            lines: list[str] = []
            if isinstance(explanation, str) and explanation.strip():
                lines.append(explanation.strip())
            if summary_parts:
                lines.append(" | ".join(summary_parts))
            if current_step:
                lines.append(f"Current: {current_step}")
            if lines:
                return "\n".join(lines), None

    if tool_name == "exec_command":
        parsed = parse_jsonish(tool_input)
        if isinstance(parsed, dict):
            command_text = format_command(parsed.get("cmd"))
            workdir = parsed.get("workdir")
            workdir_name = Path(workdir).name if isinstance(workdir, str) and workdir.strip() else None
            parts = [command_text or "Shell command"]
            if workdir_name:
                parts.append(f"in {workdir_name}")
            return "\n".join(parts), command_text or None
    if tool_name == "write_stdin":
        parsed = parse_jsonish(tool_input)
        if isinstance(parsed, dict):
            session_id = parsed.get("session_id")
            chars = parsed.get("chars")
            if isinstance(chars, str) and chars:
                label = "Send input to command session"
            else:
                label = "Continue command session"
            if session_id not in (None, ""):
                label = f"{label} {session_id}"
            return label, None

    return raw_text, None


def summarize_token_count(payload: dict[str, object]) -> str:
    info = payload.get("info")
    if not isinstance(info, dict):
        return "Token usage updated"
    totals = info.get("total_token_usage")
    if not isinstance(totals, dict):
        return "Token usage updated"

    total_tokens = totals.get("total_tokens")
    input_tokens = totals.get("input_tokens")
    output_tokens = totals.get("output_tokens")

    parts: list[str] = []
    if isinstance(total_tokens, int):
        parts.append(f"{total_tokens:,} total")
    if isinstance(input_tokens, int):
        parts.append(f"{input_tokens:,} in")
    if isinstance(output_tokens, int):
        parts.append(f"{output_tokens:,} out")
    return "Token usage: " + ", ".join(parts) if parts else "Token usage updated"


def summarize_web_search_action(action: object, query: object = None) -> str:
    if isinstance(action, dict):
        action_type = str(action.get("type") or "").strip()
        if action_type == "search":
            primary_query = str(action.get("query") or query or "").strip()
            if primary_query:
                return primary_query
            queries = action.get("queries")
            if isinstance(queries, list):
                joined = ", ".join(str(item).strip() for item in queries if str(item).strip())
                if joined:
                    return joined
        elif action_type == "open_page":
            url = str(action.get("url") or query or "").strip()
            if url:
                return f"Open {url}"
        elif action_type == "find_in_page":
            pattern = str(action.get("pattern") or "").strip()
            url = str(action.get("url") or "").strip()
            if pattern and url:
                return f"Find {pattern} in {url}"
            if pattern:
                return f"Find {pattern}"
            if url:
                return f"Find in {url}"
    return str(query or "").strip() or "Web search"


@dataclass(slots=True)
class NormalizedEvent:
    event_index: int
    timestamp: str | None
    record_type: str
    payload_type: str | None
    kind: str
    role: str | None
    title: str
    display_text: str
    detail_text: str
    tool_name: str | None
    call_id: str | None
    command_text: str | None
    exit_code: int | None
    record_json: str


@dataclass(slots=True)
class ParsedSession:
    session_id: str
    source_path: Path
    source_root: Path
    file_size: int
    file_mtime_ns: int
    content_sha256: str
    session_timestamp: str | None
    started_at: str | None
    ended_at: str | None
    cwd: str | None
    cwd_name: str
    source_host: str
    originator: str | None
    cli_version: str | None
    source: str | None
    model_provider: str | None
    git_branch: str | None
    git_commit_hash: str | None
    git_repository_url: str | None
    github_remote_url: str | None
    github_org: str | None
    github_repo: str | None
    github_slug: str | None
    forked_from_id: str | None
    agent_nickname: str | None
    agent_role: str | None
    agent_path: str | None
    memory_mode: str | None
    inferred_project_kind: str
    inferred_project_key: str
    inferred_project_label: str
    summary: str
    event_count: int
    user_message_count: int
    assistant_message_count: int
    tool_call_count: int
    rollup_version: int
    turn_count: int
    last_user_message: str
    last_turn_timestamp: str | None
    latest_turn_summary: str | None
    command_failure_count: int
    aborted_turn_count: int
    latest_usage_timestamp: str | None
    latest_input_tokens: int
    latest_cached_input_tokens: int
    latest_output_tokens: int
    latest_reasoning_output_tokens: int
    latest_total_tokens: int
    latest_context_window: int | None
    latest_context_remaining_percent: int | None
    latest_primary_limit_used_percent: float | None
    latest_primary_limit_resets_at: str | None
    latest_secondary_limit_used_percent: float | None
    latest_secondary_limit_resets_at: str | None
    latest_rate_limit_name: str | None
    latest_rate_limit_reached_type: str | None
    import_warning: str | None
    search_text: str
    raw_meta_json: str
    imported_at: str
    updated_at: str
    events: list[NormalizedEvent]
    raw_artifact_sha256: str | None = None


@dataclass(slots=True)
class SessionPreScan:
    session_format: str
    session_id: str
    cwd: str | None
    inferred_project_kind: str
    inferred_project_key: str
    inferred_project_label: str


class SessionParseError(ValueError):
    def __init__(
        self,
        source_path: Path,
        message: str,
        *,
        line_number: int | None = None,
        line_preview: str | None = None,
    ) -> None:
        detail = message
        if line_number is not None:
            detail = f"line {line_number}: {detail}"
        if line_preview:
            detail = f"{detail} [{line_preview}]"
        super().__init__(f"{source_path}: {detail}")


class SessionSkipError(SessionParseError):
    """Raised for recognized session files that should not be imported."""


def normalize_event(record: dict[str, object], event_index: int) -> NormalizedEvent | None:
    record_type = str(record.get("type") or "")
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    payload_type = payload.get("type") if isinstance(payload.get("type"), str) else None
    timestamp = record.get("timestamp") if isinstance(record.get("timestamp"), str) else None

    kind = "system"
    role = None
    title = payload_type or record_type or "event"
    display_text = ""
    detail_text = ""
    tool_name = None
    call_id = None
    command_text = None
    exit_code = None

    if record_type == "response_item" and payload_type == "message":
        role = payload.get("role") if isinstance(payload.get("role"), str) else None
        phase = payload.get("phase") if isinstance(payload.get("phase"), str) else None
        kind = "message" if role in {"user", "assistant"} else "system"
        if role == "assistant":
            if phase == "commentary":
                title = "Assistant Update"
            elif phase == "final_answer":
                title = "Final Answer"
            else:
                title = "Assistant Message"
        elif role == "user":
            title = "User Message"
        elif role:
            title = f"{role.title()} Message"
        else:
            title = "Message"
        display_text = first_text_from_content(payload.get("content"))
    elif record_type == "event_msg" and payload_type == "user_message":
        role = "user"
        kind = "message"
        title = "User Message"
        display_text = str(payload.get("message") or "")
    elif record_type == "event_msg" and payload_type == "agent_message":
        role = "assistant"
        kind = "message"
        title = "Assistant Update"
        display_text = str(payload.get("message") or "")
    elif record_type == "response_item" and payload_type == "local_shell_call":
        kind = "tool_call"
        tool_name = "exec_command"
        call_id = (
            payload.get("call_id")
            if isinstance(payload.get("call_id"), str)
            else (payload.get("id") if isinstance(payload.get("id"), str) else None)
        )
        title = friendly_tool_title(tool_name)
        action = payload.get("action") if isinstance(payload.get("action"), dict) else {}
        detail_payload = action if action else payload
        detail_text = safe_json(detail_payload)
        if action.get("type") == "exec":
            raw_input = {
                "cmd": action.get("command"),
                "workdir": action.get("working_directory"),
            }
            display_text, command_text = summarize_tool_call_input(tool_name, raw_input)
            command_text = command_text or format_command(action.get("command"))
        else:
            display_text = "Shell command"
            command_text = None
    elif record_type == "response_item" and payload_type in {"function_call", "custom_tool_call"}:
        kind = "tool_call"
        tool_name = payload.get("name") if isinstance(payload.get("name"), str) else None
        call_id = payload.get("call_id") if isinstance(payload.get("call_id"), str) else None
        title = friendly_tool_title(tool_name)
        if payload_type == "function_call":
            raw_input = payload.get("arguments")
            detail_text = str(raw_input or "")
        else:
            raw_input = payload.get("input")
            detail_text = safe_json(raw_input)
        display_text, command_text = summarize_tool_call_input(tool_name, raw_input)
    elif record_type == "response_item" and payload_type in {"function_call_output", "custom_tool_call_output"}:
        kind = "tool_result"
        call_id = payload.get("call_id") if isinstance(payload.get("call_id"), str) else None
        title = "Tool Result"
        output = payload.get("output")
        display_text = output if isinstance(output, str) else safe_json(output)
    elif record_type == "response_item" and payload_type == "tool_search_call":
        kind = "tool_call"
        tool_name = "tool_search"
        call_id = payload.get("call_id") if isinstance(payload.get("call_id"), str) else None
        title = friendly_tool_title(tool_name)
        execution = payload.get("execution")
        arguments = payload.get("arguments")
        detail_text = safe_json(
            {
                "execution": execution,
                "arguments": arguments,
            }
        )
        display_text = str(execution or "Tool search")
    elif record_type == "response_item" and payload_type == "tool_search_output":
        kind = "tool_result"
        call_id = payload.get("call_id") if isinstance(payload.get("call_id"), str) else None
        title = "Tool Search Result"
        tools = payload.get("tools") if isinstance(payload.get("tools"), list) else []
        status = payload.get("status")
        execution = payload.get("execution")
        summary_bits = [str(execution or "").strip(), str(status or "").strip()]
        summary = " | ".join(bit for bit in summary_bits if bit)
        display_text = summary or f"{len(tools)} tool result{'s' if len(tools) != 1 else ''}"
        detail_text = safe_json(payload)
    elif record_type == "response_item" and payload_type == "image_generation_call":
        kind = "tool_result"
        call_id = payload.get("id") if isinstance(payload.get("id"), str) else None
        title = "Image Generation"
        status = str(payload.get("status") or "").strip()
        revised_prompt = str(payload.get("revised_prompt") or "").strip()
        saved_path = str(payload.get("saved_path") or "").strip()
        display_text = revised_prompt or status or "Image generation completed"
        detail_lines = []
        if status:
            detail_lines.append(f"Status: {status}")
        if revised_prompt:
            detail_lines.append(revised_prompt)
        if saved_path:
            detail_lines.append(f"Saved: {saved_path}")
        detail_text = "\n".join(detail_lines)
    elif record_type == "event_msg" and payload_type == "exec_command_end":
        kind = "command"
        title = "Command Result"
        call_id = payload.get("call_id") if isinstance(payload.get("call_id"), str) else None
        command_text = format_command(payload.get("command"))
        exit_value = payload.get("exit_code")
        exit_code = exit_value if isinstance(exit_value, int) else None
        display_text = command_text or "Shell command completed"
        detail_text = str(payload.get("aggregated_output") or "")
    elif record_type == "event_msg" and payload_type == "patch_apply_end":
        kind = "system"
        title = "Patch Apply"
        call_id = payload.get("call_id") if isinstance(payload.get("call_id"), str) else None
        success = payload.get("success")
        status = payload.get("status")
        display_text = f"Status: {status or 'unknown'}"
        if isinstance(success, bool):
            display_text += " (success)" if success else " (failed)"
        detail_text = safe_json(payload.get("changes"))
    elif record_type == "event_msg" and payload_type == "image_generation_end":
        kind = "tool_result"
        title = "Image Generation"
        call_id = payload.get("call_id") if isinstance(payload.get("call_id"), str) else None
        status = str(payload.get("status") or "").strip()
        revised_prompt = str(payload.get("revised_prompt") or "").strip()
        saved_path = str(payload.get("saved_path") or "").strip()
        display_text = revised_prompt or status or "Image generation completed"
        detail_lines = []
        if status:
            detail_lines.append(f"Status: {status}")
        if revised_prompt:
            detail_lines.append(revised_prompt)
        if saved_path:
            detail_lines.append(f"Saved: {saved_path}")
        detail_text = "\n".join(detail_lines)
    elif record_type == "response_item" and payload_type == "reasoning":
        kind = "reasoning"
        title = "Reasoning"
        summary = payload.get("summary")
        if isinstance(summary, list):
            display_text = "\n".join(str(item) for item in summary if item)
        elif isinstance(summary, str):
            display_text = summary
        detail_text = display_text
    elif record_type == "event_msg" and payload_type == "token_count":
        kind = "telemetry"
        title = "Token Usage"
        display_text = summarize_token_count(payload)
        detail_text = safe_json(payload.get("info"))
    elif record_type == "turn_context":
        kind = "context"
        title = "Turn Context"
        model = payload.get("model")
        cwd = payload.get("cwd")
        parts = []
        if isinstance(model, str):
            parts.append(model)
        if isinstance(cwd, str):
            parts.append(cwd)
        display_text = " | ".join(parts) if parts else "Turn context updated"
        detail_text = safe_json(payload)
    elif record_type == "event_msg" and payload_type in {"task_started", "turn_started"}:
        kind = "system"
        title = "Task Started"
        display_text = str(payload.get("started_at") or "Task started")
    elif record_type == "event_msg" and payload_type in {"task_complete", "turn_complete"}:
        kind = "system"
        title = "Task Complete"
        display_text = shorten(str(payload.get("last_agent_message") or "Task completed"), 320)
    elif record_type == "event_msg" and payload_type == "context_compacted":
        kind = "system"
        title = "Context Compacted"
        display_text = "Conversation history compacted"
    elif record_type == "event_msg" and payload_type == "thread_rolled_back":
        kind = "system"
        title = "Thread Rolled Back"
        count = payload.get("num_turns")
        if isinstance(count, int):
            display_text = f"Rolled back {count} turn{'s' if count != 1 else ''}"
        else:
            display_text = "Conversation history rolled back"
    elif record_type == "event_msg" and payload_type == "item_completed":
        kind = "system"
        item = payload.get("item") if isinstance(payload.get("item"), dict) else {}
        item_type = item.get("type") if isinstance(item.get("type"), str) else None
        item_text = item.get("text") if isinstance(item.get("text"), str) else None
        title = "Plan Completed" if item_type == "Plan" else "Item Completed"
        display_text = shorten(item_text or item_type or "Item completed", 320)
        detail_text = item_text or safe_json(item)
    elif record_type == "event_msg" and payload_type == "web_search_end":
        kind = "tool_result"
        title = "Web Search"
        tool_name = "web_search"
        call_id = payload.get("call_id") if isinstance(payload.get("call_id"), str) else None
        query = payload.get("query")
        action = payload.get("action")
        display_text = summarize_web_search_action(action, query)
        detail_text = safe_json(payload)
    elif record_type == "response_item" and payload_type == "web_search_call":
        kind = "tool_call"
        tool_name = "web_search"
        title = "Web Search"
        action = payload.get("action")
        display_text = summarize_web_search_action(action)
        detail_text = safe_json(payload)
    elif record_type == "response_item" and payload_type == "compaction":
        kind = "system"
        title = "Context Compacted"
        display_text = "Conversation history compacted"
    elif record_type == "compacted":
        kind = "system"
        title = "Context Compacted"
        display_text = str(payload.get("message") or "Conversation history compacted")
    else:
        kind = "system"
        title = title.replace("_", " ").title()
        display_text = str(payload.get("message") or payload.get("reason") or title)
        detail_text = safe_json(payload)

    if not detail_text and display_text:
        detail_text = display_text
    if not display_text:
        display_text = title

    return NormalizedEvent(
        event_index=event_index,
        timestamp=timestamp,
        record_type=record_type,
        payload_type=payload_type,
        kind=kind,
        role=role,
        title=title,
        display_text=display_text.strip(),
        detail_text=detail_text.strip(),
        tool_name=tool_name,
        call_id=call_id,
        command_text=command_text,
        exit_code=exit_code,
        record_json=safe_json(record),
    )


def normalize_jsonl_line(line: str, line_number: int) -> str | None:
    normalized = line
    if line_number == 1:
        normalized = normalized.lstrip("\ufeff")
    normalized = normalized.rstrip("\r\n")
    if not normalized.strip(" \t\x00"):
        return None
    return normalized.lstrip("\x00")


CLAUDE_TRANSCRIPT_RECORD_TYPES = {
    "user",
    "assistant",
    "system",
    "attachment",
    "progress",
    "summary",
    "custom-title",
    "ai-title",
    "last-prompt",
    "task-summary",
    "tag",
    "agent-name",
    "agent-color",
    "agent-setting",
    "pr-link",
    "file-history-snapshot",
    "attribution-snapshot",
    "mode",
    "worktree-state",
    "content-replacement",
    "marble-origami-commit",
    "marble-origami-snapshot",
}

CLAUDE_COMMAND_TOOL_NAMES = {"Bash", "PowerShell"}

CLAUDE_SERVER_TOOL_USE_TYPES = {"server_tool_use", "mcp_tool_use"}

CLAUDE_SERVER_TOOL_RESULT_TYPES = {
    "advisor_tool_result",
    "code_execution_tool_result",
    "mcp_tool_result",
    "tool_search_tool_result",
    "web_fetch_tool_result",
    "web_search_tool_result",
    "bash_code_execution_tool_result",
    "text_editor_code_execution_tool_result",
}

CLAUDE_PERSISTED_OUTPUT_MAX_CHARS = 200_000
CLAUDE_PERSISTED_OUTPUT_PATH_RE = re.compile(r"Full output saved to:\s*(?P<path>[^\n<]+)")


def _parse_json_record(
    normalized_line: str,
    source_path: Path,
    *,
    line_number: int,
) -> dict[str, object]:
    try:
        record = json.loads(normalized_line)
    except json.JSONDecodeError as exc:
        raise SessionParseError(
            source_path,
            exc.msg,
            line_number=line_number,
            line_preview=shorten(normalized_line.replace("\n", " "), 120),
        ) from exc
    if not isinstance(record, dict):
        raise SessionParseError(
            source_path,
            "record must be a JSON object",
            line_number=line_number,
            line_preview=shorten(normalized_line.replace("\n", " "), 120),
        )
    return record


def _append_normalized_event(
    events: list[NormalizedEvent],
    record: dict[str, object],
) -> NormalizedEvent | None:
    normalized = normalize_event(record, len(events))
    if normalized is None:
        return None
    events.append(normalized)
    return normalized


def _assistant_usage_payload(message: dict[str, object]) -> dict[str, object] | None:
    usage = message.get("usage") if isinstance(message.get("usage"), dict) else {}
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    cache_read_input_tokens = usage.get("cache_read_input_tokens")
    cache_creation_input_tokens = usage.get("cache_creation_input_tokens")

    if not any(
        isinstance(value, (int, float))
        for value in (
            input_tokens,
            output_tokens,
            cache_read_input_tokens,
            cache_creation_input_tokens,
        )
    ):
        return None

    coerced_input_tokens = int(input_tokens) if isinstance(input_tokens, (int, float)) else 0
    coerced_output_tokens = int(output_tokens) if isinstance(output_tokens, (int, float)) else 0
    cached_input_tokens = int(cache_read_input_tokens) if isinstance(cache_read_input_tokens, (int, float)) else 0
    total_tokens = coerced_input_tokens + coerced_output_tokens
    if isinstance(cache_creation_input_tokens, (int, float)):
        total_tokens += int(cache_creation_input_tokens)

    return {
        "type": "token_count",
        "info": {
            "total_token_usage": {
                "input_tokens": coerced_input_tokens,
                "cached_input_tokens": cached_input_tokens,
                "output_tokens": coerced_output_tokens,
                "reasoning_output_tokens": 0,
                "total_tokens": total_tokens,
            }
        },
    }


def _claude_output_text(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str) and item.strip():
                parts.append(item.strip())
                continue
            if not isinstance(item, dict):
                continue
            for key in ("text", "content", "output", "message", "result"):
                field_value = item.get(key)
                if isinstance(field_value, str) and field_value.strip():
                    parts.append(field_value.strip())
                    break
        return "\n".join(parts).strip()
    return ""


def _claude_content_blocks(value: object) -> list[dict[str, object]]:
    if isinstance(value, str):
        return [{"type": "text", "text": value}]
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _claude_block_identity(block: dict[str, object]) -> tuple[str, str] | None:
    block_type = str(block.get("type") or "").strip()
    if not block_type:
        return None
    if block_type in {"text", "thinking", "redacted_thinking"}:
        return None
    if block_type in {"tool_use", *CLAUDE_SERVER_TOOL_USE_TYPES}:
        block_id = block.get("id")
        if isinstance(block_id, str) and block_id.strip():
            return (block_type, block_id.strip())
    if (
        block_type in CLAUDE_SERVER_TOOL_RESULT_TYPES
        or (block_type.endswith("_tool_result") and isinstance(block.get("tool_use_id"), str))
    ):
        tool_use_id = block.get("tool_use_id")
        if isinstance(tool_use_id, str) and tool_use_id.strip():
            return (block_type, tool_use_id.strip())
    return (block_type, safe_json(block))


def _merge_claude_textlike_block(
    merged_blocks: list[dict[str, object]],
    block: dict[str, object],
) -> bool:
    block_type = str(block.get("type") or "").strip()
    if block_type == "text":
        field = "text"
    elif block_type in {"thinking", "redacted_thinking"}:
        field = "thinking"
    else:
        return False

    candidate = block.get(field)
    if not isinstance(candidate, str) or not candidate:
        return False

    for index in range(len(merged_blocks) - 1, -1, -1):
        existing = merged_blocks[index]
        if str(existing.get("type") or "").strip() != block_type:
            continue
        existing_value = existing.get(field)
        if not isinstance(existing_value, str) or not existing_value:
            continue
        if existing_value == candidate or candidate.startswith(existing_value):
            merged_blocks[index] = dict(block)
            return True
        if existing_value.startswith(candidate):
            return True
        merged_blocks[index] = dict(block)
        return True

    merged_blocks.append(dict(block))
    return True


def _merge_claude_content_blocks(
    existing_blocks: list[dict[str, object]],
    new_blocks: list[dict[str, object]],
) -> list[dict[str, object]]:
    merged_blocks = [dict(block) for block in existing_blocks]
    for block in new_blocks:
        if _merge_claude_textlike_block(merged_blocks, block):
            continue
        identity = _claude_block_identity(block)
        if identity is not None:
            matched = False
            for index, existing in enumerate(merged_blocks):
                if _claude_block_identity(existing) != identity:
                    continue
                merged_blocks[index] = dict(block)
                matched = True
                break
            if matched:
                continue
        if any(existing == block for existing in merged_blocks):
            continue
        merged_blocks.append(dict(block))
    return merged_blocks


def _merge_claude_assistant_records(
    existing_record: dict[str, object],
    fragment_record: dict[str, object],
) -> dict[str, object]:
    merged_record = dict(existing_record)
    for key, value in fragment_record.items():
        if key == "message":
            continue
        if value is not None:
            merged_record[key] = value

    existing_message = (
        dict(existing_record.get("message"))
        if isinstance(existing_record.get("message"), dict)
        else {}
    )
    fragment_message = (
        dict(fragment_record.get("message"))
        if isinstance(fragment_record.get("message"), dict)
        else {}
    )
    merged_message = dict(existing_message)

    existing_blocks = _claude_content_blocks(existing_message.get("content"))
    fragment_blocks = _claude_content_blocks(fragment_message.get("content"))
    if existing_blocks or fragment_blocks:
        merged_message["content"] = _merge_claude_content_blocks(existing_blocks, fragment_blocks)

    existing_stop_reason = existing_message.get("stop_reason")
    fragment_stop_reason = fragment_message.get("stop_reason")
    fragment_has_final_stop = isinstance(fragment_stop_reason, str) and fragment_stop_reason.strip()
    existing_has_final_stop = isinstance(existing_stop_reason, str) and existing_stop_reason.strip()

    for key, value in fragment_message.items():
        if key == "content":
            continue
        if key == "usage":
            if not isinstance(value, dict):
                continue
            if fragment_has_final_stop or not isinstance(merged_message.get("usage"), dict) or not existing_has_final_stop:
                merged_message["usage"] = value
            continue
        if key == "stop_reason":
            if fragment_has_final_stop:
                merged_message[key] = fragment_stop_reason
            elif "stop_reason" not in merged_message:
                merged_message[key] = fragment_stop_reason
            continue
        if value is not None:
            merged_message[key] = value

    merged_record["message"] = merged_message
    return merged_record


def _extract_claude_persisted_output_path(value: object) -> str | None:
    if isinstance(value, dict):
        direct = value.get("persistedOutputPath")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        for key in ("content", "stdout", "stderr", "output", "message", "result"):
            resolved = _extract_claude_persisted_output_path(value.get(key))
            if resolved:
                return resolved
        return None
    if isinstance(value, list):
        for item in value:
            resolved = _extract_claude_persisted_output_path(item)
            if resolved:
                return resolved
        return None
    if isinstance(value, str):
        match = CLAUDE_PERSISTED_OUTPUT_PATH_RE.search(value)
        if match is not None:
            candidate = match.group("path").strip()
            return candidate or None
    return None


def _resolve_claude_persisted_output_path(path_text: str | None, source_path: Path) -> Path | None:
    if not path_text:
        return None
    raw_path = Path(path_text).expanduser()
    candidates = [raw_path]
    if not raw_path.is_absolute():
        candidates.append((source_path.parent / raw_path).expanduser())
        candidates.append((source_path.parent / raw_path.name).expanduser())
    seen: set[str] = set()
    for candidate in candidates:
        candidate_key = str(candidate)
        if candidate_key in seen:
            continue
        seen.add(candidate_key)
        if candidate.is_file():
            return candidate
    return None


def _read_claude_persisted_output(path_text: str | None, source_path: Path) -> str | None:
    resolved = _resolve_claude_persisted_output_path(path_text, source_path)
    if resolved is None:
        return None
    try:
        with resolved.open("r", encoding="utf-8", errors="replace") as handle:
            content = handle.read(CLAUDE_PERSISTED_OUTPUT_MAX_CHARS + 1)
    except OSError:
        return None
    normalized = content.strip()
    if not normalized:
        return None
    if len(normalized) > CLAUDE_PERSISTED_OUTPUT_MAX_CHARS:
        truncated = normalized[:CLAUDE_PERSISTED_OUTPUT_MAX_CHARS].rstrip()
        return f"{truncated}\n\n[Persisted output truncated from {resolved}]"
    return normalized


def _claude_materialize_tool_output(value: object, source_path: Path) -> object:
    persisted_text = _read_claude_persisted_output(
        _extract_claude_persisted_output_path(value),
        source_path,
    )
    if persisted_text:
        return persisted_text
    flattened = _claude_output_text(value)
    if flattened:
        return flattened
    return value


def _claude_display_output(value: object, source_path: Path) -> str:
    materialized = _claude_materialize_tool_output(value, source_path)
    if isinstance(materialized, str):
        return materialized.strip()
    value = materialized
    if isinstance(value, dict):
        parts: list[str] = []
        for key in ("stdout", "stderr", "output", "content", "message", "result", "returnCodeInterpretation"):
            field_value = value.get(key)
            if isinstance(field_value, str) and field_value.strip():
                parts.append(field_value.strip())
        persisted_output_path = value.get("persistedOutputPath")
        if isinstance(persisted_output_path, str) and persisted_output_path.strip():
            parts.append(f"Full output saved to {persisted_output_path.strip()}")
        if parts:
            return "\n".join(parts)
    return safe_json(value).strip()


def _claude_command_exit_code(block: dict[str, object], output: object) -> int:
    if isinstance(output, dict) and isinstance(output.get("interrupted"), bool) and output.get("interrupted"):
        return 130
    if isinstance(block.get("is_error"), bool):
        return 1 if block["is_error"] else 0
    return 0


def _normalize_claude_tool_name(tool_name: str | None) -> str | None:
    normalized = str(tool_name or "").strip()
    if not normalized:
        return None
    if normalized in CLAUDE_COMMAND_TOOL_NAMES:
        return "exec_command"
    return normalized


def _claude_record_type(record: dict[str, object]) -> str:
    return str(record.get("type") or "").strip()


def _claude_relative_source_path(source_path: Path, source_root: Path) -> str:
    try:
        return source_path.relative_to(source_root).as_posix()
    except ValueError:
        return source_path.as_posix()


def _claude_source_path_agent_id(source_path: Path) -> str | None:
    stem = source_path.stem.strip()
    if stem.startswith("agent-") and len(stem) > len("agent-"):
        return stem[len("agent-") :]
    return None


def _claude_parent_session_from_path(source_path: Path, source_root: Path) -> str | None:
    try:
        relative = source_path.relative_to(source_root)
    except ValueError:
        return None
    lowered_parts = [part.lower() for part in relative.parts]
    if "subagents" not in lowered_parts:
        return None
    subagents_index = lowered_parts.index("subagents")
    if subagents_index <= 0:
        return None
    candidate = relative.parts[subagents_index - 1].strip()
    return candidate or None


def _claude_sidechain_session_id(
    root_session_id: str,
    agent_id: str | None,
    source_path: Path,
    source_root: Path,
) -> str:
    if agent_id:
        return f"{root_session_id}:agent:{agent_id}"
    relative = _claude_relative_source_path(source_path, source_root)
    normalized = relative.removesuffix(".jsonl").replace("/", ":")
    return f"{root_session_id}:sidechain:{normalized}"


def _is_claude_warmup_transcript(events: list[NormalizedEvent], is_sidechain: bool) -> bool:
    if not is_sidechain:
        return False
    meaningful_events = [event for event in events if event.kind in {"message", "tool_call", "tool_result", "command"}]
    if len(meaningful_events) != 1:
        return False
    only_event = meaningful_events[0]
    if only_event.kind != "message" or only_event.role != "user":
        return False
    return strip_codex_wrappers(only_event.display_text).strip() == "Warmup"


def _is_claude_transcript_record(record: dict[str, object]) -> bool:
    record_type = _claude_record_type(record)
    if record_type in CLAUDE_TRANSCRIPT_RECORD_TYPES:
        return True
    return any(
        key in record
        for key in ("sessionId", "parentUuid", "logicalParentUuid", "isSidechain", "userType", "version")
    )


def _detect_session_format(
    lines: Iterable[str],
    source_path: Path,
) -> str:
    for idx, line in enumerate(lines):
        normalized_line = normalize_jsonl_line(line, idx + 1)
        if normalized_line is None:
            continue
        record = _parse_json_record(normalized_line, source_path, line_number=idx + 1)
        record_type = str(record.get("type") or "").strip()
        if record_type == "session_meta":
            return "codex"
        if record_type in {"event_msg", "response_item", "turn_context", "compacted"} or "payload" in record:
            return "codex"
        if _is_claude_transcript_record(record):
            return "claude"
        break
    return "codex"


def _build_session_prescan(
    *,
    session_format: str,
    session_id: str,
    cwd: str | None,
    raw_git: dict[str, object] | None,
    source_host: str,
) -> SessionPreScan:
    git_info = resolve_git_info(
        cwd,
        raw_git,
        allow_probe=can_probe_git_for_source(source_host),
    )
    project_root = resolve_project_root(
        cwd,
        allow_probe=can_probe_git_for_source(source_host),
    )
    project_identity = infer_project_identity(
        source_host=source_host,
        cwd=cwd,
        github_org=git_info["github_org"],
        github_repo=git_info["github_repo"],
        github_slug=git_info["github_slug"],
        git_repository_url=git_info["repository_url"],
        project_root=project_root,
    )
    return SessionPreScan(
        session_format=session_format,
        session_id=session_id,
        cwd=cwd,
        inferred_project_kind=str(project_identity["kind"]),
        inferred_project_key=str(project_identity["key"]),
        inferred_project_label=str(project_identity["label"]),
    )


def prescan_session_source(
    source_path: Path,
    source_root: Path,
    source_host: str,
    *,
    max_lines: int = 128,
    max_bytes: int = 131_072,
) -> SessionPreScan | None:
    del source_root  # reserved for future pre-scan routing if file layout matters

    line_count = 0
    byte_count = 0
    with source_path.open("r", encoding="utf-8", newline="") as handle:
        for raw_line in handle:
            line_count += 1
            byte_count += len(raw_line.encode("utf-8"))
            normalized_line = normalize_jsonl_line(raw_line, line_count)
            if normalized_line is None:
                if line_count >= max_lines or byte_count >= max_bytes:
                    break
                continue

            record = _parse_json_record(normalized_line, source_path, line_number=line_count)
            record_type = str(record.get("type") or "").strip()

            if record_type == "session_meta" and isinstance(record.get("payload"), dict):
                payload = dict(record["payload"])
                session_id = str(payload.get("id") or source_path.stem).strip() or source_path.stem
                cwd = payload.get("cwd") if isinstance(payload.get("cwd"), str) else None
                raw_git = payload.get("git") if isinstance(payload.get("git"), dict) else None
                return _build_session_prescan(
                    session_format="codex",
                    session_id=session_id,
                    cwd=cwd,
                    raw_git=raw_git,
                    source_host=source_host,
                )

            if _is_claude_transcript_record(record):
                session_id = str(record.get("sessionId") or source_path.stem).strip() or source_path.stem
                cwd = record.get("cwd") if isinstance(record.get("cwd"), str) else None
                return _build_session_prescan(
                    session_format="claude",
                    session_id=session_id,
                    cwd=cwd,
                    raw_git=None,
                    source_host=source_host,
                )

            if line_count >= max_lines or byte_count >= max_bytes:
                break

    return None


def _finalize_parsed_session(
    *,
    source_path: Path,
    source_root: Path,
    source_host: str,
    file_size: int,
    file_mtime_ns: int,
    content_sha256: str,
    raw_meta: dict[str, object],
    started_at: str | None,
    ended_at: str | None,
    events: list[NormalizedEvent],
    imported_at: str,
    warning: str | None = None,
) -> ParsedSession:
    user_messages: list[str] = []
    assistant_messages: list[str] = []
    search_parts: list[str] = []

    for normalized in events:
        if normalized.kind == "message" and normalized.role == "user" and normalized.display_text:
            cleaned_prompt = strip_codex_wrappers(normalized.display_text)
            if normalized.record_type == "event_msg":
                user_messages.append(cleaned_prompt or normalized.display_text)
            elif cleaned_prompt and not user_messages:
                user_messages.append(cleaned_prompt)
        if normalized.kind == "message" and normalized.role == "assistant" and normalized.display_text:
            assistant_messages.append(normalized.display_text)
        if normalized.display_text:
            search_parts.append(normalized.display_text)

    session_id = str(raw_meta.get("id") or source_path.stem)
    session_timestamp = raw_meta.get("timestamp") if isinstance(raw_meta.get("timestamp"), str) else started_at
    cwd = raw_meta.get("cwd") if isinstance(raw_meta.get("cwd"), str) else None
    cwd_name = Path(cwd).name if cwd else ""
    originator = raw_meta.get("originator") if isinstance(raw_meta.get("originator"), str) else None
    cli_version = raw_meta.get("cli_version") if isinstance(raw_meta.get("cli_version"), str) else None
    source = raw_meta.get("source") if isinstance(raw_meta.get("source"), str) else None
    model_provider = raw_meta.get("model_provider") if isinstance(raw_meta.get("model_provider"), str) else None
    git_info = resolve_git_info(
        cwd,
        raw_meta.get("git") if isinstance(raw_meta.get("git"), dict) else None,
        allow_probe=can_probe_git_for_source(source_host),
    )
    project_root = resolve_project_root(
        cwd,
        allow_probe=can_probe_git_for_source(source_host),
    )
    project_identity = infer_project_identity(
        source_host=source_host,
        cwd=cwd,
        github_org=git_info["github_org"],
        github_repo=git_info["github_repo"],
        github_slug=git_info["github_slug"],
        git_repository_url=git_info["repository_url"],
        project_root=project_root,
    )

    summary = shorten(user_messages[0], 120) if user_messages else f"Session {session_id}"
    if cwd and not user_messages:
        summary = f"{Path(cwd).name}: {summary}"

    search_head = [
        summary,
        cwd or "",
        cwd_name,
        source_host,
        cli_version or "",
        model_provider or "",
        project_root or "",
        git_info["github_org"] or "",
        git_info["github_repo"] or "",
        git_info["github_slug"] or "",
        git_info["github_remote_url"] or "",
        git_info["repository_url"] or "",
    ]
    search_text = "\n".join(part for part in [*search_head, *search_parts] if part)
    if len(search_text) > 200_000:
        warning = warning or "Search text truncated during import"
        search_text = search_text[:200_000]
    rollups = compute_session_rollups(events)
    agent_metadata = extract_agent_metadata(raw_meta)

    return ParsedSession(
        session_id=session_id,
        source_path=source_path,
        source_root=source_root,
        file_size=file_size,
        file_mtime_ns=file_mtime_ns,
        content_sha256=content_sha256,
        session_timestamp=session_timestamp,
        started_at=started_at,
        ended_at=ended_at,
        cwd=cwd,
        cwd_name=cwd_name,
        source_host=source_host,
        originator=originator,
        cli_version=cli_version,
        source=source,
        model_provider=model_provider,
        git_branch=git_info["branch"],
        git_commit_hash=git_info["commit_hash"],
        git_repository_url=git_info["repository_url"],
        github_remote_url=git_info["github_remote_url"],
        github_org=git_info["github_org"],
        github_repo=git_info["github_repo"],
        github_slug=git_info["github_slug"],
        forked_from_id=agent_metadata["forked_from_id"],
        agent_nickname=agent_metadata["agent_nickname"],
        agent_role=agent_metadata["agent_role"],
        agent_path=agent_metadata["agent_path"],
        memory_mode=agent_metadata["memory_mode"],
        inferred_project_kind=project_identity["kind"],
        inferred_project_key=project_identity["key"],
        inferred_project_label=project_identity["label"],
        summary=summary,
        event_count=len(events),
        user_message_count=len(user_messages),
        assistant_message_count=len(assistant_messages),
        tool_call_count=sum(1 for event in events if event.kind == "tool_call"),
        rollup_version=int(rollups["rollup_version"]),
        turn_count=int(rollups["turn_count"]),
        last_user_message=str(rollups["last_user_message"] or ""),
        last_turn_timestamp=rollups["last_turn_timestamp"],
        latest_turn_summary=rollups["latest_turn_summary"],
        command_failure_count=int(rollups["command_failure_count"]),
        aborted_turn_count=int(rollups["aborted_turn_count"]),
        latest_usage_timestamp=rollups["latest_usage_timestamp"],
        latest_input_tokens=int(rollups["latest_input_tokens"] or 0),
        latest_cached_input_tokens=int(rollups["latest_cached_input_tokens"] or 0),
        latest_output_tokens=int(rollups["latest_output_tokens"] or 0),
        latest_reasoning_output_tokens=int(rollups["latest_reasoning_output_tokens"] or 0),
        latest_total_tokens=int(rollups["latest_total_tokens"] or 0),
        latest_context_window=(
            int(rollups["latest_context_window"])
            if rollups["latest_context_window"] is not None
            else None
        ),
        latest_context_remaining_percent=(
            int(rollups["latest_context_remaining_percent"])
            if rollups["latest_context_remaining_percent"] is not None
            else None
        ),
        latest_primary_limit_used_percent=(
            float(rollups["latest_primary_limit_used_percent"])
            if rollups["latest_primary_limit_used_percent"] is not None
            else None
        ),
        latest_primary_limit_resets_at=rollups["latest_primary_limit_resets_at"],
        latest_secondary_limit_used_percent=(
            float(rollups["latest_secondary_limit_used_percent"])
            if rollups["latest_secondary_limit_used_percent"] is not None
            else None
        ),
        latest_secondary_limit_resets_at=rollups["latest_secondary_limit_resets_at"],
        latest_rate_limit_name=rollups["latest_rate_limit_name"],
        latest_rate_limit_reached_type=rollups["latest_rate_limit_reached_type"],
        import_warning=warning,
        search_text=search_text,
        raw_meta_json=safe_json(raw_meta),
        imported_at=imported_at,
        updated_at=imported_at,
        events=events,
    )


def _parse_session_lines(
    lines: Iterable[str],
    source_path: Path,
    source_root: Path,
    source_host: str,
    *,
    file_size: int,
    file_mtime_ns: int,
) -> ParsedSession:
    imported_at = utc_now_iso()
    content_hash = hashlib.sha256()
    raw_meta: dict[str, object] | None = None
    events: list[NormalizedEvent] = []
    started_at: str | None = None
    ended_at: str | None = None

    for idx, line in enumerate(lines):
        line_number = idx + 1
        normalized_line = normalize_jsonl_line(line, line_number)
        if normalized_line is None:
            continue
        content_hash.update(normalized_line.encode("utf-8"))
        record = _parse_json_record(normalized_line, source_path, line_number=line_number)
        record_type = record.get("type")
        if record_type == "session_meta" and isinstance(record.get("payload"), dict):
            raw_meta = record["payload"]
            started_at = raw_meta.get("timestamp") if isinstance(raw_meta.get("timestamp"), str) else started_at
            continue

        normalized = normalize_event(record, idx)
        if normalized is None:
            continue

        ended_at = normalized.timestamp or ended_at
        events.append(normalized)

    if raw_meta is None:
        raise SessionParseError(source_path, "does not contain a session_meta record")

    return _finalize_parsed_session(
        source_path=source_path,
        source_root=source_root,
        source_host=source_host,
        file_size=file_size,
        file_mtime_ns=file_mtime_ns,
        content_sha256=content_hash.hexdigest(),
        raw_meta=raw_meta,
        started_at=started_at,
        ended_at=ended_at,
        events=events,
        imported_at=imported_at,
    )


def _parse_claude_session_lines(
    lines: Iterable[str],
    source_path: Path,
    source_root: Path,
    source_host: str,
    *,
    file_size: int,
    file_mtime_ns: int,
) -> ParsedSession:
    imported_at = utc_now_iso()
    content_hash = hashlib.sha256()
    events: list[NormalizedEvent] = []
    started_at: str | None = None
    ended_at: str | None = None

    root_session_id = source_path.stem
    cwd: str | None = None
    user_type: str | None = None
    cli_version: str | None = None
    model_name: str | None = None
    custom_title: str | None = None
    agent_name: str | None = None
    agent_setting: str | None = None
    pr_url: str | None = None
    pr_repository: str | None = None
    mode: str | None = None
    team_name: str | None = None
    transcript_agent_id = _claude_source_path_agent_id(source_path)
    parent_session_id_from_path = _claude_parent_session_from_path(source_path, source_root)
    is_sidechain = transcript_agent_id is not None or parent_session_id_from_path is not None

    tool_calls: dict[str, dict[str, object]] = {}
    pending_assistant_record: dict[str, object] | None = None
    pending_assistant_key: str | None = None

    def process_assistant_record(record: dict[str, object]) -> None:
        nonlocal model_name
        timestamp = record.get("timestamp") if isinstance(record.get("timestamp"), str) else None
        message = record.get("message") if isinstance(record.get("message"), dict) else {}
        model_name = (
            message.get("model")
            if isinstance(message.get("model"), str) and str(message.get("model")).strip()
            else model_name
        )
        content_blocks = _claude_content_blocks(message.get("content"))
        text_blocks: list[dict[str, object]] = []

        def flush_assistant_text() -> None:
            if not text_blocks:
                return
            _append_normalized_event(
                events,
                {
                    "type": "response_item",
                    "timestamp": timestamp,
                    "payload": {
                        "type": "message",
                        "role": "assistant",
                        "content": list(text_blocks),
                    },
                },
            )
            text_blocks.clear()

        for block in content_blocks:
            block_type = str(block.get("type") or "").strip()
            if block_type == "text":
                text_blocks.append(block)
                continue

            flush_assistant_text()

            if block_type in {"thinking", "redacted_thinking"}:
                reasoning_text = (
                    block.get("thinking")
                    if isinstance(block.get("thinking"), str)
                    else ("Redacted reasoning" if block_type == "redacted_thinking" else "")
                )
                if reasoning_text:
                    _append_normalized_event(
                        events,
                        {
                            "type": "response_item",
                            "timestamp": timestamp,
                            "payload": {
                                "type": "reasoning",
                                "summary": [reasoning_text],
                            },
                        },
                    )
                continue

            if block_type in {"tool_use", *CLAUDE_SERVER_TOOL_USE_TYPES}:
                original_tool_name = (
                    block.get("name")
                    if isinstance(block.get("name"), str) and str(block.get("name")).strip()
                    else block_type
                )
                tool_name = _normalize_claude_tool_name(original_tool_name)
                call_id = block.get("id") if isinstance(block.get("id"), str) else None
                tool_input = block.get("input")
                if call_id:
                    tool_calls[call_id] = {
                        "tool_name": tool_name,
                        "original_tool_name": original_tool_name,
                        "input": tool_input,
                        "timestamp": timestamp,
                        "cwd": cwd,
                    }
                arguments = tool_input
                if tool_name == "exec_command":
                    arguments = {"cmd": tool_input.get("command"), "workdir": cwd} if isinstance(tool_input, dict) else tool_input
                _append_normalized_event(
                    events,
                    {
                        "type": "response_item",
                        "timestamp": timestamp,
                        "payload": {
                            "type": "function_call",
                            "name": tool_name,
                            "call_id": call_id,
                            "arguments": arguments,
                        },
                    },
                )
                continue

            if (
                block_type in CLAUDE_SERVER_TOOL_RESULT_TYPES
                or (block_type.endswith("_tool_result") and isinstance(block.get("tool_use_id"), str))
            ):
                _append_normalized_event(
                    events,
                    {
                        "type": "response_item",
                        "timestamp": timestamp,
                        "payload": {
                            "type": "function_call_output",
                            "call_id": block.get("tool_use_id") if isinstance(block.get("tool_use_id"), str) else None,
                            "output": _claude_materialize_tool_output(
                                block.get("content") if "content" in block else block,
                                source_path,
                            ),
                        },
                    },
                )
                continue

        flush_assistant_text()

        usage_payload = _assistant_usage_payload(message)
        if usage_payload is not None:
            _append_normalized_event(
                events,
                {
                    "type": "event_msg",
                    "timestamp": timestamp,
                    "payload": usage_payload,
                },
            )

    def flush_pending_assistant() -> None:
        nonlocal pending_assistant_record, pending_assistant_key
        if pending_assistant_record is None:
            return
        process_assistant_record(pending_assistant_record)
        pending_assistant_record = None
        pending_assistant_key = None

    for idx, line in enumerate(lines):
        line_number = idx + 1
        normalized_line = normalize_jsonl_line(line, line_number)
        if normalized_line is None:
            continue
        content_hash.update(normalized_line.encode("utf-8"))
        record = _parse_json_record(normalized_line, source_path, line_number=line_number)
        record_type = _claude_record_type(record)
        timestamp = record.get("timestamp") if isinstance(record.get("timestamp"), str) else None
        if started_at is None and timestamp:
            started_at = timestamp
        if timestamp:
            ended_at = timestamp

        root_session_id = (
            record.get("sessionId")
            if isinstance(record.get("sessionId"), str) and str(record.get("sessionId")).strip()
            else root_session_id
        )
        cwd = record.get("cwd") if isinstance(record.get("cwd"), str) and str(record.get("cwd")).strip() else cwd
        user_type = (
            record.get("userType")
            if isinstance(record.get("userType"), str) and str(record.get("userType")).strip()
            else user_type
        )
        cli_version = (
            record.get("version")
            if isinstance(record.get("version"), str) and str(record.get("version")).strip()
            else cli_version
        )
        if isinstance(record.get("isSidechain"), bool) and bool(record.get("isSidechain")):
            is_sidechain = True
        if isinstance(record.get("agentId"), str) and str(record.get("agentId")).strip():
            transcript_agent_id = str(record.get("agentId")).strip()
            is_sidechain = True
        if isinstance(record.get("teamName"), str) and str(record.get("teamName")).strip():
            team_name = str(record.get("teamName")).strip()

        if record_type == "assistant":
            message = record.get("message") if isinstance(record.get("message"), dict) else {}
            message_key = (
                message.get("id")
                if isinstance(message.get("id"), str) and str(message.get("id")).strip()
                else (
                    record.get("uuid")
                    if isinstance(record.get("uuid"), str) and str(record.get("uuid")).strip()
                    else None
                )
            )
            if (
                pending_assistant_record is not None
                and pending_assistant_key is not None
                and message_key == pending_assistant_key
            ):
                pending_assistant_record = _merge_claude_assistant_records(
                    pending_assistant_record,
                    record,
                )
                continue
            flush_pending_assistant()
            pending_assistant_record = record
            pending_assistant_key = message_key
            if pending_assistant_key is None:
                flush_pending_assistant()
            continue

        flush_pending_assistant()

        if record_type == "custom-title":
            custom_title = (
                record.get("customTitle")
                if isinstance(record.get("customTitle"), str) and str(record.get("customTitle")).strip()
                else custom_title
            )
            continue
        if record_type == "agent-name":
            agent_name = (
                record.get("agentName")
                if isinstance(record.get("agentName"), str) and str(record.get("agentName")).strip()
                else agent_name
            )
            continue
        if record_type == "agent-setting":
            agent_setting = (
                record.get("agentSetting")
                if isinstance(record.get("agentSetting"), str) and str(record.get("agentSetting")).strip()
                else agent_setting
            )
            continue
        if record_type == "pr-link":
            pr_url = record.get("prUrl") if isinstance(record.get("prUrl"), str) and str(record.get("prUrl")).strip() else pr_url
            pr_repository = (
                record.get("prRepository")
                if isinstance(record.get("prRepository"), str) and str(record.get("prRepository")).strip()
                else pr_repository
            )
            continue
        if record_type == "mode":
            mode = record.get("mode") if isinstance(record.get("mode"), str) and str(record.get("mode")).strip() else mode
            continue

        if record_type == "user":
            content = None
            message = record.get("message")
            if isinstance(message, dict):
                content = message.get("content")
            if isinstance(content, str):
                content = [{"type": "text", "text": content}]
            content_blocks = [item for item in content if isinstance(item, dict)] if isinstance(content, list) else []
            tool_result_blocks = [block for block in content_blocks if str(block.get("type") or "").strip() == "tool_result"]
            native_tool_result = record.get("toolUseResult")

            for block in tool_result_blocks:
                call_id = block.get("tool_use_id") if isinstance(block.get("tool_use_id"), str) else None
                paired_tool = tool_calls.get(call_id or "")
                output = native_tool_result if len(tool_result_blocks) == 1 and native_tool_result is not None else block.get("content")
                materialized_output = _claude_materialize_tool_output(output, source_path)
                if paired_tool and paired_tool.get("tool_name") == "exec_command":
                    command_input = paired_tool.get("input")
                    command_text = (
                        command_input.get("command")
                        if isinstance(command_input, dict) and isinstance(command_input.get("command"), str)
                        else None
                    )
                    _append_normalized_event(
                        events,
                        {
                            "type": "event_msg",
                            "timestamp": timestamp,
                            "payload": {
                                "type": "exec_command_end",
                                "call_id": call_id,
                                "command": command_text,
                                "exit_code": _claude_command_exit_code(block, output),
                                "aggregated_output": _claude_display_output(output, source_path),
                            },
                        },
                    )
                    continue
                _append_normalized_event(
                    events,
                    {
                        "type": "response_item",
                        "timestamp": timestamp,
                        "payload": {
                            "type": "function_call_output",
                            "call_id": call_id,
                            "output": materialized_output,
                        },
                    },
                )

            if not tool_result_blocks and not bool(record.get("isMeta")):
                text_content = first_text_from_content(content_blocks)
                if text_content:
                    _append_normalized_event(
                        events,
                        {
                            "type": "response_item",
                            "timestamp": timestamp,
                            "payload": {
                                "type": "message",
                                "role": "user",
                                "content": content_blocks,
                            },
                        },
                    )
            continue

        if record_type == "system":
            subtype = str(record.get("subtype") or "").strip()
            if subtype in {"thinking"} and isinstance(record.get("content"), str):
                _append_normalized_event(
                    events,
                    {
                        "type": "response_item",
                        "timestamp": timestamp,
                        "payload": {
                            "type": "reasoning",
                            "summary": [record["content"]],
                        },
                    },
                )
                continue
            if subtype in {"compact_boundary", "microcompact_boundary"}:
                _append_normalized_event(
                    events,
                    {
                        "type": "compacted",
                        "timestamp": timestamp,
                        "payload": {"message": str(record.get("content") or "Conversation history compacted")},
                    },
                )
                continue
            if isinstance(record.get("content"), str) and str(record.get("content")).strip():
                _append_normalized_event(
                    events,
                    {
                        "type": "event_msg",
                        "timestamp": timestamp,
                        "payload": {
                            "type": "agent_message",
                            "message": str(record.get("content") or ""),
                        },
                    },
                )
            continue

        if record_type == "attachment":
            attachment = record.get("attachment") if isinstance(record.get("attachment"), dict) else {}
            attachment_type = str(attachment.get("type") or "").strip()
            if attachment_type == "queued_command" and not bool(attachment.get("isMeta")):
                prompt = attachment.get("prompt")
                prompt_text = prompt if isinstance(prompt, str) else first_text_from_content(prompt)
                if prompt_text:
                    _append_normalized_event(
                        events,
                        {
                            "type": "response_item",
                            "timestamp": timestamp,
                            "payload": {
                                "type": "message",
                                "role": "user",
                                "content": [{"type": "text", "text": prompt_text}],
                            },
                        },
                    )
            continue

    flush_pending_assistant()

    parent_session_id = parent_session_id_from_path or (root_session_id if is_sidechain else None)
    session_id = (
        _claude_sidechain_session_id(root_session_id, transcript_agent_id, source_path, source_root)
        if is_sidechain
        else root_session_id
    )

    if _is_claude_warmup_transcript(events, is_sidechain):
        raise SessionSkipError(source_path, "warmup agent transcript")

    raw_meta: dict[str, object] = {
        "id": session_id,
        "timestamp": started_at,
        "cwd": cwd,
        "originator": user_type or "claude-code",
        "cli_version": cli_version,
        "source": "claude-code",
        "model_provider": "anthropic",
        "transcript_format": "claude",
    }
    if model_name:
        raw_meta["model"] = model_name
    if custom_title:
        raw_meta["custom_title"] = custom_title
    if agent_name:
        raw_meta["agent_nickname"] = agent_name
    elif transcript_agent_id:
        raw_meta["agent_nickname"] = transcript_agent_id.split("@", 1)[0].strip() or transcript_agent_id
    if agent_setting:
        raw_meta["agent_role"] = agent_setting
    if transcript_agent_id:
        raw_meta["agent_id"] = transcript_agent_id
        raw_meta["agent_path"] = _claude_relative_source_path(source_path, source_root)
    if parent_session_id and parent_session_id != session_id:
        raw_meta["forked_from_id"] = parent_session_id
    if pr_url:
        raw_meta["pr_url"] = pr_url
    if pr_repository:
        raw_meta["pr_repository"] = pr_repository
    if mode:
        raw_meta["mode"] = mode
    if team_name:
        raw_meta["team_name"] = team_name
    if is_sidechain:
        raw_meta["transcript_scope"] = "sidechain"

    return _finalize_parsed_session(
        source_path=source_path,
        source_root=source_root,
        source_host=source_host,
        file_size=file_size,
        file_mtime_ns=file_mtime_ns,
        content_sha256=content_hash.hexdigest(),
        raw_meta=raw_meta,
        started_at=started_at,
        ended_at=ended_at,
        events=events,
        imported_at=imported_at,
    )


def parse_session_file(source_path: Path, source_root: Path, source_host: str) -> ParsedSession:
    stat = source_path.stat()
    raw_jsonl = read_session_source_text(source_path)
    return parse_session_text(
        raw_jsonl,
        source_path,
        source_root,
        source_host,
        file_size=stat.st_size,
        file_mtime_ns=stat.st_mtime_ns,
    )


def parse_session_text(
    raw_jsonl: str,
    source_path: Path,
    source_root: Path,
    source_host: str,
    *,
    file_size: int | None = None,
    file_mtime_ns: int | None = None,
) -> ParsedSession:
    content_bytes = raw_jsonl.encode("utf-8")
    lines = raw_jsonl.splitlines(keepends=True)
    parse_kwargs = {
        "file_size": file_size if file_size is not None else len(content_bytes),
        "file_mtime_ns": file_mtime_ns if file_mtime_ns is not None else 0,
    }
    session_format = _detect_session_format(lines, source_path)
    if session_format == "claude":
        return _parse_claude_session_lines(
            lines,
            source_path,
            source_root,
            source_host,
            **parse_kwargs,
        )
    return _parse_session_lines(
        lines,
        source_path,
        source_root,
        source_host,
        **parse_kwargs,
    )


def iter_session_files(roots: Iterable[Path]) -> Iterable[tuple[Path, Path]]:
    for root in roots:
        expanded_root = root.expanduser()
        if not expanded_root.exists():
            continue
        for path in sorted(expanded_root.rglob("*.jsonl")):
            yield expanded_root, path


def normalized_event_to_dict(event: NormalizedEvent) -> dict[str, object]:
    return {
        "event_index": event.event_index,
        "timestamp": event.timestamp,
        "record_type": event.record_type,
        "payload_type": event.payload_type,
        "kind": event.kind,
        "role": event.role,
        "title": event.title,
        "display_text": event.display_text,
        "detail_text": event.detail_text,
        "tool_name": event.tool_name,
        "call_id": event.call_id,
        "command_text": event.command_text,
        "exit_code": event.exit_code,
        "record_json": event.record_json,
    }


def parsed_session_to_payload(parsed: ParsedSession) -> dict[str, object]:
    return {
        "session": {
            "id": parsed.session_id,
            "source_path": str(parsed.source_path),
            "source_root": str(parsed.source_root),
            "file_size": parsed.file_size,
            "file_mtime_ns": parsed.file_mtime_ns,
            "content_sha256": parsed.content_sha256,
            "raw_artifact_sha256": parsed.raw_artifact_sha256,
            "session_timestamp": parsed.session_timestamp,
            "started_at": parsed.started_at,
            "ended_at": parsed.ended_at,
            "cwd": parsed.cwd,
            "cwd_name": parsed.cwd_name,
            "source_host": parsed.source_host,
            "originator": parsed.originator,
            "cli_version": parsed.cli_version,
            "source": parsed.source,
            "model_provider": parsed.model_provider,
            "git_branch": parsed.git_branch,
            "git_commit_hash": parsed.git_commit_hash,
            "git_repository_url": parsed.git_repository_url,
            "github_remote_url": parsed.github_remote_url,
            "github_org": parsed.github_org,
            "github_repo": parsed.github_repo,
            "github_slug": parsed.github_slug,
            "forked_from_id": parsed.forked_from_id,
            "agent_nickname": parsed.agent_nickname,
            "agent_role": parsed.agent_role,
            "agent_path": parsed.agent_path,
            "memory_mode": parsed.memory_mode,
            "inferred_project_kind": parsed.inferred_project_kind,
            "inferred_project_key": parsed.inferred_project_key,
            "inferred_project_label": parsed.inferred_project_label,
            "summary": parsed.summary,
            "event_count": parsed.event_count,
            "user_message_count": parsed.user_message_count,
            "assistant_message_count": parsed.assistant_message_count,
            "tool_call_count": parsed.tool_call_count,
            "rollup_version": parsed.rollup_version,
            "turn_count": parsed.turn_count,
            "last_user_message": parsed.last_user_message,
            "last_turn_timestamp": parsed.last_turn_timestamp,
            "latest_turn_summary": parsed.latest_turn_summary,
            "command_failure_count": parsed.command_failure_count,
            "aborted_turn_count": parsed.aborted_turn_count,
            "latest_usage_timestamp": parsed.latest_usage_timestamp,
            "latest_input_tokens": parsed.latest_input_tokens,
            "latest_cached_input_tokens": parsed.latest_cached_input_tokens,
            "latest_output_tokens": parsed.latest_output_tokens,
            "latest_reasoning_output_tokens": parsed.latest_reasoning_output_tokens,
            "latest_total_tokens": parsed.latest_total_tokens,
            "latest_context_window": parsed.latest_context_window,
            "latest_context_remaining_percent": parsed.latest_context_remaining_percent,
            "latest_primary_limit_used_percent": parsed.latest_primary_limit_used_percent,
            "latest_primary_limit_resets_at": parsed.latest_primary_limit_resets_at,
            "latest_secondary_limit_used_percent": parsed.latest_secondary_limit_used_percent,
            "latest_secondary_limit_resets_at": parsed.latest_secondary_limit_resets_at,
            "latest_rate_limit_name": parsed.latest_rate_limit_name,
            "latest_rate_limit_reached_type": parsed.latest_rate_limit_reached_type,
            "import_warning": parsed.import_warning,
            "search_text": parsed.search_text,
            "raw_meta_json": parsed.raw_meta_json,
            "imported_at": parsed.imported_at,
            "updated_at": parsed.updated_at,
        },
        "events": [normalized_event_to_dict(event) for event in parsed.events],
    }


def _payload_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _payload_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _payload_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def parsed_session_from_payload(payload: dict[str, object]) -> ParsedSession:
    session = payload.get("session")
    events = payload.get("events")
    if not isinstance(session, dict) or not isinstance(events, list):
        raise ValueError("Payload must include session and events objects")

    session_id = _payload_str(session.get("id"))
    source_path = _payload_str(session.get("source_path"))
    source_root = _payload_str(session.get("source_root"))
    source_host = _payload_str(session.get("source_host"))
    content_sha256 = _payload_str(session.get("content_sha256"))
    raw_artifact_sha256 = _payload_str(session.get("raw_artifact_sha256"))
    file_size = _payload_int(session.get("file_size"))
    file_mtime_ns = _payload_int(session.get("file_mtime_ns"))
    cwd_name = _payload_str(session.get("cwd_name"))
    inferred_project_kind = _payload_str(session.get("inferred_project_kind"))
    inferred_project_key = _payload_str(session.get("inferred_project_key"))
    inferred_project_label = _payload_str(session.get("inferred_project_label"))
    summary = _payload_str(session.get("summary"))
    event_count = _payload_int(session.get("event_count"))
    user_message_count = _payload_int(session.get("user_message_count"))
    assistant_message_count = _payload_int(session.get("assistant_message_count"))
    tool_call_count = _payload_int(session.get("tool_call_count"))
    rollup_version = _payload_int(session.get("rollup_version"))
    turn_count = _payload_int(session.get("turn_count"))
    last_user_message = _payload_str(session.get("last_user_message"))
    command_failure_count = _payload_int(session.get("command_failure_count"))
    aborted_turn_count = _payload_int(session.get("aborted_turn_count"))
    search_text = _payload_str(session.get("search_text"))
    raw_meta_json = _payload_str(session.get("raw_meta_json"))
    imported_at = _payload_str(session.get("imported_at"))
    updated_at = _payload_str(session.get("updated_at"))

    required_values = (
        session_id,
        source_path,
        source_root,
        source_host,
        content_sha256,
        file_size,
        file_mtime_ns,
        cwd_name,
        inferred_project_kind,
        inferred_project_key,
        inferred_project_label,
        summary,
        event_count,
        user_message_count,
        assistant_message_count,
        tool_call_count,
        search_text,
        raw_meta_json,
        imported_at,
        updated_at,
    )
    if any(value is None for value in required_values):
        raise ValueError("Session payload is missing required fields")

    normalized_events: list[NormalizedEvent] = []
    for item in events:
        if not isinstance(item, dict):
            raise ValueError("Event payload entries must be objects")
        event_index = _payload_int(item.get("event_index"))
        record_type = _payload_str(item.get("record_type"))
        kind = _payload_str(item.get("kind"))
        title = _payload_str(item.get("title"))
        display_text = _payload_str(item.get("display_text"))
        detail_text = _payload_str(item.get("detail_text"))
        record_json = _payload_str(item.get("record_json"))
        if None in {event_index, record_type, kind, title, display_text, detail_text, record_json}:
            raise ValueError("Event payload is missing required fields")
        normalized_events.append(
            NormalizedEvent(
                event_index=event_index,
                timestamp=_payload_str(item.get("timestamp")),
                record_type=record_type,
                payload_type=_payload_str(item.get("payload_type")),
                kind=kind,
                role=_payload_str(item.get("role")),
                title=title,
                display_text=display_text,
                detail_text=detail_text,
                tool_name=_payload_str(item.get("tool_name")),
                call_id=_payload_str(item.get("call_id")),
                command_text=_payload_str(item.get("command_text")),
                exit_code=_payload_int(item.get("exit_code")),
                record_json=record_json,
            )
        )

    if len(normalized_events) != event_count:
        raise ValueError("Session event count does not match uploaded event payload")

    derived_rollups = compute_session_rollups(normalized_events)
    derived_agent_metadata = extract_agent_metadata(parse_raw_meta_json(raw_meta_json))

    rollup_keys = {
        "turn_count",
        "last_user_message",
        "last_turn_timestamp",
        "latest_turn_summary",
        "command_failure_count",
        "aborted_turn_count",
        "latest_usage_timestamp",
        "latest_input_tokens",
        "latest_cached_input_tokens",
        "latest_output_tokens",
        "latest_reasoning_output_tokens",
        "latest_total_tokens",
        "latest_context_window",
        "latest_context_remaining_percent",
        "latest_primary_limit_used_percent",
        "latest_primary_limit_resets_at",
        "latest_secondary_limit_used_percent",
        "latest_secondary_limit_resets_at",
        "latest_rate_limit_name",
        "latest_rate_limit_reached_type",
    }
    use_derived_rollups = (
        rollup_version is None
        or rollup_version < int(derived_rollups["rollup_version"])
        or any(key not in session for key in rollup_keys)
        or None in {
            turn_count,
            last_user_message,
            command_failure_count,
            aborted_turn_count,
        }
    )
    if use_derived_rollups:
        rollup_version = int(derived_rollups["rollup_version"])
        turn_count = int(derived_rollups["turn_count"])
        last_user_message = str(derived_rollups["last_user_message"] or "")
        last_turn_timestamp = _payload_str(session.get("last_turn_timestamp")) or derived_rollups["last_turn_timestamp"]
        latest_turn_summary = _payload_str(session.get("latest_turn_summary")) or derived_rollups["latest_turn_summary"]
        command_failure_count = int(derived_rollups["command_failure_count"])
        aborted_turn_count = int(derived_rollups["aborted_turn_count"])
        latest_usage_timestamp = derived_rollups["latest_usage_timestamp"]
        latest_input_tokens = int(derived_rollups["latest_input_tokens"] or 0)
        latest_cached_input_tokens = int(derived_rollups["latest_cached_input_tokens"] or 0)
        latest_output_tokens = int(derived_rollups["latest_output_tokens"] or 0)
        latest_reasoning_output_tokens = int(derived_rollups["latest_reasoning_output_tokens"] or 0)
        latest_total_tokens = int(derived_rollups["latest_total_tokens"] or 0)
        latest_context_window = (
            int(derived_rollups["latest_context_window"])
            if derived_rollups["latest_context_window"] is not None
            else None
        )
        latest_context_remaining_percent = (
            int(derived_rollups["latest_context_remaining_percent"])
            if derived_rollups["latest_context_remaining_percent"] is not None
            else None
        )
        latest_primary_limit_used_percent = (
            float(derived_rollups["latest_primary_limit_used_percent"])
            if derived_rollups["latest_primary_limit_used_percent"] is not None
            else None
        )
        latest_primary_limit_resets_at = derived_rollups["latest_primary_limit_resets_at"]
        latest_secondary_limit_used_percent = (
            float(derived_rollups["latest_secondary_limit_used_percent"])
            if derived_rollups["latest_secondary_limit_used_percent"] is not None
            else None
        )
        latest_secondary_limit_resets_at = derived_rollups["latest_secondary_limit_resets_at"]
        latest_rate_limit_name = derived_rollups["latest_rate_limit_name"]
        latest_rate_limit_reached_type = derived_rollups["latest_rate_limit_reached_type"]
    else:
        last_turn_timestamp = _payload_str(session.get("last_turn_timestamp"))
        latest_turn_summary = _payload_str(session.get("latest_turn_summary"))
        latest_usage_timestamp = _payload_str(session.get("latest_usage_timestamp"))
        latest_input_tokens = _payload_int(session.get("latest_input_tokens")) or 0
        latest_cached_input_tokens = _payload_int(session.get("latest_cached_input_tokens")) or 0
        latest_output_tokens = _payload_int(session.get("latest_output_tokens")) or 0
        latest_reasoning_output_tokens = _payload_int(session.get("latest_reasoning_output_tokens")) or 0
        latest_total_tokens = _payload_int(session.get("latest_total_tokens")) or 0
        latest_context_window = _payload_int(session.get("latest_context_window"))
        latest_context_remaining_percent = _payload_int(session.get("latest_context_remaining_percent"))
        latest_primary_limit_used_percent = _payload_float(session.get("latest_primary_limit_used_percent"))
        latest_primary_limit_resets_at = _payload_str(session.get("latest_primary_limit_resets_at"))
        latest_secondary_limit_used_percent = _payload_float(session.get("latest_secondary_limit_used_percent"))
        latest_secondary_limit_resets_at = _payload_str(session.get("latest_secondary_limit_resets_at"))
        latest_rate_limit_name = _payload_str(session.get("latest_rate_limit_name"))
        latest_rate_limit_reached_type = _payload_str(session.get("latest_rate_limit_reached_type"))

    forked_from_id = _payload_str(session.get("forked_from_id"))
    agent_nickname = _payload_str(session.get("agent_nickname"))
    agent_role = _payload_str(session.get("agent_role"))
    agent_path = _payload_str(session.get("agent_path"))
    memory_mode = _payload_str(session.get("memory_mode"))
    if "forked_from_id" not in session:
        forked_from_id = derived_agent_metadata["forked_from_id"]
    if "agent_nickname" not in session:
        agent_nickname = derived_agent_metadata["agent_nickname"]
    if "agent_role" not in session:
        agent_role = derived_agent_metadata["agent_role"]
    if "agent_path" not in session:
        agent_path = derived_agent_metadata["agent_path"]
    if "memory_mode" not in session:
        memory_mode = derived_agent_metadata["memory_mode"]

    return ParsedSession(
        session_id=session_id,
        source_path=Path(source_path),
        source_root=Path(source_root),
        file_size=file_size,
        file_mtime_ns=file_mtime_ns,
        content_sha256=content_sha256,
        session_timestamp=_payload_str(session.get("session_timestamp")),
        started_at=_payload_str(session.get("started_at")),
        ended_at=_payload_str(session.get("ended_at")),
        cwd=_payload_str(session.get("cwd")),
        cwd_name=cwd_name,
        source_host=source_host,
        originator=_payload_str(session.get("originator")),
        cli_version=_payload_str(session.get("cli_version")),
        source=_payload_str(session.get("source")),
        model_provider=_payload_str(session.get("model_provider")),
        git_branch=_payload_str(session.get("git_branch")),
        git_commit_hash=_payload_str(session.get("git_commit_hash")),
        git_repository_url=_payload_str(session.get("git_repository_url")),
        github_remote_url=_payload_str(session.get("github_remote_url")),
        github_org=_payload_str(session.get("github_org")),
        github_repo=_payload_str(session.get("github_repo")),
        github_slug=_payload_str(session.get("github_slug")),
        forked_from_id=forked_from_id,
        agent_nickname=agent_nickname,
        agent_role=agent_role,
        agent_path=agent_path,
        memory_mode=memory_mode,
        inferred_project_kind=inferred_project_kind,
        inferred_project_key=inferred_project_key,
        inferred_project_label=inferred_project_label,
        summary=summary,
        event_count=event_count,
        user_message_count=user_message_count,
        assistant_message_count=assistant_message_count,
        tool_call_count=tool_call_count,
        rollup_version=rollup_version,
        turn_count=turn_count,
        last_user_message=last_user_message,
        last_turn_timestamp=last_turn_timestamp,
        latest_turn_summary=latest_turn_summary,
        command_failure_count=command_failure_count,
        aborted_turn_count=aborted_turn_count,
        latest_usage_timestamp=latest_usage_timestamp,
        latest_input_tokens=latest_input_tokens,
        latest_cached_input_tokens=latest_cached_input_tokens,
        latest_output_tokens=latest_output_tokens,
        latest_reasoning_output_tokens=latest_reasoning_output_tokens,
        latest_total_tokens=latest_total_tokens,
        latest_context_window=latest_context_window,
        latest_context_remaining_percent=latest_context_remaining_percent,
        latest_primary_limit_used_percent=latest_primary_limit_used_percent,
        latest_primary_limit_resets_at=latest_primary_limit_resets_at,
        latest_secondary_limit_used_percent=latest_secondary_limit_used_percent,
        latest_secondary_limit_resets_at=latest_secondary_limit_resets_at,
        latest_rate_limit_name=latest_rate_limit_name,
        latest_rate_limit_reached_type=latest_rate_limit_reached_type,
        import_warning=_payload_str(session.get("import_warning")),
        search_text=search_text,
        raw_meta_json=raw_meta_json,
        imported_at=imported_at,
        updated_at=updated_at,
        events=normalized_events,
        raw_artifact_sha256=raw_artifact_sha256,
    )


def upsert_parsed_session(connection: sqlite3.Connection, parsed: ParsedSession) -> None:
    existing_by_path = connection.execute(
        """
        SELECT id
        FROM sessions
        WHERE source_host = ? AND source_path = ?
        """,
        (parsed.source_host, str(parsed.source_path)),
    ).fetchone()
    if existing_by_path is not None and str(existing_by_path["id"] or "") != parsed.session_id:
        connection.execute(
            "DELETE FROM events WHERE session_id = ?",
            (str(existing_by_path["id"]),),
        )
        connection.execute(
            "DELETE FROM sessions WHERE id = ?",
            (str(existing_by_path["id"]),),
        )

    connection.execute("DELETE FROM events WHERE session_id = ?", (parsed.session_id,))
    existing_by_id = connection.execute(
        "SELECT raw_artifact_sha256 FROM sessions WHERE id = ?",
        (parsed.session_id,),
    ).fetchone()
    raw_artifact_sha256 = parsed.raw_artifact_sha256
    if raw_artifact_sha256 is None and existing_by_id is not None:
        raw_artifact_sha256 = str(existing_by_id["raw_artifact_sha256"] or "").strip() or None
    session_columns = (
        "id",
        "source_path",
        "source_root",
        "file_size",
        "file_mtime_ns",
        "content_sha256",
        "raw_artifact_sha256",
        "session_timestamp",
        "started_at",
        "ended_at",
        "cwd",
        "cwd_name",
        "source_host",
        "originator",
        "cli_version",
        "source",
        "model_provider",
        "git_branch",
        "git_commit_hash",
        "git_repository_url",
        "github_remote_url",
        "github_org",
        "github_repo",
        "github_slug",
        "forked_from_id",
        "agent_nickname",
        "agent_role",
        "agent_path",
        "memory_mode",
        "inferred_project_kind",
        "inferred_project_key",
        "inferred_project_label",
        "summary",
        "event_count",
        "user_message_count",
        "assistant_message_count",
        "tool_call_count",
        "rollup_version",
        "turn_count",
        "last_user_message",
        "last_turn_timestamp",
        "latest_turn_summary",
        "command_failure_count",
        "aborted_turn_count",
        "latest_usage_timestamp",
        "latest_input_tokens",
        "latest_cached_input_tokens",
        "latest_output_tokens",
        "latest_reasoning_output_tokens",
        "latest_total_tokens",
        "latest_context_window",
        "latest_context_remaining_percent",
        "latest_primary_limit_used_percent",
        "latest_primary_limit_resets_at",
        "latest_secondary_limit_used_percent",
        "latest_secondary_limit_resets_at",
        "latest_rate_limit_name",
        "latest_rate_limit_reached_type",
        "import_warning",
        "search_text",
        "raw_meta_json",
        "imported_at",
        "updated_at",
    )
    session_values = (
        parsed.session_id,
        str(parsed.source_path),
        str(parsed.source_root),
        parsed.file_size,
        parsed.file_mtime_ns,
        parsed.content_sha256,
        raw_artifact_sha256,
        parsed.session_timestamp,
        parsed.started_at,
        parsed.ended_at,
        parsed.cwd,
        parsed.cwd_name,
        parsed.source_host,
        parsed.originator,
        parsed.cli_version,
        parsed.source,
        parsed.model_provider,
        parsed.git_branch,
        parsed.git_commit_hash,
        parsed.git_repository_url,
        parsed.github_remote_url,
        parsed.github_org,
        parsed.github_repo,
        parsed.github_slug,
        parsed.forked_from_id,
        parsed.agent_nickname,
        parsed.agent_role,
        parsed.agent_path,
        parsed.memory_mode,
        parsed.inferred_project_kind,
        parsed.inferred_project_key,
        parsed.inferred_project_label,
        parsed.summary,
        parsed.event_count,
        parsed.user_message_count,
        parsed.assistant_message_count,
        parsed.tool_call_count,
        parsed.rollup_version,
        parsed.turn_count,
        parsed.last_user_message,
        parsed.last_turn_timestamp,
        parsed.latest_turn_summary,
        parsed.command_failure_count,
        parsed.aborted_turn_count,
        parsed.latest_usage_timestamp,
        parsed.latest_input_tokens,
        parsed.latest_cached_input_tokens,
        parsed.latest_output_tokens,
        parsed.latest_reasoning_output_tokens,
        parsed.latest_total_tokens,
        parsed.latest_context_window,
        parsed.latest_context_remaining_percent,
        parsed.latest_primary_limit_used_percent,
        parsed.latest_primary_limit_resets_at,
        parsed.latest_secondary_limit_used_percent,
        parsed.latest_secondary_limit_resets_at,
        parsed.latest_rate_limit_name,
        parsed.latest_rate_limit_reached_type,
        parsed.import_warning,
        parsed.search_text,
        parsed.raw_meta_json,
        parsed.imported_at,
        parsed.updated_at,
    )
    if existing_by_id is None:
        insert_columns_sql = ", ".join(session_columns)
        insert_placeholders_sql = ", ".join("?" for _ in session_columns)
        connection.execute(
            f"INSERT INTO sessions ({insert_columns_sql}) VALUES ({insert_placeholders_sql})",
            session_values,
        )
    else:
        update_columns = session_columns[1:]
        update_assignments_sql = ", ".join(f"{column} = ?" for column in update_columns)
        connection.execute(
            f"UPDATE sessions SET {update_assignments_sql} WHERE id = ?",
            session_values[1:] + (parsed.session_id,),
        )
    connection.executemany(
        """
        INSERT INTO events (
            session_id, event_index, timestamp, record_type, payload_type,
            kind, role, title, display_text, detail_text, tool_name,
            call_id, command_text, exit_code, record_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                parsed.session_id,
                event.event_index,
                event.timestamp,
                event.record_type,
                event.payload_type,
                event.kind,
                event.role,
                event.title,
                event.display_text,
                event.detail_text,
                event.tool_name,
                event.call_id,
                event.command_text,
                event.exit_code,
                event.record_json,
            )
            for event in parsed.events
        ],
    )
    replace_session_turn_activity_daily(connection, parsed.session_id, parsed.events)
    replace_session_turns(connection, parsed.session_id, parsed.events)
    replace_session_turn_search(connection, parsed.session_id, parsed.events)
    from .action_queue import replace_session_action_queue_rollups
    from .environment_audit import replace_session_environment_rollups

    replace_session_action_queue_rollups(connection, parsed.session_id, parsed.events)
    replace_session_environment_rollups(connection, parsed.session_id, parsed.events)


def fetch_host_sync_manifest(connection: sqlite3.Connection, source_host: str) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT
            s.id,
            s.source_host,
            s.source_path,
            s.source_root,
            s.file_size,
            s.file_mtime_ns,
            s.content_sha256,
            MAX(CASE WHEN s.raw_artifact_sha256 IS NOT NULL AND sa.sha256 IS NOT NULL THEN 1 ELSE 0 END) AS has_raw_artifact,
            s.event_count,
            COUNT(e.id) AS stored_event_count,
            s.updated_at
        FROM sessions AS s
        LEFT JOIN events AS e
            ON e.session_id = s.id
        LEFT JOIN session_artifacts AS sa
            ON sa.sha256 = s.raw_artifact_sha256
        WHERE s.source_host = ?
        GROUP BY
            s.id,
            s.source_host,
            s.source_path,
            s.source_root,
            s.file_size,
            s.file_mtime_ns,
            s.content_sha256,
            s.event_count,
            s.updated_at
        ORDER BY s.source_path ASC
        """,
        (source_host,),
    ).fetchall()
    return [dict(row) for row in rows]


def sync_sessions(settings: Settings, force: bool = False) -> dict[str, int]:
    imported = 0
    updated = 0
    skipped = 0
    project_registry_changed = False

    with connect(settings.database_path) as connection:
        with write_transaction(connection):
            from .projects import sync_project_registry

            existing_rows = connection.execute(
                """
                SELECT
                    id,
                    source_path,
                    source_root,
                    file_size,
                    file_mtime_ns,
                    content_sha256,
                    source_host,
                    raw_artifact_sha256
                FROM sessions
                """
            ).fetchall()
            existing_by_source = {
                (row["source_host"], row["source_path"]): {
                    "id": row["id"],
                    "source_root": row["source_root"],
                    "file_size": row["file_size"],
                    "file_mtime_ns": row["file_mtime_ns"],
                    "content_sha256": row["content_sha256"],
                    "source_host": row["source_host"],
                    "raw_artifact_sha256": row["raw_artifact_sha256"],
                }
                for row in existing_rows
            }
            if force:
                connection.execute("DELETE FROM events")
                connection.execute("DELETE FROM sessions")
                connection.execute("DELETE FROM environment_command_observations")
                connection.execute("DELETE FROM environment_host_capabilities")
                project_registry_changed = True

            restored_source_keys: set[tuple[str, str]] = set()

            for source_root, path in iter_session_files(settings.session_roots):
                stat = path.stat()
                record = existing_by_source.get((settings.source_host, str(path)))
                if (
                    not force
                    and record
                    and record["file_size"] == stat.st_size
                    and record["file_mtime_ns"] == stat.st_mtime_ns
                    and record["source_host"] == settings.source_host
                    and str(record["raw_artifact_sha256"] or "").strip()
                ):
                    skipped += 1
                    continue

                try:
                    raw_jsonl = read_session_source_text(path)
                    parsed = parse_session_text(
                        raw_jsonl,
                        path,
                        source_root,
                        settings.source_host,
                        file_size=stat.st_size,
                        file_mtime_ns=stat.st_mtime_ns,
                    )
                except SessionSkipError as exc:
                    logger.info("Skipping session file %s", exc)
                    skipped += 1
                    continue
                except SessionParseError as exc:
                    logger.warning("Skipping malformed session file %s", exc)
                    skipped += 1
                    continue
                if project_is_ignored(connection, parsed.inferred_project_key):
                    skipped += 1
                    continue
                parsed.raw_artifact_sha256 = store_session_artifact(connection, settings, raw_jsonl)
                upsert_parsed_session(connection, parsed)
                restored_source_keys.add((parsed.source_host, str(parsed.source_path)))
                project_registry_changed = True

                if record:
                    updated += 1
                else:
                    imported += 1

            if force:
                for row in existing_rows:
                    source_host = str(row["source_host"] or "").strip()
                    source_path = str(row["source_path"] or "").strip()
                    if not source_host or not source_path:
                        continue
                    source_key = (source_host, source_path)
                    if source_key in restored_source_keys:
                        continue
                    artifact_sha256 = str(row["raw_artifact_sha256"] or "").strip()
                    if not artifact_sha256:
                        continue

                    raw_jsonl = load_session_artifact_text(connection, settings, artifact_sha256)
                    if raw_jsonl is None:
                        skipped += 1
                        continue

                    try:
                        parsed = parse_session_text(
                            raw_jsonl,
                            Path(source_path),
                            Path(str(row["source_root"] or "").strip() or Path(source_path).parent),
                            source_host,
                            file_size=int(row["file_size"] or len(raw_jsonl.encode("utf-8"))),
                            file_mtime_ns=int(row["file_mtime_ns"] or 0),
                        )
                    except SessionSkipError as exc:
                        logger.info("Skipping stored session artifact %s", exc)
                        skipped += 1
                        continue
                    except SessionParseError as exc:
                        logger.warning("Skipping malformed stored session artifact %s", exc)
                        skipped += 1
                        continue

                    if project_is_ignored(connection, parsed.inferred_project_key):
                        skipped += 1
                        continue

                    parsed.raw_artifact_sha256 = artifact_sha256
                    upsert_parsed_session(connection, parsed)
                    restored_source_keys.add(source_key)
                    project_registry_changed = True
                    updated += 1

            if project_registry_changed:
                sync_project_registry(connection)

    return {"imported": imported, "updated": updated, "skipped": skipped}
