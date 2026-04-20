from __future__ import annotations

import hashlib
import json
import logging
import shlex
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .config import Settings
from .db import connect, write_transaction
from .git_utils import infer_project_identity, resolve_git_info
from .projects import project_is_ignored
from .session_rollups import (
    compute_session_rollups,
    replace_session_turn_activity_daily,
)
from .turn_index import replace_session_turns
from .text_utils import shorten, strip_codex_wrappers

logger = logging.getLogger("codex_session_viewer.importer")


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


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
    import_warning: str | None
    search_text: str
    raw_meta_json: str
    imported_at: str
    updated_at: str
    events: list[NormalizedEvent]


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
        kind = "message" if role in {"user", "assistant"} else "system"
        if role == "assistant":
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
    elif record_type == "event_msg" and payload_type == "task_started":
        kind = "system"
        title = "Task Started"
        display_text = str(payload.get("started_at") or "Task started")
    elif record_type == "event_msg" and payload_type == "task_complete":
        kind = "system"
        title = "Task Complete"
        display_text = shorten(str(payload.get("last_agent_message") or "Task completed"), 320)
    elif record_type == "event_msg" and payload_type == "web_search_end":
        kind = "tool_result"
        title = "Web Search"
        call_id = payload.get("call_id") if isinstance(payload.get("call_id"), str) else None
        query = payload.get("query")
        action = payload.get("action")
        display_text = str(query or action or "Web search completed")
    elif record_type == "response_item" and payload_type == "web_search_call":
        kind = "tool_call"
        title = "Web Search"
        display_text = safe_json(payload)
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
    user_messages: list[str] = []
    assistant_messages: list[str] = []
    search_parts: list[str] = []
    warning: str | None = None

    for idx, line in enumerate(lines):
        line_number = idx + 1
        normalized_line = normalize_jsonl_line(line, line_number)
        if normalized_line is None:
            continue
        content_hash.update(normalized_line.encode("utf-8"))
        try:
            record = json.loads(normalized_line)
        except json.JSONDecodeError as exc:
            raise SessionParseError(
                source_path,
                exc.msg,
                line_number=line_number,
                line_preview=shorten(normalized_line.replace("\n", " "), 120),
            ) from exc
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

    if raw_meta is None:
        raise SessionParseError(source_path, "does not contain a session_meta record")

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
    )
    project_identity = infer_project_identity(
        source_host=source_host,
        cwd=cwd,
        github_org=git_info["github_org"],
        github_repo=git_info["github_repo"],
        github_slug=git_info["github_slug"],
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
        git_info["github_org"] or "",
        git_info["github_repo"] or "",
        git_info["github_slug"] or "",
        git_info["github_remote_url"] or "",
    ]
    search_text = "\n".join(part for part in [*search_head, *search_parts] if part)
    if len(search_text) > 200_000:
        warning = "Search text truncated during import"
        search_text = search_text[:200_000]
    rollups = compute_session_rollups(events)

    return ParsedSession(
        session_id=session_id,
        source_path=source_path,
        source_root=source_root,
        file_size=file_size,
        file_mtime_ns=file_mtime_ns,
        content_sha256=content_hash.hexdigest(),
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
        import_warning=warning,
        search_text=search_text,
        raw_meta_json=safe_json(raw_meta),
        imported_at=imported_at,
        updated_at=imported_at,
        events=events,
    )


def parse_session_file(source_path: Path, source_root: Path, source_host: str) -> ParsedSession:
    stat = source_path.stat()
    with source_path.open("r", encoding="utf-8") as handle:
        return _parse_session_lines(
            handle,
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
    return _parse_session_lines(
        raw_jsonl.splitlines(keepends=True),
        source_path,
        source_root,
        source_host,
        file_size=file_size if file_size is not None else len(content_bytes),
        file_mtime_ns=file_mtime_ns if file_mtime_ns is not None else 0,
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
    return value if isinstance(value, int) else None


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

    if None in {
        rollup_version,
        turn_count,
        last_user_message,
        command_failure_count,
        aborted_turn_count,
    }:
        rollups = compute_session_rollups(normalized_events)
        rollup_version = int(rollups["rollup_version"])
        turn_count = int(rollups["turn_count"])
        last_user_message = str(rollups["last_user_message"] or "")
        command_failure_count = int(rollups["command_failure_count"])
        aborted_turn_count = int(rollups["aborted_turn_count"])
        last_turn_timestamp = _payload_str(session.get("last_turn_timestamp")) or rollups["last_turn_timestamp"]
        latest_turn_summary = _payload_str(session.get("latest_turn_summary")) or rollups["latest_turn_summary"]
    else:
        last_turn_timestamp = _payload_str(session.get("last_turn_timestamp"))
        latest_turn_summary = _payload_str(session.get("latest_turn_summary"))

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
        import_warning=_payload_str(session.get("import_warning")),
        search_text=search_text,
        raw_meta_json=raw_meta_json,
        imported_at=imported_at,
        updated_at=updated_at,
        events=normalized_events,
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
    session_values = (
        parsed.session_id,
        str(parsed.source_path),
        str(parsed.source_root),
        parsed.file_size,
        parsed.file_mtime_ns,
        parsed.content_sha256,
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
        parsed.import_warning,
        parsed.search_text,
        parsed.raw_meta_json,
        parsed.imported_at,
        parsed.updated_at,
    )
    existing_by_id = connection.execute(
        "SELECT 1 FROM sessions WHERE id = ?",
        (parsed.session_id,),
    ).fetchone()
    if existing_by_id is None:
        connection.execute(
            """
            INSERT INTO sessions (
                id, source_path, source_root, file_size, file_mtime_ns, content_sha256,
                session_timestamp, started_at, ended_at, cwd, cwd_name,
                source_host, originator, cli_version, source, model_provider,
                git_branch, git_commit_hash, git_repository_url, github_remote_url,
                github_org, github_repo, github_slug, inferred_project_kind,
                inferred_project_key, inferred_project_label, summary, event_count,
                user_message_count, assistant_message_count, tool_call_count,
                rollup_version, turn_count, last_user_message, last_turn_timestamp,
                latest_turn_summary, command_failure_count, aborted_turn_count,
                import_warning, search_text, raw_meta_json, imported_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            session_values,
        )
    else:
        connection.execute(
            """
            UPDATE sessions
            SET
                source_path = ?,
                source_root = ?,
                file_size = ?,
                file_mtime_ns = ?,
                content_sha256 = ?,
                session_timestamp = ?,
                started_at = ?,
                ended_at = ?,
                cwd = ?,
                cwd_name = ?,
                source_host = ?,
                originator = ?,
                cli_version = ?,
                source = ?,
                model_provider = ?,
                git_branch = ?,
                git_commit_hash = ?,
                git_repository_url = ?,
                github_remote_url = ?,
                github_org = ?,
                github_repo = ?,
                github_slug = ?,
                inferred_project_kind = ?,
                inferred_project_key = ?,
                inferred_project_label = ?,
                summary = ?,
                event_count = ?,
                user_message_count = ?,
                assistant_message_count = ?,
                tool_call_count = ?,
                rollup_version = ?,
                turn_count = ?,
                last_user_message = ?,
                last_turn_timestamp = ?,
                latest_turn_summary = ?,
                command_failure_count = ?,
                aborted_turn_count = ?,
                import_warning = ?,
                search_text = ?,
                raw_meta_json = ?,
                imported_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
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
            s.event_count,
            COUNT(e.id) AS stored_event_count,
            s.updated_at
        FROM sessions AS s
        LEFT JOIN events AS e
            ON e.session_id = s.id
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

    with connect(settings.database_path) as connection:
        with write_transaction(connection):
            if force:
                connection.execute("DELETE FROM events")
                connection.execute("DELETE FROM sessions")

            existing_rows = connection.execute(
                """
                SELECT id, source_path, file_size, file_mtime_ns, content_sha256, source_host
                FROM sessions
                """
            ).fetchall()
            existing_by_source = {
                (row["source_host"], row["source_path"]): {
                    "id": row["id"],
                    "file_size": row["file_size"],
                    "file_mtime_ns": row["file_mtime_ns"],
                    "content_sha256": row["content_sha256"],
                    "source_host": row["source_host"],
                }
                for row in existing_rows
            }

            for source_root, path in iter_session_files(settings.session_roots):
                stat = path.stat()
                record = existing_by_source.get((settings.source_host, str(path)))
                if (
                    not force
                    and record
                    and record["file_size"] == stat.st_size
                    and record["file_mtime_ns"] == stat.st_mtime_ns
                    and record["source_host"] == settings.source_host
                ):
                    skipped += 1
                    continue

                try:
                    parsed = parse_session_file(path, source_root, settings.source_host)
                except SessionParseError as exc:
                    logger.warning("Skipping malformed session file %s", exc)
                    skipped += 1
                    continue
                if project_is_ignored(connection, parsed.inferred_project_key):
                    skipped += 1
                    continue
                upsert_parsed_session(connection, parsed)

                if record:
                    updated += 1
                else:
                    imported += 1

    return {"imported": imported, "updated": updated, "skipped": skipped}
