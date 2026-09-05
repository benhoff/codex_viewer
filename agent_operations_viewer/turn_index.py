from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from .session_status import (
    abort_display_label,
    is_assistant_final_message,
    is_assistant_update,
    is_task_complete,
    is_turn_aborted,
    is_user_turn_start,
    legacy_terminal_assistant_event,
    prefers_event_msg_user_turns,
)
from .session_insights import compute_usage_rollup
from .text_utils import shorten, strip_codex_wrappers


TURN_INDEX_VERSION = 6
TURN_SEARCH_VERSION = 3
SEARCH_CHUNK_VERSION = 3

MAX_PROMPT_SEARCH_CHARS = 8_000
MAX_RESPONSE_SEARCH_CHARS = 12_000
MAX_EVENT_SEARCH_CHARS = 24_000
MAX_EVENT_FRAGMENT_CHARS = 4_000
MAX_PROJECT_SEARCH_CHARS = 2_000
SEARCH_CHUNK_TARGET_CHARS = 4_000
SEARCH_CHUNK_OVERLAP_CHARS = 400
LEGACY_SEARCH_TRUNCATION_WARNING = "Search text truncated during import"


def _refresh_search_indexed_at(
    connection: sqlite3.Connection,
    session_ids: Sequence[str],
) -> None:
    normalized_ids = sorted(
        {str(session_id or "").strip() for session_id in session_ids if session_id}
    )
    if not normalized_ids:
        return
    indexed_at = datetime.now(tz=UTC).replace(microsecond=0).isoformat()
    connection.executemany(
        """
        UPDATE sessions
        SET search_indexed_at = CASE
            WHEN COALESCE(turn_index_version, 0) >= ?
             AND COALESCE(turn_search_version, 0) >= ?
             AND COALESCE(search_chunk_version, 0) >= ?
            THEN ?
            ELSE NULL
        END
        WHERE id = ?
        """,
        [
            (
                TURN_INDEX_VERSION,
                TURN_SEARCH_VERSION,
                SEARCH_CHUNK_VERSION,
                indexed_at,
                session_id,
            )
            for session_id in normalized_ids
        ],
    )


def _event_value(event: sqlite3.Row | dict[str, Any] | object, key: str) -> Any:
    if isinstance(event, sqlite3.Row):
        try:
            return event[key]
        except (IndexError, KeyError):
            return None
    if isinstance(event, dict):
        return event.get(key)
    return getattr(event, key, None)


def _compact_event(event: sqlite3.Row | dict[str, Any] | object) -> dict[str, Any]:
    return {
        "event_index": _event_value(event, "event_index"),
        "timestamp": _event_value(event, "timestamp"),
        "record_type": _event_value(event, "record_type"),
        "payload_type": _event_value(event, "payload_type"),
        "kind": _event_value(event, "kind"),
        "role": _event_value(event, "role"),
        "display_text": _event_value(event, "display_text"),
        "detail_text": _event_value(event, "detail_text"),
        "tool_name": _event_value(event, "tool_name"),
        "command_text": _event_value(event, "command_text"),
        "exit_code": _event_value(event, "exit_code"),
        "record_json": _event_value(event, "record_json"),
    }


def _normalize_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _latest_timestamp(candidates: Sequence[object]) -> str | None:
    best_raw: str | None = None
    best_dt: datetime | None = None
    for candidate in candidates:
        parsed = _normalize_timestamp(candidate)
        if parsed is None:
            continue
        if best_dt is None or parsed > best_dt:
            best_dt = parsed
            best_raw = str(candidate)
    return best_raw


def _parse_patch_change_count(detail_text: object) -> int:
    if not isinstance(detail_text, str):
        return 0
    text = detail_text.strip()
    if not text or text[0] != "{":
        return 0
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return 0
    if isinstance(parsed, dict):
        return len(parsed)
    return 0


def _is_apply_patch_exec_tool_call(event: dict[str, Any]) -> bool:
    if event.get("kind") != "tool_call" or event.get("tool_name") != "exec_command":
        return False
    text = "\n".join(
        str(event.get(key) or "")
        for key in ("display_text", "command_text", "detail_text")
    ).strip().lower()
    return bool(text and "apply_patch" in text and "*** begin patch" in text)


def _decode_json_string(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith('"') and text.endswith('"'):
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            return text
        if isinstance(decoded, str):
            return decoded.strip()
    return text


def _diff_stat_counts(unified_diff: object) -> tuple[int, int, int]:
    additions = 0
    deletions = 0
    hunks = 0
    for line in _decode_json_string(unified_diff).splitlines():
        if line.startswith("@@"):
            hunks += 1
        elif line.startswith("+") and not line.startswith("+++"):
            additions += 1
        elif line.startswith("-") and not line.startswith("---"):
            deletions += 1
    return additions, deletions, hunks


def _parse_patch_file_changes(event: dict[str, Any]) -> list[dict[str, Any]]:
    if event.get("record_type") != "event_msg" or event.get("payload_type") != "patch_apply_end":
        return []
    detail_text = event.get("detail_text")
    if not isinstance(detail_text, str):
        return []
    text = detail_text.strip()
    if not text or text[0] != "{":
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, dict):
        return []

    changes: list[dict[str, Any]] = []
    for raw_path, metadata in parsed.items():
        path = str(raw_path or "").strip()
        if not path:
            continue
        operation = "update"
        additions = 0
        deletions = 0
        hunks = 0
        if isinstance(metadata, dict):
            operation = str(metadata.get("type") or "update").strip().lower() or "update"
            additions, deletions, hunks = _diff_stat_counts(metadata.get("unified_diff"))
        changes.append(
            {
                "event_index": int(event.get("event_index") or 0),
                "timestamp": event.get("timestamp"),
                "path": path,
                "operation": operation,
                "additions": additions,
                "deletions": deletions,
                "hunks": hunks,
            }
        )
    changes.sort(key=lambda item: (int(item["event_index"]), str(item["path"])))
    return changes


def _trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _compact_search_text(value: object, limit: int) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = " ".join(value.replace("\x00", " ").split())
    if not cleaned:
        return ""
    if len(cleaned) <= limit:
        return cleaned
    return shorten(cleaned, limit)


def _combine_search_fragments(
    fragments: Sequence[str],
    *,
    limit: int,
) -> str:
    combined: list[str] = []
    remaining = limit
    for fragment in fragments:
        text = _compact_search_text(fragment, min(limit, MAX_EVENT_FRAGMENT_CHARS))
        if not text or remaining <= 0:
            continue
        if len(text) > remaining:
            text = shorten(text, remaining)
        if not text:
            continue
        combined.append(text)
        remaining -= len(text)
        if remaining > 0:
            remaining -= 1
    return "\n".join(combined)


def _event_search_text(event: dict[str, Any]) -> str:
    fragments: list[str] = []
    kind = str(event.get("kind") or "")
    role = str(event.get("role") or "")
    tool_name = _compact_search_text(event.get("tool_name"), 120)
    command_text = _compact_search_text(event.get("command_text"), 320)
    display_text = _compact_search_text(event.get("display_text"), MAX_EVENT_FRAGMENT_CHARS)
    detail_text = _compact_search_text(event.get("detail_text"), MAX_EVENT_FRAGMENT_CHARS)

    if kind == "message" and role == "user":
        return ""
    if kind == "tool_call" and tool_name:
        fragments.append(tool_name)
    if kind == "command":
        if command_text:
            fragments.append(command_text)
        exit_code = event.get("exit_code")
        if isinstance(exit_code, int):
            fragments.append(f"exit code {exit_code}")
    elif command_text:
        fragments.append(command_text)

    if display_text:
        fragments.append(display_text)
    if detail_text and detail_text != display_text:
        fragments.append(detail_text)
    return _combine_search_fragments(fragments, limit=MAX_EVENT_FRAGMENT_CHARS)


def _full_search_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.replace("\x00", " ").replace("\r\n", "\n").replace("\r", "\n").strip()


def _record_payload_text(event: dict[str, Any], key: str) -> str:
    raw_record = event.get("record_json")
    if not isinstance(raw_record, str) or not raw_record.strip():
        return ""
    try:
        record = json.loads(raw_record)
    except json.JSONDecodeError:
        return ""
    if not isinstance(record, dict):
        return ""
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return ""
    return _full_search_text(payload.get(key))


def _event_search_text_full(event: dict[str, Any]) -> str:
    fragments: list[str] = []
    kind = str(event.get("kind") or "")
    role = str(event.get("role") or "")
    tool_name = _full_search_text(event.get("tool_name"))
    command_text = _full_search_text(event.get("command_text"))
    display_text = _full_search_text(event.get("display_text"))
    detail_text = _full_search_text(event.get("detail_text"))

    if kind == "message" and role == "user":
        return ""
    if kind == "tool_call" and tool_name:
        fragments.append(tool_name)
    if kind == "command":
        if command_text:
            fragments.append(command_text)
        exit_code = event.get("exit_code")
        if isinstance(exit_code, int):
            fragments.append(f"exit code {exit_code}")
    elif command_text:
        fragments.append(command_text)
    if display_text:
        fragments.append(display_text)
    if detail_text and detail_text != display_text:
        fragments.append(detail_text)
    return "\n".join(fragments)


def _command_search_text_full(event: dict[str, Any]) -> str:
    kind = str(event.get("kind") or "")
    tool_name = _full_search_text(event.get("tool_name"))
    command_text = _full_search_text(event.get("command_text"))
    if (
        not command_text
        and kind != "command"
        and not (kind == "tool_call" and tool_name in {"exec_command", "write_stdin"})
    ):
        return ""
    fragments = [tool_name, command_text]
    if not command_text and kind == "tool_call":
        fragments.extend(
            [
                _full_search_text(event.get("display_text")),
                _full_search_text(event.get("detail_text")),
            ]
        )
    return "\n".join(fragment for fragment in fragments if fragment)


def _tool_output_search_text_full(event: dict[str, Any]) -> str:
    kind = str(event.get("kind") or "")
    payload_type = str(event.get("payload_type") or "")
    if kind not in {"command", "tool_result"} and payload_type not in {
        "exec_command_end",
        "function_call_output",
        "patch_apply_end",
    }:
        return ""
    display_text = _full_search_text(event.get("display_text"))
    detail_text = _full_search_text(event.get("detail_text"))
    return "\n".join(
        fragment
        for fragment in (display_text, detail_text if detail_text != display_text else "")
        if fragment
    )


def _patch_search_text_full(event: dict[str, Any]) -> str:
    """Submitted patch bodies and structured applied diffs, excluding status output."""
    tool = str(event.get("tool_name") or "").split(".")[-1]
    if event.get("kind") == "tool_call" and (tool == "apply_patch" or _is_apply_patch_exec_tool_call(event)):
        candidates = [str(event.get(key) or "") for key in ("display_text", "command_text", "detail_text")]
        text = next((value for value in candidates if "*** Begin Patch" in value), candidates[0])
        try:
            decoded = json.loads(text)
            if isinstance(decoded, dict):
                text = str(decoded.get("patch") or decoded.get("input") or decoded.get("cmd") or "")
            elif isinstance(decoded, str):
                text = decoded
        except (ValueError, TypeError):
            pass
        start, end = text.find("*** Begin Patch"), text.find("*** End Patch")
        if start >= 0 and end >= start:
            return text[start:end + len("*** End Patch")]
        if tool == "apply_patch":
            return text
    if event.get("payload_type") in {"patch_apply_begin", "patch_apply_end"}:
        try:
            record = json.loads(str(event.get("record_json") or "{}"))
            changes = (record.get("payload") or {}).get("changes")
            if not isinstance(changes, dict):
                changes = json.loads(str(event.get("detail_text") or "{}"))
            if not isinstance(changes, dict):
                return ""
            return "\n".join(str(change.get("unified_diff") or "") for _, change in sorted(changes.items()) if isinstance(change, dict))
        except (ValueError, AttributeError):
            return ""
    return ""


def patch_line_ranges(text: str, *, start_offset: int = 0) -> list[dict[str, Any]]:
    lines = []
    offset = start_offset
    for line in text.splitlines(keepends=True):
        kind = "header" if line.startswith(("***", "@@", "---", "+++", "diff ", "index ")) else (
            "addition" if line.startswith("+") else "deletion" if line.startswith("-") else "context"
        )
        lines.append({"kind": kind, "start_offset": offset, "end_offset": offset + len(line)})
        offset += len(line)
    return lines


def split_search_text_chunks(
    value: object,
    *,
    target_chars: int = SEARCH_CHUNK_TARGET_CHARS,
    overlap_chars: int = SEARCH_CHUNK_OVERLAP_CHARS,
) -> list[dict[str, Any]]:
    """Split complete normalized text into deterministic overlapping chunks."""

    text = _full_search_text(value)
    if not text:
        return []
    normalized_target = max(int(target_chars or SEARCH_CHUNK_TARGET_CHARS), 256)
    normalized_overlap = max(0, min(int(overlap_chars or 0), normalized_target // 2))
    chunks: list[dict[str, Any]] = []
    start = 0
    while start < len(text):
        hard_end = min(start + normalized_target, len(text))
        end = hard_end
        if hard_end < len(text):
            boundary_floor = start + (normalized_target // 2)
            newline_boundary = text.rfind("\n", boundary_floor, hard_end + 1)
            space_boundary = text.rfind(" ", boundary_floor, hard_end + 1)
            boundary = max(newline_boundary, space_boundary)
            if boundary >= boundary_floor:
                end = boundary + 1
        raw_chunk = text[start:end]
        leading = len(raw_chunk) - len(raw_chunk.lstrip())
        trailing = len(raw_chunk) - len(raw_chunk.rstrip())
        content_start = start + leading
        content_end = end - trailing if trailing else end
        if content_end > content_start:
            content = text[content_start:content_end]
            chunks.append(
                {
                    "chunk_index": len(chunks),
                    "start_offset": content_start,
                    "end_offset": content_end,
                    "content": content,
                    "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                }
            )
        if end >= len(text):
            break
        next_start = max(end - normalized_overlap, start + 1)
        while next_start < end and text[next_start].isspace():
            next_start += 1
        start = next_start
    return chunks


def compute_session_turn_index(
    events: Sequence[sqlite3.Row | dict[str, Any] | object],
) -> list[dict[str, Any]]:
    if not events:
        return []

    compact_events = [_compact_event(event) for event in events]
    prefer_event_msg = prefers_event_msg_user_turns(compact_events)
    turns: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    def finalize_turn(turn: dict[str, Any]) -> dict[str, Any]:
        assistant_messages: list[dict[str, Any]] = turn["assistant_messages"]
        assistant_updates: list[dict[str, Any]] = turn["assistant_updates"]
        completion_events: list[dict[str, Any]] = turn["completion_events"]
        aborted_events: list[dict[str, Any]] = turn["aborted_events"]
        all_events: list[dict[str, Any]] = turn["events"]

        completion_event = completion_events[-1] if completion_events else None
        final_response_event = None
        if completion_event is not None:
            completed_messages = [
                event
                for event in assistant_messages
                if int(event.get("event_index") or 0) < int(completion_event.get("event_index") or 0)
            ]
            final_response_event = completed_messages[-1] if completed_messages else completion_event
        elif all_events:
            final_response_event = legacy_terminal_assistant_event(all_events)
        update_event = assistant_updates[-1] if assistant_updates else None
        abort_event = aborted_events[-1] if aborted_events else None

        response_state = "missing"
        response_text = ""
        response_timestamp = None

        if completion_event is not None:
            if final_response_event is completion_event:
                response_text = (
                    _record_payload_text(completion_event, "last_agent_message")
                    or str(completion_event.get("detail_text") or completion_event.get("display_text") or "")
                )
            else:
                response_text = str(final_response_event.get("display_text") or "")
            response_timestamp = completion_event.get("timestamp") or final_response_event.get("timestamp")
            response_state = "final"
        elif final_response_event is not None:
            response_text = str(final_response_event.get("display_text") or "")
            response_timestamp = final_response_event.get("timestamp")
            response_state = "update" if is_assistant_update(final_response_event) else "final"
        elif abort_event is not None:
            response_text = abort_display_label(abort_event)
            response_timestamp = abort_event.get("timestamp")
            response_state = "canceled"
        elif update_event is not None:
            response_text = str(update_event.get("display_text") or "")
            response_timestamp = update_event.get("timestamp")
            response_state = "update"

        full_prompt_text = _full_search_text(turn.get("prompt_text"))
        full_response_text = _full_search_text(response_text)
        full_event_text = "\n".join(
            fragment
            for event in all_events
            if event is not final_response_event and event is not completion_event
            if (fragment := _event_search_text_full(event))
        )
        full_command_text = "\n".join(
            fragment
            for event in all_events
            if (fragment := _command_search_text_full(event))
        )
        full_tool_output_text = "\n".join(
            fragment
            for event in all_events
            if (fragment := _tool_output_search_text_full(event))
        )
        prompt_text = _compact_search_text(full_prompt_text, MAX_PROMPT_SEARCH_CHARS)
        response_text = _compact_search_text(full_response_text, MAX_RESPONSE_SEARCH_CHARS)
        event_text = _combine_search_fragments(
            [
                _event_search_text(event)
                for event in all_events
                if event is not final_response_event and event is not completion_event
            ],
            limit=MAX_EVENT_SEARCH_CHARS,
        )
        command_count = sum(
            1
            for event in all_events
            if event.get("kind") == "tool_call" and event.get("tool_name") in {"exec_command", "write_stdin"}
        )
        patch_count = sum(
            1
            for event in all_events
            if event.get("kind") == "tool_call"
            and (event.get("tool_name") == "apply_patch" or _is_apply_patch_exec_tool_call(event))
        )
        failure_count = sum(
            1
            for event in all_events
            if event.get("kind") == "command"
            and isinstance(event.get("exit_code"), int)
            and int(event["exit_code"]) != 0
        )
        files_touched_count = sum(
            _parse_patch_change_count(event.get("detail_text"))
            for event in all_events
            if event.get("record_type") == "event_msg" and event.get("payload_type") == "patch_apply_end"
        )
        file_changes = [
            change
            for event in all_events
            for change in _parse_patch_file_changes(event)
        ]
        full_path_text = "\n".join(
            str(change.get("path") or "").strip()
            for change in file_changes
            if str(change.get("path") or "").strip()
        )
        usage_rollup = compute_usage_rollup(all_events)
        latest_timestamp = _latest_timestamp(
            [response_timestamp, turn.get("prompt_timestamp")] + [event.get("timestamp") for event in all_events]
        )

        return {
            "turn_number": int(turn["number"]),
            "start_event_index": int(turn["start_event_index"]),
            "end_event_index": int(turn["end_event_index"]),
            "prompt_excerpt": shorten(prompt_text, 280),
            "prompt_text": prompt_text,
            "full_prompt_text": full_prompt_text,
            "prompt_timestamp": turn.get("prompt_timestamp"),
            "response_excerpt": shorten(response_text, 320) if response_text else "",
            "response_text": response_text,
            "full_response_text": full_response_text,
            "response_timestamp": response_timestamp,
            "response_state": response_state,
            "latest_timestamp": latest_timestamp,
            "command_count": command_count,
            "patch_count": patch_count,
            "failure_count": failure_count,
            "files_touched_count": files_touched_count,
            "file_changes": file_changes,
            "latest_usage_timestamp": usage_rollup["latest_usage_timestamp"],
            "latest_input_tokens": int(usage_rollup["latest_input_tokens"] or 0),
            "latest_cached_input_tokens": int(usage_rollup["latest_cached_input_tokens"] or 0),
            "latest_output_tokens": int(usage_rollup["latest_output_tokens"] or 0),
            "latest_reasoning_output_tokens": int(usage_rollup["latest_reasoning_output_tokens"] or 0),
            "latest_total_tokens": int(usage_rollup["latest_total_tokens"] or 0),
            "latest_context_window": usage_rollup["latest_context_window"],
            "latest_context_remaining_percent": usage_rollup["latest_context_remaining_percent"],
            "latest_primary_limit_used_percent": usage_rollup["latest_primary_limit_used_percent"],
            "latest_primary_limit_resets_at": usage_rollup["latest_primary_limit_resets_at"],
            "latest_secondary_limit_used_percent": usage_rollup["latest_secondary_limit_used_percent"],
            "latest_secondary_limit_resets_at": usage_rollup["latest_secondary_limit_resets_at"],
            "latest_rate_limit_name": usage_rollup["latest_rate_limit_name"],
            "latest_rate_limit_reached_type": usage_rollup["latest_rate_limit_reached_type"],
            "event_text": event_text,
            "full_event_text": full_event_text,
            "full_patch_text": "\n".join(text for event in all_events if (text := _patch_search_text_full(event))),
            "command_text": _compact_search_text(full_command_text, MAX_EVENT_SEARCH_CHARS),
            "full_command_text": full_command_text,
            "tool_output_text": _compact_search_text(
                full_tool_output_text,
                MAX_EVENT_SEARCH_CHARS,
            ),
            "full_tool_output_text": full_tool_output_text,
            "path_text": _compact_search_text(full_path_text, MAX_EVENT_SEARCH_CHARS),
        }

    for event in compact_events:
        if is_user_turn_start(event, prefer_event_msg):
            cleaned_prompt = strip_codex_wrappers(str(event.get("display_text") or "")).strip()
            if not cleaned_prompt:
                continue
            if current is not None:
                turns.append(finalize_turn(current))
            current = {
                "number": len(turns) + 1,
                "start_event_index": int(event.get("event_index") or 0),
                "end_event_index": int(event.get("event_index") or 0),
                "prompt_text": cleaned_prompt,
                "prompt_timestamp": event.get("timestamp"),
                "events": [],
                "assistant_messages": [],
                "assistant_updates": [],
                "completion_events": [],
                "aborted_events": [],
            }
            continue

        if current is None:
            continue

        current["events"].append(event)
        current["end_event_index"] = int(event.get("event_index") or current["end_event_index"])
        if is_assistant_final_message(event):
            current["assistant_messages"].append(event)
        elif is_assistant_update(event):
            current["assistant_updates"].append(event)
        elif is_task_complete(event):
            current["completion_events"].append(event)
        elif is_turn_aborted(event):
            current["aborted_events"].append(event)

    if current is not None:
        turns.append(finalize_turn(current))

    return turns


def replace_session_turns(
    connection: sqlite3.Connection,
    session_id: str,
    events: Sequence[sqlite3.Row | dict[str, Any] | object],
) -> None:
    connection.execute("DELETE FROM session_turns WHERE session_id = ?", (session_id,))
    connection.execute("DELETE FROM session_file_changes WHERE session_id = ?", (session_id,))
    rows = compute_session_turn_index(events)
    _insert_session_turn_rows(connection, session_id, rows)
    connection.execute(
        """
        UPDATE sessions
        SET
            turn_index_version = ?,
            search_chunk_version = 0,
            search_indexed_at = NULL
        WHERE id = ?
        """,
        (TURN_INDEX_VERSION, session_id),
    )


def _insert_session_turn_rows(
    connection: sqlite3.Connection,
    session_id: str,
    rows: Sequence[dict[str, Any]],
) -> None:
    if not rows:
        return
    connection.executemany(
        """
        INSERT INTO session_turns (
            session_id,
            turn_number,
            start_event_index,
            end_event_index,
            prompt_excerpt,
            prompt_timestamp,
            response_excerpt,
            response_timestamp,
            response_state,
            latest_timestamp,
            command_count,
            patch_count,
            failure_count,
            files_touched_count,
            latest_usage_timestamp,
            latest_input_tokens,
            latest_cached_input_tokens,
            latest_output_tokens,
            latest_reasoning_output_tokens,
            latest_total_tokens,
            latest_context_window,
            latest_context_remaining_percent,
            latest_primary_limit_used_percent,
            latest_primary_limit_resets_at,
            latest_secondary_limit_used_percent,
            latest_secondary_limit_resets_at,
            latest_rate_limit_name,
            latest_rate_limit_reached_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                session_id,
                int(row["turn_number"]),
                int(row["start_event_index"]),
                int(row["end_event_index"]),
                str(row["prompt_excerpt"] or ""),
                row["prompt_timestamp"],
                str(row["response_excerpt"] or ""),
                row["response_timestamp"],
                str(row["response_state"] or "missing"),
                row["latest_timestamp"],
                int(row["command_count"] or 0),
                int(row["patch_count"] or 0),
                int(row["failure_count"] or 0),
                int(row["files_touched_count"] or 0),
                row["latest_usage_timestamp"],
                int(row["latest_input_tokens"] or 0),
                int(row["latest_cached_input_tokens"] or 0),
                int(row["latest_output_tokens"] or 0),
                int(row["latest_reasoning_output_tokens"] or 0),
                int(row["latest_total_tokens"] or 0),
                row["latest_context_window"],
                row["latest_context_remaining_percent"],
                row["latest_primary_limit_used_percent"],
                row["latest_primary_limit_resets_at"],
                row["latest_secondary_limit_used_percent"],
                row["latest_secondary_limit_resets_at"],
                row["latest_rate_limit_name"],
                row["latest_rate_limit_reached_type"],
            )
            for row in rows
        ],
    )
    file_change_rows: list[tuple[Any, ...]] = []
    for row in rows:
        turn_number = int(row["turn_number"])
        for change in row.get("file_changes", []):
            if not isinstance(change, dict):
                continue
            path = str(change.get("path") or "").strip()
            if not path:
                continue
            file_change_rows.append(
                (
                    session_id,
                    turn_number,
                    int(change.get("event_index") or 0),
                    path,
                    str(change.get("operation") or "update").strip() or "update",
                    int(change.get("additions") or 0),
                    int(change.get("deletions") or 0),
                    int(change.get("hunks") or 0),
                    change.get("timestamp"),
                )
            )
    if file_change_rows:
        connection.executemany(
            """
            INSERT OR REPLACE INTO session_file_changes (
                session_id,
                turn_number,
                event_index,
                path,
                operation,
                additions,
                deletions,
                hunks,
                timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            file_change_rows,
        )


def backfill_session_turns(
    connection: sqlite3.Connection,
    *,
    batch_size: int | None = None,
) -> int:
    normalized_batch_size = (
        max(1, min(int(batch_size), 500)) if batch_size is not None else None
    )
    limit_sql = " LIMIT ?" if normalized_batch_size is not None else ""
    params: tuple[int, ...] = (TURN_INDEX_VERSION,)
    if normalized_batch_size is not None:
        params = (*params, normalized_batch_size)
    stale_rows = connection.execute(
        f"""
        SELECT id
        FROM sessions
        WHERE COALESCE(turn_index_version, 0) < ?
        ORDER BY id ASC
        {limit_sql}
        """,
        params,
    ).fetchall()
    session_ids = [str(row["id"]) for row in stale_rows]
    if not session_ids:
        return 0

    placeholders = ", ".join("?" for _ in session_ids)
    event_rows = connection.execute(
        f"""
        SELECT
            session_id,
            event_index,
            timestamp,
            record_type,
            payload_type,
            kind,
            role,
            display_text,
            detail_text,
            tool_name,
            command_text,
            exit_code,
            record_json
        FROM events
        WHERE session_id IN ({placeholders})
        ORDER BY session_id ASC, event_index ASC
        """,
        session_ids,
    ).fetchall()

    rows_by_session: dict[str, list[sqlite3.Row]] = {}
    for row in event_rows:
        rows_by_session.setdefault(str(row["session_id"]), []).append(row)

    connection.execute(
        f"DELETE FROM session_turns WHERE session_id IN ({placeholders})",
        session_ids,
    )
    connection.execute(
        f"DELETE FROM session_file_changes WHERE session_id IN ({placeholders})",
        session_ids,
    )

    inserts: list[tuple[Any, ...]] = []
    file_change_inserts: list[tuple[Any, ...]] = []
    for session_id in session_ids:
        for row in compute_session_turn_index(rows_by_session.get(session_id, [])):
            inserts.append(
                (
                    session_id,
                    int(row["turn_number"]),
                    int(row["start_event_index"]),
                    int(row["end_event_index"]),
                    str(row["prompt_excerpt"] or ""),
                    row["prompt_timestamp"],
                    str(row["response_excerpt"] or ""),
                    row["response_timestamp"],
                    str(row["response_state"] or "missing"),
                    row["latest_timestamp"],
                    int(row["command_count"] or 0),
                    int(row["patch_count"] or 0),
                    int(row["failure_count"] or 0),
                    int(row["files_touched_count"] or 0),
                    row["latest_usage_timestamp"],
                    int(row["latest_input_tokens"] or 0),
                    int(row["latest_cached_input_tokens"] or 0),
                    int(row["latest_output_tokens"] or 0),
                    int(row["latest_reasoning_output_tokens"] or 0),
                    int(row["latest_total_tokens"] or 0),
                    row["latest_context_window"],
                    row["latest_context_remaining_percent"],
                    row["latest_primary_limit_used_percent"],
                    row["latest_primary_limit_resets_at"],
                    row["latest_secondary_limit_used_percent"],
                    row["latest_secondary_limit_resets_at"],
                    row["latest_rate_limit_name"],
                    row["latest_rate_limit_reached_type"],
                )
            )
            turn_number = int(row["turn_number"])
            for change in row.get("file_changes", []):
                if not isinstance(change, dict):
                    continue
                path = str(change.get("path") or "").strip()
                if not path:
                    continue
                file_change_inserts.append(
                    (
                        session_id,
                        turn_number,
                        int(change.get("event_index") or 0),
                        path,
                        str(change.get("operation") or "update").strip() or "update",
                        int(change.get("additions") or 0),
                        int(change.get("deletions") or 0),
                        int(change.get("hunks") or 0),
                        change.get("timestamp"),
                    )
                )

    if inserts:
        connection.executemany(
            """
            INSERT INTO session_turns (
                session_id,
                turn_number,
                start_event_index,
                end_event_index,
                prompt_excerpt,
                prompt_timestamp,
                response_excerpt,
                response_timestamp,
                response_state,
                latest_timestamp,
                command_count,
                patch_count,
                failure_count,
                files_touched_count,
                latest_usage_timestamp,
                latest_input_tokens,
                latest_cached_input_tokens,
                latest_output_tokens,
                latest_reasoning_output_tokens,
                latest_total_tokens,
                latest_context_window,
                latest_context_remaining_percent,
                latest_primary_limit_used_percent,
                latest_primary_limit_resets_at,
                latest_secondary_limit_used_percent,
                latest_secondary_limit_resets_at,
                latest_rate_limit_name,
                latest_rate_limit_reached_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )
    if file_change_inserts:
        connection.executemany(
            """
            INSERT OR REPLACE INTO session_file_changes (
                session_id,
                turn_number,
                event_index,
                path,
                operation,
                additions,
                deletions,
                hunks,
                timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            file_change_inserts,
        )

    connection.executemany(
        """
        UPDATE sessions
        SET
            turn_index_version = ?,
            search_chunk_version = 0,
            search_indexed_at = NULL
        WHERE id = ?
        """,
        [(TURN_INDEX_VERSION, session_id) for session_id in session_ids],
    )
    return len(session_ids)


def _session_turn_search_project_text(session_row: sqlite3.Row | dict[str, Any] | None) -> str:
    if session_row is None:
        return ""
    source_host = _trimmed(_event_value(session_row, "source_host")) or "unknown-host"
    organization = (
        _trimmed(_event_value(session_row, "override_organization"))
        or _trimmed(_event_value(session_row, "github_org"))
        or source_host
    )
    repository = (
        _trimmed(_event_value(session_row, "override_repository"))
        or _trimmed(_event_value(session_row, "github_repo"))
        or _trimmed(_event_value(session_row, "cwd_name"))
        or _trimmed(_event_value(session_row, "cwd"))
        or "unassigned"
    )
    display_label = (
        _trimmed(_event_value(session_row, "override_display_label"))
        or (f"{organization}/{repository}" if organization and repository else repository)
        or _trimmed(_event_value(session_row, "inferred_project_label"))
        or _trimmed(_event_value(session_row, "inferred_project_key"))
        or ""
    )
    fragments: list[str] = []
    for candidate in (
        display_label,
        _trimmed(_event_value(session_row, "override_display_label")),
        organization,
        repository,
        _trimmed(_event_value(session_row, "override_group_key")),
        _trimmed(_event_value(session_row, "inferred_project_label")),
        _trimmed(_event_value(session_row, "inferred_project_key")),
        _trimmed(_event_value(session_row, "github_slug")),
        _trimmed(_event_value(session_row, "github_org")),
        _trimmed(_event_value(session_row, "github_repo")),
        _trimmed(_event_value(session_row, "source_host")),
        _trimmed(_event_value(session_row, "cwd")),
        _trimmed(_event_value(session_row, "cwd_name")),
        _trimmed(_event_value(session_row, "override_remote_url")),
        _trimmed(_event_value(session_row, "github_remote_url")),
        _trimmed(_event_value(session_row, "git_repository_url")),
    ):
        if candidate and candidate not in fragments:
            fragments.append(candidate)
    return _combine_search_fragments(fragments, limit=MAX_PROJECT_SEARCH_CHARS)


def _fetch_session_turn_search_metadata(
    connection: sqlite3.Connection,
    session_ids: Sequence[str],
) -> dict[str, sqlite3.Row]:
    normalized_ids = [str(session_id) for session_id in session_ids if session_id]
    if not normalized_ids:
        return {}
    placeholders = ", ".join("?" for _ in normalized_ids)
    rows = connection.execute(
        f"""
        SELECT
            s.id,
            s.cwd,
            s.cwd_name,
            s.source_host,
            s.git_repository_url,
            s.git_commit_hash,
            s.github_remote_url,
            s.github_org,
            s.github_repo,
            s.github_slug,
            s.inferred_project_key,
            s.inferred_project_label,
            o.override_group_key,
            o.override_organization,
            o.override_repository,
            o.override_remote_url,
            o.override_display_label
        FROM sessions AS s
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        WHERE s.id IN ({placeholders})
        """,
        normalized_ids,
    ).fetchall()
    return {str(row["id"]): row for row in rows}


def _fetch_turn_search_events(
    connection: sqlite3.Connection,
    session_ids: Sequence[str],
) -> dict[str, list[sqlite3.Row]]:
    normalized_ids = [str(session_id) for session_id in session_ids if session_id]
    if not normalized_ids:
        return {}
    placeholders = ", ".join("?" for _ in normalized_ids)
    rows = connection.execute(
        f"""
        SELECT
            session_id,
            event_index,
            timestamp,
            record_type,
            payload_type,
            kind,
            role,
            display_text,
            detail_text,
            tool_name,
            command_text,
            exit_code,
            record_json
        FROM events
        WHERE session_id IN ({placeholders})
        ORDER BY session_id ASC, event_index ASC
        """,
        normalized_ids,
    ).fetchall()
    rows_by_session: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        rows_by_session.setdefault(str(row["session_id"]), []).append(row)
    return rows_by_session


def _session_turn_search_inserts(
    session_id: str,
    project_text: str,
    turns: Sequence[dict[str, Any]],
    *,
    commit_id_text: str = "",
) -> list[tuple[Any, ...]]:
    return [
        (
            project_text,
            str(row["prompt_text"] or ""),
            str(row["response_text"] or ""),
            str(row["event_text"] or ""),
            str(row.get("command_text") or ""),
            str(row.get("path_text") or ""),
            commit_id_text,
            str(row.get("tool_output_text") or ""),
            session_id,
            int(row["turn_number"]),
        )
        for row in turns
    ]


def _session_search_chunk_records(
    session_id: str,
    project_text: str,
    turns: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for turn in turns:
        turn_number = int(turn["turn_number"])
        for field, value in (
            ("prompt", turn.get("full_prompt_text")),
            ("response", turn.get("full_response_text")),
            ("activity", turn.get("full_event_text")),
            ("commands", turn.get("full_command_text")),
            ("tool_output", turn.get("full_tool_output_text")),
            ("patches", turn.get("full_patch_text")),
        ):
            for chunk in split_search_text_chunks(value):
                content_sha256 = str(chunk["content_sha256"])
                stable_key = "\0".join(
                    (
                        str(SEARCH_CHUNK_VERSION),
                        session_id,
                        str(turn_number),
                        field,
                        str(chunk["chunk_index"]),
                        content_sha256,
                    )
                )
                records.append(
                    {
                        "chunk_id": hashlib.sha256(stable_key.encode("utf-8")).hexdigest(),
                        "session_id": session_id,
                        "turn_number": turn_number,
                        "field": field,
                        "chunk_index": int(chunk["chunk_index"]),
                        "start_offset": int(chunk["start_offset"]),
                        "end_offset": int(chunk["end_offset"]),
                        "content_sha256": content_sha256,
                        "content": str(chunk["content"]),
                        "project_text": project_text,
                    }
                )
    return records


def _insert_session_search_chunk_records(
    connection: sqlite3.Connection,
    records: Sequence[dict[str, Any]],
) -> None:
    if not records:
        return

    connection.executemany(
        """
        INSERT INTO session_search_chunks (
            chunk_id,
            session_id,
            turn_number,
            field,
            chunk_index,
            start_offset,
            end_offset,
            content_sha256,
            index_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                record["chunk_id"],
                record["session_id"],
                record["turn_number"],
                record["field"],
                record["chunk_index"],
                record["start_offset"],
                record["end_offset"],
                record["content_sha256"],
                SEARCH_CHUNK_VERSION,
            )
            for record in records
        ],
    )

    session_id = str(records[0]["session_id"])
    rowid_by_chunk_id = {
        str(row["chunk_id"]): int(row["rowid"])
        for row in connection.execute(
            "SELECT rowid, chunk_id FROM session_search_chunks WHERE session_id = ?",
            (session_id,),
        ).fetchall()
    }
    connection.executemany(
        """
        INSERT INTO session_search_chunk_fts (
            rowid,
            content,
            project_text,
            chunk_id,
            session_id,
            turn_number,
            field
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                rowid_by_chunk_id[str(record["chunk_id"])],
                record["content"],
                record["project_text"],
                record["chunk_id"],
                record["session_id"],
                record["turn_number"],
                record["field"],
            )
            for record in records
        ],
    )


def _write_session_search_chunks(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    project_text: str,
    turns: Sequence[dict[str, Any]],
) -> int:
    connection.execute(
        "DELETE FROM session_search_chunks WHERE session_id = ?",
        (session_id,),
    )
    records = _session_search_chunk_records(session_id, project_text, turns)
    _insert_session_search_chunk_records(connection, records)
    if records:
        connection.execute(
            """
            UPDATE sessions
            SET
                search_chunk_version = ?,
                import_warning = CASE
                    WHEN import_warning = ? THEN NULL
                    ELSE import_warning
                END
            WHERE id = ?
            """,
            (
                SEARCH_CHUNK_VERSION,
                LEGACY_SEARCH_TRUNCATION_WARNING,
                session_id,
            ),
        )
    else:
        connection.execute(
            "UPDATE sessions SET search_chunk_version = ? WHERE id = ?",
            (SEARCH_CHUNK_VERSION, session_id),
        )
    _refresh_search_indexed_at(connection, [session_id])
    return len(records)


def replace_session_search_chunks(
    connection: sqlite3.Connection,
    session_id: str,
    events: Sequence[sqlite3.Row | dict[str, Any] | object] | None = None,
) -> int:
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return 0
    source_events = (
        events
        if events is not None
        else _fetch_turn_search_events(connection, [normalized_session_id]).get(normalized_session_id, [])
    )
    turns = compute_session_turn_index(source_events)
    metadata = _fetch_session_turn_search_metadata(connection, [normalized_session_id]).get(
        normalized_session_id
    )
    return _write_session_search_chunks(
        connection,
        session_id=normalized_session_id,
        project_text=_session_turn_search_project_text(metadata),
        turns=turns,
    )


def backfill_session_search_chunks(
    connection: sqlite3.Connection,
    *,
    batch_size: int = 50,
) -> int:
    normalized_batch_size = max(1, min(int(batch_size or 50), 500))
    stale_rows = connection.execute(
        """
        SELECT id
        FROM sessions
        WHERE COALESCE(search_chunk_version, 0) < ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (SEARCH_CHUNK_VERSION, normalized_batch_size),
    ).fetchall()
    session_ids = [str(row["id"]) for row in stale_rows]
    if not session_ids:
        return 0

    rows_by_session = _fetch_turn_search_events(connection, session_ids)
    metadata_by_session = _fetch_session_turn_search_metadata(connection, session_ids)
    for session_id in session_ids:
        _write_session_search_chunks(
            connection,
            session_id=session_id,
            project_text=_session_turn_search_project_text(metadata_by_session.get(session_id)),
            turns=compute_session_turn_index(rows_by_session.get(session_id, [])),
        )
    return len(session_ids)


def replace_session_turn_search(
    connection: sqlite3.Connection,
    session_id: str,
    events: Sequence[sqlite3.Row | dict[str, Any] | object] | None = None,
) -> None:
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return
    turns = compute_session_turn_index(
        events if events is not None else _fetch_turn_search_events(connection, [normalized_session_id]).get(normalized_session_id, [])
    )
    metadata = _fetch_session_turn_search_metadata(connection, [normalized_session_id]).get(
        normalized_session_id
    )
    project_text = _session_turn_search_project_text(metadata)
    connection.execute(
        "DELETE FROM session_turn_search WHERE session_id = ?",
        (normalized_session_id,),
    )
    inserts = _session_turn_search_inserts(
        normalized_session_id,
        project_text,
        turns,
        commit_id_text=_trimmed(_event_value(metadata, "git_commit_hash")),
    )
    if inserts:
        connection.executemany(
            """
            INSERT INTO session_turn_search (
                project_text,
                prompt_text,
                response_text,
                event_text,
                command_text,
                path_text,
                commit_id_text,
                tool_output_text,
                session_id,
                turn_number
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )
    connection.execute(
        "UPDATE sessions SET turn_search_version = ? WHERE id = ?",
        (TURN_SEARCH_VERSION, normalized_session_id),
    )
    _refresh_search_indexed_at(connection, [normalized_session_id])


def replace_session_turn_suffix(
    connection: sqlite3.Connection,
    session_id: str,
    events: Sequence[sqlite3.Row | dict[str, Any] | object],
    *,
    start_turn_number: int,
) -> list[dict[str, Any]]:
    """Replace only the open/new turn suffix after an append-only sync."""
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return []
    normalized_start = max(1, int(start_turn_number or 1))
    rows = compute_session_turn_index(events)
    for row in rows:
        row["turn_number"] = int(row["turn_number"]) + normalized_start - 1

    suffix_params = (normalized_session_id, normalized_start)
    connection.execute(
        "DELETE FROM session_turn_search WHERE session_id = ? AND CAST(turn_number AS INTEGER) >= ?",
        suffix_params,
    )
    connection.execute(
        "DELETE FROM session_search_chunks WHERE session_id = ? AND turn_number >= ?",
        suffix_params,
    )
    connection.execute(
        "DELETE FROM session_file_changes WHERE session_id = ? AND turn_number >= ?",
        suffix_params,
    )
    connection.execute(
        "DELETE FROM session_turns WHERE session_id = ? AND turn_number >= ?",
        suffix_params,
    )
    _insert_session_turn_rows(connection, normalized_session_id, rows)

    metadata = _fetch_session_turn_search_metadata(connection, [normalized_session_id]).get(
        normalized_session_id
    )
    project_text = _session_turn_search_project_text(metadata)
    turn_search_inserts = _session_turn_search_inserts(
        normalized_session_id,
        project_text,
        rows,
        commit_id_text=_trimmed(_event_value(metadata, "git_commit_hash")),
    )
    if turn_search_inserts:
        connection.executemany(
            """
            INSERT INTO session_turn_search (
                project_text,
                prompt_text,
                response_text,
                event_text,
                command_text,
                path_text,
                commit_id_text,
                tool_output_text,
                session_id,
                turn_number
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            turn_search_inserts,
        )

    chunk_records = _session_search_chunk_records(
        normalized_session_id,
        project_text,
        rows,
    )
    _insert_session_search_chunk_records(connection, chunk_records)

    connection.execute(
        """
        UPDATE sessions
        SET
            turn_index_version = ?,
            turn_search_version = ?,
            search_chunk_version = ?,
            import_warning = CASE
                WHEN import_warning = ? THEN NULL
                ELSE import_warning
            END
        WHERE id = ?
        """,
        (
            TURN_INDEX_VERSION,
            TURN_SEARCH_VERSION,
            SEARCH_CHUNK_VERSION,
            LEGACY_SEARCH_TRUNCATION_WARNING,
            normalized_session_id,
        ),
    )
    _refresh_search_indexed_at(connection, [normalized_session_id])
    return rows


def backfill_session_turn_search(
    connection: sqlite3.Connection,
    *,
    batch_size: int | None = None,
) -> int:
    normalized_batch_size = (
        max(1, min(int(batch_size), 500)) if batch_size is not None else None
    )
    limit_sql = " LIMIT ?" if normalized_batch_size is not None else ""
    params: tuple[int, ...] = (TURN_SEARCH_VERSION,)
    if normalized_batch_size is not None:
        params = (*params, normalized_batch_size)
    stale_rows = connection.execute(
        f"""
        SELECT id
        FROM sessions
        WHERE COALESCE(turn_search_version, 0) < ?
        ORDER BY id ASC
        {limit_sql}
        """,
        params,
    ).fetchall()
    session_ids = [str(row["id"]) for row in stale_rows]
    if not session_ids:
        return 0

    placeholders = ", ".join("?" for _ in session_ids)
    rows_by_session = _fetch_turn_search_events(connection, session_ids)
    metadata_by_session = _fetch_session_turn_search_metadata(connection, session_ids)
    connection.execute(
        f"DELETE FROM session_turn_search WHERE session_id IN ({placeholders})",
        session_ids,
    )

    inserts: list[tuple[Any, ...]] = []
    for session_id in session_ids:
        metadata = metadata_by_session.get(session_id)
        project_text = _session_turn_search_project_text(metadata)
        turns = compute_session_turn_index(rows_by_session.get(session_id, []))
        inserts.extend(
            _session_turn_search_inserts(
                session_id,
                project_text,
                turns,
                commit_id_text=_trimmed(_event_value(metadata, "git_commit_hash")),
            )
        )

    if inserts:
        connection.executemany(
            """
            INSERT INTO session_turn_search (
                project_text,
                prompt_text,
                response_text,
                event_text,
                command_text,
                path_text,
                commit_id_text,
                tool_output_text,
                session_id,
                turn_number
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )

    connection.executemany(
        "UPDATE sessions SET turn_search_version = ? WHERE id = ?",
        [(TURN_SEARCH_VERSION, session_id) for session_id in session_ids],
    )
    _refresh_search_indexed_at(connection, session_ids)
    return len(session_ids)


def reindex_session_turn_search_for_project_keys(
    connection: sqlite3.Connection,
    project_keys: Sequence[str],
) -> int:
    keys = sorted({str(key).strip() for key in project_keys if str(key).strip()})
    if not keys:
        return 0
    placeholders = ", ".join("?" for _ in keys)
    rows = connection.execute(
        f"""
        SELECT id
        FROM sessions
        WHERE inferred_project_key IN ({placeholders})
        ORDER BY id ASC
        """,
        keys,
    ).fetchall()
    session_ids = [str(row["id"]) for row in rows]
    rows_by_session = _fetch_turn_search_events(connection, session_ids)
    metadata_by_session = _fetch_session_turn_search_metadata(connection, session_ids)
    for session_id in session_ids:
        connection.execute(
            "DELETE FROM session_turn_search WHERE session_id = ?",
            (session_id,),
        )
        turns = compute_session_turn_index(rows_by_session.get(session_id, []))
        project_text = _session_turn_search_project_text(metadata_by_session.get(session_id))
        inserts = _session_turn_search_inserts(
            session_id,
            project_text,
            turns,
            commit_id_text=_trimmed(
                _event_value(metadata_by_session.get(session_id), "git_commit_hash")
            ),
        )
        if inserts:
            connection.executemany(
                """
                INSERT INTO session_turn_search (
                    project_text,
                    prompt_text,
                    response_text,
                    event_text,
                    command_text,
                    path_text,
                    commit_id_text,
                    tool_output_text,
                    session_id,
                    turn_number
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                inserts,
            )
        connection.execute(
            "UPDATE sessions SET turn_search_version = ? WHERE id = ?",
            (TURN_SEARCH_VERSION, session_id),
        )
        _write_session_search_chunks(
            connection,
            session_id=session_id,
            project_text=project_text,
            turns=turns,
        )
    return len(session_ids)


def turn_window_size(view_mode: str) -> int:
    return 5 if str(view_mode or "").strip().lower() == "audit" else 10


def fetch_session_turn_window(
    connection: sqlite3.Connection,
    session_id: str,
    *,
    window_size: int,
    turn_number: int | None = None,
    before_turn: int | None = None,
    page_number: int | None = None,
) -> dict[str, Any]:
    total_row = connection.execute(
        "SELECT COUNT(*) AS count FROM session_turns WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    total_turns = int(total_row["count"] or 0) if total_row is not None else 0
    if total_turns <= 0:
        return {
            "total_turns": 0,
            "window_size": window_size,
            "total_pages": 0,
            "current_page": 1,
            "oldest_turn": None,
            "newest_turn": None,
            "display_turns": [],
            "context_turn": None,
            "event_start_index": None,
            "event_end_index": None,
            "has_older": False,
            "has_newer": False,
            "older_before_turn": None,
            "newer_turn": None,
            "older_page": None,
            "newer_page": None,
        }

    normalized_window = max(int(window_size or 10), 1)
    total_pages = max((total_turns + normalized_window - 1) // normalized_window, 1)
    target_turn = max(1, min(int(turn_number or 0), total_turns)) if turn_number else None
    older_cursor = max(1, min(int(before_turn or 0), total_turns + 1)) if before_turn else None
    explicit_page = max(1, min(int(page_number or 0), total_pages)) if page_number else None

    if target_turn is not None:
        page_index = (total_turns - target_turn) // normalized_window
        current_page = page_index + 1
        newest_turn = max(1, total_turns - (page_index * normalized_window))
    elif explicit_page is not None:
        current_page = explicit_page
        page_index = current_page - 1
        newest_turn = max(1, total_turns - (page_index * normalized_window))
    elif older_cursor is not None:
        newest_turn = max(1, min(total_turns, older_cursor - 1))
        current_page = ((total_turns - newest_turn) // normalized_window) + 1
    else:
        current_page = 1
        newest_turn = total_turns

    oldest_turn = max(1, newest_turn - normalized_window + 1)
    rows = connection.execute(
        """
        SELECT *
        FROM session_turns
        WHERE session_id = ?
          AND turn_number BETWEEN ? AND ?
        ORDER BY turn_number ASC
        """,
        (session_id, oldest_turn, newest_turn),
    ).fetchall()

    context_turn = None
    if oldest_turn > 1:
        context_turn = connection.execute(
            """
            SELECT *
            FROM session_turns
            WHERE session_id = ?
              AND turn_number = ?
            """,
            (session_id, oldest_turn - 1),
        ).fetchone()

    display_turns = [dict(row) for row in rows]
    context_turn_dict = dict(context_turn) if context_turn is not None else None
    event_start_index = None
    event_end_index = None
    if display_turns:
        first_turn = context_turn_dict or display_turns[0]
        event_start_index = int(first_turn["start_event_index"])
        event_end_index = int(display_turns[-1]["end_event_index"])

    return {
        "total_turns": total_turns,
        "window_size": normalized_window,
        "total_pages": total_pages,
        "current_page": current_page,
        "oldest_turn": oldest_turn,
        "newest_turn": newest_turn,
        "display_turns": display_turns,
        "context_turn": context_turn_dict,
        "event_start_index": event_start_index,
        "event_end_index": event_end_index,
        "has_older": oldest_turn > 1,
        "has_newer": newest_turn < total_turns,
        "older_before_turn": oldest_turn if oldest_turn > 1 else None,
        "newer_turn": newest_turn + 1 if newest_turn < total_turns else None,
        "older_page": current_page + 1 if current_page < total_pages else None,
        "newer_page": current_page - 1 if current_page > 1 else None,
    }
