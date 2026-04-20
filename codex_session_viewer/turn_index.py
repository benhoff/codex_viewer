from __future__ import annotations

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
from .text_utils import shorten, strip_codex_wrappers


TURN_INDEX_VERSION = 3
TURN_SEARCH_VERSION = 2

MAX_PROMPT_SEARCH_CHARS = 8_000
MAX_RESPONSE_SEARCH_CHARS = 12_000
MAX_EVENT_SEARCH_CHARS = 24_000
MAX_EVENT_FRAGMENT_CHARS = 4_000
MAX_PROJECT_SEARCH_CHARS = 2_000


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
                response_text = str(completion_event.get("detail_text") or completion_event.get("display_text") or "")
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

        prompt_text = _compact_search_text(turn.get("prompt_text"), MAX_PROMPT_SEARCH_CHARS)
        response_text = _compact_search_text(response_text, MAX_RESPONSE_SEARCH_CHARS)
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
            if event.get("kind") == "tool_call" and event.get("tool_name") == "apply_patch"
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
        latest_timestamp = _latest_timestamp(
            [response_timestamp, turn.get("prompt_timestamp")] + [event.get("timestamp") for event in all_events]
        )

        return {
            "turn_number": int(turn["number"]),
            "start_event_index": int(turn["start_event_index"]),
            "end_event_index": int(turn["end_event_index"]),
            "prompt_excerpt": shorten(prompt_text, 280),
            "prompt_text": prompt_text,
            "prompt_timestamp": turn.get("prompt_timestamp"),
            "response_excerpt": shorten(response_text, 320) if response_text else "",
            "response_text": response_text,
            "response_timestamp": response_timestamp,
            "response_state": response_state,
            "latest_timestamp": latest_timestamp,
            "command_count": command_count,
            "patch_count": patch_count,
            "failure_count": failure_count,
            "files_touched_count": files_touched_count,
            "event_text": event_text,
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
    rows = compute_session_turn_index(events)
    if rows:
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
                files_touched_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                )
                for row in rows
            ],
        )
    connection.execute(
        "UPDATE sessions SET turn_index_version = ? WHERE id = ?",
        (TURN_INDEX_VERSION, session_id),
    )


def backfill_session_turns(connection: sqlite3.Connection) -> int:
    stale_rows = connection.execute(
        """
        SELECT id
        FROM sessions
        WHERE COALESCE(turn_index_version, 0) < ?
        ORDER BY id ASC
        """,
        (TURN_INDEX_VERSION,),
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

    inserts: list[tuple[Any, ...]] = []
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
                files_touched_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )

    connection.executemany(
        "UPDATE sessions SET turn_index_version = ? WHERE id = ?",
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
) -> list[tuple[Any, ...]]:
    return [
        (
            project_text,
            str(row["prompt_text"] or ""),
            str(row["response_text"] or ""),
            str(row["event_text"] or ""),
            session_id,
            int(row["turn_number"]),
        )
        for row in turns
    ]


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
    project_text = _session_turn_search_project_text(
        _fetch_session_turn_search_metadata(connection, [normalized_session_id]).get(normalized_session_id)
    )
    connection.execute(
        "DELETE FROM session_turn_search WHERE session_id = ?",
        (normalized_session_id,),
    )
    inserts = _session_turn_search_inserts(normalized_session_id, project_text, turns)
    if inserts:
        connection.executemany(
            """
            INSERT INTO session_turn_search (
                project_text,
                prompt_text,
                response_text,
                event_text,
                session_id,
                turn_number
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )
    connection.execute(
        "UPDATE sessions SET turn_search_version = ? WHERE id = ?",
        (TURN_SEARCH_VERSION, normalized_session_id),
    )


def backfill_session_turn_search(connection: sqlite3.Connection) -> int:
    stale_rows = connection.execute(
        """
        SELECT id
        FROM sessions
        WHERE COALESCE(turn_search_version, 0) < ?
        ORDER BY id ASC
        """,
        (TURN_SEARCH_VERSION,),
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
        project_text = _session_turn_search_project_text(metadata_by_session.get(session_id))
        turns = compute_session_turn_index(rows_by_session.get(session_id, []))
        inserts.extend(_session_turn_search_inserts(session_id, project_text, turns))

    if inserts:
        connection.executemany(
            """
            INSERT INTO session_turn_search (
                project_text,
                prompt_text,
                response_text,
                event_text,
                session_id,
                turn_number
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )

    connection.executemany(
        "UPDATE sessions SET turn_search_version = ? WHERE id = ?",
        [(TURN_SEARCH_VERSION, session_id) for session_id in session_ids],
    )
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
        inserts = _session_turn_search_inserts(
            session_id,
            _session_turn_search_project_text(metadata_by_session.get(session_id)),
            turns,
        )
        if inserts:
            connection.executemany(
                """
                INSERT INTO session_turn_search (
                    project_text,
                    prompt_text,
                    response_text,
                    event_text,
                    session_id,
                    turn_number
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                inserts,
            )
        connection.execute(
            "UPDATE sessions SET turn_search_version = ? WHERE id = ?",
            (TURN_SEARCH_VERSION, session_id),
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
