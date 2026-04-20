from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

from .session_status import (
    is_user_turn_start,
    prefers_event_msg_user_turns,
    terminal_turn_summary,
)
from .session_insights import compute_usage_rollup, default_usage_rollup
from .text_utils import shorten, strip_codex_wrappers


ROLLUP_VERSION = 3
TURN_ACTIVITY_ROLLUP_VERSION = 1


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
        "timestamp": _event_value(event, "timestamp"),
        "record_type": _event_value(event, "record_type"),
        "payload_type": _event_value(event, "payload_type"),
        "kind": _event_value(event, "kind"),
        "role": _event_value(event, "role"),
        "display_text": _event_value(event, "display_text"),
        "exit_code": _event_value(event, "exit_code"),
        "record_json": _event_value(event, "record_json"),
    }


def _parse_timestamp(value: object) -> datetime | None:
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
    return parsed.astimezone()


def activity_date_key(value: object) -> str | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return parsed.date().isoformat()


def default_rollups() -> dict[str, Any]:
    rollups = {
        "rollup_version": ROLLUP_VERSION,
        "turn_count": 0,
        "last_user_message": "",
        "last_turn_timestamp": None,
        "latest_turn_summary": None,
        "command_failure_count": 0,
        "aborted_turn_count": 0,
    }
    rollups.update(default_usage_rollup())
    return rollups


def compute_session_turn_activity_daily(
    events: Sequence[sqlite3.Row | dict[str, Any] | object],
) -> list[dict[str, Any]]:
    if not events:
        return []

    compact_events = [_compact_event(event) for event in events]
    prefer_event_msg = prefers_event_msg_user_turns(compact_events)
    by_date: dict[str, dict[str, Any]] = {}

    for event in compact_events:
        if not is_user_turn_start(event, prefer_event_msg):
            continue
        cleaned_prompt = strip_codex_wrappers(str(event.get("display_text") or "")).strip()
        if not cleaned_prompt:
            continue
        timestamp = str(event.get("timestamp") or "").strip()
        date_key = activity_date_key(timestamp)
        if not date_key:
            continue
        item = by_date.setdefault(
            date_key,
            {
                "activity_date": date_key,
                "turn_count": 0,
                "latest_timestamp": "",
                "_latest_dt": None,
            },
        )
        item["turn_count"] = int(item["turn_count"]) + 1
        parsed = _parse_timestamp(timestamp)
        if parsed is not None and (
            item["_latest_dt"] is None or parsed > item["_latest_dt"]
        ):
            item["_latest_dt"] = parsed
            item["latest_timestamp"] = timestamp

    ordered = sorted(by_date.values(), key=lambda item: str(item["activity_date"]))
    for item in ordered:
        item.pop("_latest_dt", None)
    return ordered


def compute_session_rollups(
    events: Sequence[sqlite3.Row | dict[str, Any] | object],
) -> dict[str, Any]:
    if not events:
        return default_rollups()

    compact_events = [_compact_event(event) for event in events]
    prefer_event_msg = prefers_event_msg_user_turns(compact_events)
    usage_rollups = compute_usage_rollup(compact_events)

    turn_count = 0
    last_user_message = ""
    last_turn_timestamp: str | None = None
    command_failure_count = 0
    aborted_turn_count = 0

    for event in compact_events:
        exit_code = event.get("exit_code")
        if event.get("kind") == "command" and isinstance(exit_code, int) and exit_code != 0:
            command_failure_count += 1
        if event.get("record_type") == "event_msg" and event.get("payload_type") == "turn_aborted":
            aborted_turn_count += 1
        if not is_user_turn_start(event, prefer_event_msg):
            continue
        cleaned_prompt = strip_codex_wrappers(str(event.get("display_text") or "")).strip()
        if not cleaned_prompt:
            continue
        turn_count += 1
        last_user_message = shorten(cleaned_prompt, 220)
        timestamp = str(event.get("timestamp") or "").strip()
        last_turn_timestamp = timestamp or last_turn_timestamp

    rollups = {
        "rollup_version": ROLLUP_VERSION,
        "turn_count": turn_count,
        "last_user_message": last_user_message,
        "last_turn_timestamp": last_turn_timestamp,
        "latest_turn_summary": terminal_turn_summary(compact_events),
        "command_failure_count": command_failure_count,
        "aborted_turn_count": aborted_turn_count,
    }
    rollups.update(usage_rollups)
    return rollups


def backfill_session_rollups(connection: sqlite3.Connection) -> int:
    stale_rows = connection.execute(
        """
        SELECT id
        FROM sessions
        WHERE COALESCE(rollup_version, 0) < ?
        ORDER BY id ASC
        """,
        (ROLLUP_VERSION,),
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

    updates: list[tuple[Any, ...]] = []
    for session_id in session_ids:
        rollups = compute_session_rollups(rows_by_session.get(session_id, []))
        updates.append(
            (
                int(rollups["rollup_version"]),
                int(rollups["turn_count"]),
                str(rollups["last_user_message"] or ""),
                rollups["last_turn_timestamp"],
                rollups["latest_turn_summary"],
                int(rollups["command_failure_count"]),
                int(rollups["aborted_turn_count"]),
                rollups["latest_usage_timestamp"],
                int(rollups["latest_input_tokens"] or 0),
                int(rollups["latest_cached_input_tokens"] or 0),
                int(rollups["latest_output_tokens"] or 0),
                int(rollups["latest_reasoning_output_tokens"] or 0),
                int(rollups["latest_total_tokens"] or 0),
                rollups["latest_context_window"],
                rollups["latest_context_remaining_percent"],
                rollups["latest_primary_limit_used_percent"],
                rollups["latest_primary_limit_resets_at"],
                rollups["latest_secondary_limit_used_percent"],
                rollups["latest_secondary_limit_resets_at"],
                rollups["latest_rate_limit_name"],
                rollups["latest_rate_limit_reached_type"],
                session_id,
            )
        )

    connection.executemany(
        """
        UPDATE sessions
        SET
            rollup_version = ?,
            turn_count = ?,
            last_user_message = ?,
            last_turn_timestamp = ?,
            latest_turn_summary = ?,
            command_failure_count = ?,
            aborted_turn_count = ?,
            latest_usage_timestamp = ?,
            latest_input_tokens = ?,
            latest_cached_input_tokens = ?,
            latest_output_tokens = ?,
            latest_reasoning_output_tokens = ?,
            latest_total_tokens = ?,
            latest_context_window = ?,
            latest_context_remaining_percent = ?,
            latest_primary_limit_used_percent = ?,
            latest_primary_limit_resets_at = ?,
            latest_secondary_limit_used_percent = ?,
            latest_secondary_limit_resets_at = ?,
            latest_rate_limit_name = ?,
            latest_rate_limit_reached_type = ?
        WHERE id = ?
        """,
        updates,
    )
    return len(updates)


def replace_session_turn_activity_daily(
    connection: sqlite3.Connection,
    session_id: str,
    events: Sequence[sqlite3.Row | dict[str, Any] | object],
) -> None:
    connection.execute(
        "DELETE FROM session_turn_activity_daily WHERE session_id = ?",
        (session_id,),
    )
    rows = compute_session_turn_activity_daily(events)
    if rows:
        connection.executemany(
            """
            INSERT INTO session_turn_activity_daily (
                session_id,
                activity_date,
                turn_count,
                latest_timestamp
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (
                    session_id,
                    str(row["activity_date"]),
                    int(row["turn_count"]),
                    str(row["latest_timestamp"] or ""),
                )
                for row in rows
            ],
        )
    connection.execute(
        """
        UPDATE sessions
        SET turn_activity_rollup_version = ?
        WHERE id = ?
        """,
        (TURN_ACTIVITY_ROLLUP_VERSION, session_id),
    )


def backfill_session_turn_activity_daily(connection: sqlite3.Connection) -> int:
    stale_rows = connection.execute(
        """
        SELECT id
        FROM sessions
        WHERE COALESCE(turn_activity_rollup_version, 0) < ?
        ORDER BY id ASC
        """,
        (TURN_ACTIVITY_ROLLUP_VERSION,),
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
            display_text
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
        f"DELETE FROM session_turn_activity_daily WHERE session_id IN ({placeholders})",
        session_ids,
    )

    inserts: list[tuple[str, str, int, str]] = []
    for session_id in session_ids:
        for item in compute_session_turn_activity_daily(rows_by_session.get(session_id, [])):
            inserts.append(
                (
                    session_id,
                    str(item["activity_date"]),
                    int(item["turn_count"]),
                    str(item["latest_timestamp"] or ""),
                )
            )

    if inserts:
        connection.executemany(
            """
            INSERT INTO session_turn_activity_daily (
                session_id,
                activity_date,
                turn_count,
                latest_timestamp
            ) VALUES (?, ?, ?, ?)
            """,
            inserts,
        )

    connection.executemany(
        """
        UPDATE sessions
        SET turn_activity_rollup_version = ?
        WHERE id = ?
        """,
        [
            (TURN_ACTIVITY_ROLLUP_VERSION, session_id)
            for session_id in session_ids
        ],
    )
    return len(session_ids)
