from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

from .session_view import parse_timestamp


ACTION_QUEUE_STATE_STATUSES = {"resolved", "snoozed", "ignored"}
DEFAULT_ACTION_QUEUE_SNOOZE = timedelta(hours=24)


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def action_queue_fingerprint(
    *,
    project_key: str,
    host: str,
    issue_kind: str,
    signature: str,
) -> str:
    return "||".join(
        [
            str(project_key or "").strip(),
            str(host or "").strip(),
            str(issue_kind or "").strip(),
            str(signature or "").strip(),
        ]
    )


def fetch_action_queue_states(
    connection: sqlite3.Connection,
    owner_scope: str,
    fingerprints: list[str],
) -> dict[str, dict[str, object]]:
    normalized_owner_scope = str(owner_scope or "").strip()
    normalized_fingerprints = [str(item).strip() for item in fingerprints if str(item).strip()]
    if not normalized_owner_scope or not normalized_fingerprints:
        return {}

    placeholders = ", ".join("?" for _ in normalized_fingerprints)
    rows = connection.execute(
        f"""
        SELECT
            owner_scope,
            fingerprint,
            project_key,
            issue_kind,
            status,
            snoozed_until,
            created_at,
            updated_at
        FROM action_queue_states
        WHERE owner_scope = ?
          AND fingerprint IN ({placeholders})
        """,
        (normalized_owner_scope, *normalized_fingerprints),
    ).fetchall()
    return {str(row["fingerprint"]): dict(row) for row in rows}


def set_action_queue_state(
    connection: sqlite3.Connection,
    *,
    owner_scope: str,
    fingerprint: str,
    project_key: str,
    issue_kind: str,
    status: str,
    snoozed_until: str | None = None,
) -> None:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in ACTION_QUEUE_STATE_STATUSES:
        raise ValueError(f"Unsupported action queue status: {status}")

    now = utc_now_iso()
    connection.execute(
        """
        INSERT INTO action_queue_states (
            owner_scope,
            fingerprint,
            project_key,
            issue_kind,
            status,
            snoozed_until,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(owner_scope, fingerprint)
        DO UPDATE SET
            project_key = excluded.project_key,
            issue_kind = excluded.issue_kind,
            status = excluded.status,
            snoozed_until = excluded.snoozed_until,
            updated_at = excluded.updated_at
        """,
        (
            str(owner_scope or "").strip(),
            str(fingerprint or "").strip(),
            str(project_key or "").strip(),
            str(issue_kind or "").strip(),
            normalized_status,
            str(snoozed_until or "").strip() or None,
            now,
            now,
        ),
    )


def clear_action_queue_state(
    connection: sqlite3.Connection,
    *,
    owner_scope: str,
    fingerprint: str,
) -> bool:
    cursor = connection.execute(
        """
        DELETE FROM action_queue_states
        WHERE owner_scope = ? AND fingerprint = ?
        """,
        (str(owner_scope or "").strip(), str(fingerprint or "").strip()),
    )
    return cursor.rowcount > 0


def default_snoozed_until(*, now: datetime | None = None) -> str:
    current = now or datetime.now(tz=UTC)
    return (current + DEFAULT_ACTION_QUEUE_SNOOZE).replace(microsecond=0).isoformat()


def filter_action_queue_items(
    items: list[dict[str, object]],
    state_by_fingerprint: dict[str, dict[str, object]],
    *,
    now: datetime | None = None,
) -> list[dict[str, object]]:
    current = now or datetime.now(tz=UTC)
    visible: list[dict[str, object]] = []
    for item in items:
        fingerprint = str(item.get("fingerprint") or "").strip()
        if not fingerprint:
            visible.append(item)
            continue

        state = state_by_fingerprint.get(fingerprint)
        if not state:
            visible.append(item)
            continue

        status = str(state.get("status") or "").strip().lower()
        if status == "ignored":
            continue

        if status == "snoozed":
            snoozed_until = parse_timestamp(str(state.get("snoozed_until") or ""))
            if snoozed_until is not None and snoozed_until.astimezone(UTC) > current:
                continue

        if status == "resolved":
            item_timestamp = parse_timestamp(str(item.get("timestamp") or ""))
            state_updated_at = parse_timestamp(str(state.get("updated_at") or ""))
            if item_timestamp is not None and state_updated_at is not None and item_timestamp <= state_updated_at:
                continue

        visible.append(item)
    return visible
