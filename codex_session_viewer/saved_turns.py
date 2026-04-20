from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from fastapi import Request

from .projects import (
    ProjectAccessContext,
    effective_project_fields,
    filter_rows_for_project_access,
)
from .session_view import build_turns
from .text_utils import shorten


GLOBAL_OWNER_SCOPE = "__global__"
SAVED_TURN_STATUSES = {"open", "resolved"}
SAVED_TURN_SORTS = {"newest", "oldest"}
EFFECTIVE_GROUP_KEY_SQL = "COALESCE(NULLIF(TRIM(o.override_group_key), ''), s.inferred_project_key)"


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def owner_scope_from_request(request: Request) -> str:
    auth_user = getattr(request.state, "auth_user", None)
    auth_enabled = bool(getattr(request.state, "auth_enabled", False))
    if isinstance(auth_user, dict):
        user_id = str(auth_user.get("user_id") or "").strip()
        if user_id:
            return user_id
        if auth_enabled:
            raise RuntimeError("Authenticated user is missing a durable user_id")
    if auth_enabled:
        raise RuntimeError("Owner-scoped state requires an authenticated user")
    return GLOBAL_OWNER_SCOPE


def migrate_global_saved_turns_to_owner(
    connection: sqlite3.Connection,
    *,
    owner_scope: str,
) -> None:
    target_scope = str(owner_scope or "").strip()
    if not target_scope or target_scope == GLOBAL_OWNER_SCOPE:
        return
    connection.execute(
        """
        DELETE FROM saved_turns
        WHERE owner_scope = ?
          AND EXISTS (
            SELECT 1
            FROM saved_turns AS target
            WHERE target.owner_scope = ?
              AND target.session_id = saved_turns.session_id
              AND target.turn_number = saved_turns.turn_number
          )
        """,
        (GLOBAL_OWNER_SCOPE, target_scope),
    )
    connection.execute(
        """
        UPDATE saved_turns
        SET owner_scope = ?
        WHERE owner_scope = ?
        """,
        (target_scope, GLOBAL_OWNER_SCOPE),
    )


def count_saved_turns(
    connection: sqlite3.Connection,
    owner_scope: str,
    *,
    status: str = "open",
    project_key: str | None = None,
    project_access: ProjectAccessContext | None = None,
) -> int:
    where_clause = "WHERE st.owner_scope = ? AND st.status = ?"
    params: list[object] = [owner_scope, status]
    if project_key:
        where_clause += f" AND {EFFECTIVE_GROUP_KEY_SQL} = ?"
        params.append(project_key)
    rows = connection.execute(
        f"""
        SELECT
            st.session_id,
            s.inferred_project_key,
            o.override_group_key,
            p.id AS project_id,
            p.visibility AS project_visibility
        FROM saved_turns AS st
        INNER JOIN sessions AS s
            ON s.id = st.session_id
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        LEFT JOIN project_sources AS ps
            ON ps.match_project_key = s.inferred_project_key
        LEFT JOIN projects AS p
            ON p.id = ps.project_id
        {where_clause}
        """,
        params,
    ).fetchall()
    return len(filter_rows_for_project_access(rows, project_access))


def count_saved_turns_by_status(
    connection: sqlite3.Connection,
    owner_scope: str,
    *,
    project_key: str | None = None,
    project_access: ProjectAccessContext | None = None,
) -> dict[str, int]:
    where_clause = "WHERE st.owner_scope = ?"
    params: list[object] = [owner_scope]
    if project_key:
        where_clause += f" AND {EFFECTIVE_GROUP_KEY_SQL} = ?"
        params.append(project_key)
    rows = connection.execute(
        f"""
        SELECT
            st.status,
            s.inferred_project_key,
            o.override_group_key,
            p.id AS project_id,
            p.visibility AS project_visibility
        FROM saved_turns AS st
        INNER JOIN sessions AS s
            ON s.id = st.session_id
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        LEFT JOIN project_sources AS ps
            ON ps.match_project_key = s.inferred_project_key
        LEFT JOIN projects AS p
            ON p.id = ps.project_id
        {where_clause}
        """,
        params,
    ).fetchall()
    counts = {"open": 0, "resolved": 0}
    for row in filter_rows_for_project_access(rows, project_access):
        status = str(row["status"] or "")
        if status in counts:
            counts[status] += 1
    return counts


def fetch_session_saved_turn_states(
    connection: sqlite3.Connection,
    owner_scope: str,
    session_id: str,
) -> dict[int, str]:
    rows = connection.execute(
        """
        SELECT turn_number, status
        FROM saved_turns
        WHERE owner_scope = ? AND session_id = ?
        """,
        (owner_scope, session_id),
    ).fetchall()
    return {
        int(row["turn_number"]): str(row["status"])
        for row in rows
        if str(row["status"] or "") in SAVED_TURN_STATUSES
    }


def normalize_saved_turn_sort(value: str | None) -> str:
    sort = str(value or "").strip().lower()
    return sort if sort in SAVED_TURN_SORTS else "newest"


def upsert_saved_turn(
    connection: sqlite3.Connection,
    *,
    owner_scope: str,
    session_id: str,
    turn_number: int,
    prompt_excerpt: str,
    response_excerpt: str,
    prompt_timestamp: str | None,
    response_timestamp: str | None,
) -> None:
    now = utc_now_iso()
    connection.execute(
        """
        INSERT INTO saved_turns (
            owner_scope,
            session_id,
            turn_number,
            prompt_excerpt,
            response_excerpt,
            prompt_timestamp,
            response_timestamp,
            status,
            created_at,
            resolved_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, NULL, ?)
        ON CONFLICT(owner_scope, session_id, turn_number)
        DO UPDATE SET
            prompt_excerpt = excluded.prompt_excerpt,
            response_excerpt = excluded.response_excerpt,
            prompt_timestamp = excluded.prompt_timestamp,
            response_timestamp = excluded.response_timestamp,
            status = 'open',
            resolved_at = NULL,
            updated_at = excluded.updated_at
        """,
        (
            owner_scope,
            session_id,
            turn_number,
            prompt_excerpt,
            response_excerpt,
            prompt_timestamp,
            response_timestamp,
            now,
            now,
        ),
    )


def set_saved_turn_status(
    connection: sqlite3.Connection,
    *,
    owner_scope: str,
    session_id: str,
    turn_number: int,
    status: str,
) -> bool:
    if status not in SAVED_TURN_STATUSES:
        raise ValueError(f"Unsupported saved turn status: {status}")
    now = utc_now_iso()
    resolved_at = now if status == "resolved" else None
    cursor = connection.execute(
        """
        UPDATE saved_turns
        SET
            status = ?,
            resolved_at = ?,
            updated_at = ?
        WHERE owner_scope = ? AND session_id = ? AND turn_number = ?
        """,
        (status, resolved_at, now, owner_scope, session_id, turn_number),
    )
    return cursor.rowcount > 0


def fetch_turn_snapshot(
    connection: sqlite3.Connection,
    session_id: str,
    turn_number: int,
) -> dict[str, Any] | None:
    events = connection.execute(
        """
        SELECT
            event_index,
            timestamp,
            record_type,
            payload_type,
            kind,
            role,
            title,
            display_text,
            detail_text,
            tool_name,
            call_id,
            command_text,
            exit_code,
            record_json
        FROM events
        WHERE session_id = ?
        ORDER BY event_index ASC
        """,
        (session_id,),
    ).fetchall()
    for turn in build_turns(events):
        if int(turn.get("number") or 0) != turn_number:
            continue
        prompt_text = str(turn.get("prompt_text") or "").strip()
        response_text = str(turn.get("response_text") or "").strip()
        return {
            "prompt_excerpt": shorten(prompt_text, 220) if prompt_text else f"Turn {turn_number}",
            "response_excerpt": shorten(response_text, 240) if response_text else "No assistant response captured.",
            "prompt_timestamp": str(turn.get("prompt_timestamp") or "").strip() or None,
            "response_timestamp": str(turn.get("response_timestamp") or "").strip() or None,
        }
    return None


def list_saved_turns(
    connection: sqlite3.Connection,
    owner_scope: str,
    *,
    status: str = "open",
    sort: str = "newest",
    project_key: str | None = None,
    project_access: ProjectAccessContext | None = None,
) -> list[dict[str, Any]]:
    order_clause = (
        "ORDER BY st.created_at DESC, st.turn_number DESC"
        if normalize_saved_turn_sort(sort) == "newest"
        else "ORDER BY st.created_at ASC, st.turn_number ASC"
    )
    where_clause = "WHERE st.owner_scope = ? AND st.status = ?"
    params: list[object] = [owner_scope, status]
    if project_key:
        where_clause += f" AND {EFFECTIVE_GROUP_KEY_SQL} = ?"
        params.append(project_key)
    rows = connection.execute(
        f"""
        SELECT
            st.owner_scope,
            st.session_id,
            st.turn_number,
            st.prompt_excerpt,
            st.response_excerpt,
            st.prompt_timestamp,
            st.response_timestamp,
            st.status,
            st.created_at,
            st.resolved_at,
            st.updated_at,
            s.source_host,
            s.cwd,
            s.cwd_name,
            s.git_repository_url,
            s.github_remote_url,
            s.github_org,
            s.github_repo,
            s.github_slug,
            s.inferred_project_kind,
            s.inferred_project_key,
            s.inferred_project_label,
            p.id AS project_id,
            p.visibility AS project_visibility,
            o.override_group_key,
            o.override_organization,
            o.override_repository,
            o.override_remote_url,
            o.override_display_label
        FROM saved_turns AS st
        INNER JOIN sessions AS s
            ON s.id = st.session_id
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        LEFT JOIN project_sources AS ps
            ON ps.match_project_key = s.inferred_project_key
        LEFT JOIN projects AS p
            ON p.id = ps.project_id
        {where_clause}
        {order_clause}
        """,
        params,
    ).fetchall()

    items: list[dict[str, Any]] = []
    for row in filter_rows_for_project_access(rows, project_access):
        project = effective_project_fields(row)
        activity_timestamp = (
            str(row["resolved_at"] or "").strip()
            or str(row["created_at"] or "").strip()
        )
        items.append(
            {
                "session_id": str(row["session_id"]),
                "turn_number": int(row["turn_number"] or 0),
                "prompt_excerpt": str(row["prompt_excerpt"] or "").strip(),
                "response_excerpt": str(row["response_excerpt"] or "").strip(),
                "prompt_timestamp": str(row["prompt_timestamp"] or "").strip(),
                "response_timestamp": str(row["response_timestamp"] or "").strip(),
                "status": str(row["status"] or "open"),
                "saved_at": str(row["created_at"] or "").strip(),
                "resolved_at": str(row["resolved_at"] or "").strip(),
                "updated_at": str(row["updated_at"] or "").strip(),
                "activity_timestamp": activity_timestamp,
                "project_label": str(project["display_label"]),
                "source_host": str(project["source_host"] or ""),
                "session_href": f"/sessions/{row['session_id']}?turn={int(row['turn_number'] or 0)}",
            }
        )
    return items
