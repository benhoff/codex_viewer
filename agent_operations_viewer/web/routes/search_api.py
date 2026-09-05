from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from typing import Literal
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse

from ...db import connect
from ...projects import (
    build_project_access_context,
    effective_project_fields,
    fetch_session_with_project,
    row_is_visible_to_project_access,
)
from ...search import search_turn_hits_raw
from ...session_view import build_turns
from ..auth import require_authenticated_user
from ..context import get_app_context


router = APIRouter()
CURSOR_VERSION = 2


def _timestamp_param(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return normalized.isoformat()


def _cursor_fingerprint(
    *,
    q: str,
    project_id: str | None,
    host: str | None,
    from_timestamp: str | None,
    to_timestamp: str | None,
    limit: int,
    sort: str,
    group_by: str,
    max_hits_per_session: int,
) -> str:
    payload = json.dumps(
        [
            q,
            project_id,
            host,
            from_timestamp,
            to_timestamp,
            limit,
            sort,
            group_by,
            max_hits_per_session,
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _encode_cursor(page: int, fingerprint: str) -> str:
    raw = json.dumps(
        {"v": CURSOR_VERSION, "page": page, "fingerprint": fingerprint},
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(value: str | None, fingerprint: str) -> int:
    candidate = str(value or "").strip()
    if not candidate:
        return 1
    try:
        padding = "=" * (-len(candidate) % 4)
        payload = json.loads(base64.urlsafe_b64decode(candidate + padding).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Invalid search cursor") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("v") != CURSOR_VERSION
        or payload.get("fingerprint") != fingerprint
        or not isinstance(payload.get("page"), int)
        or int(payload["page"]) < 1
    ):
        raise HTTPException(status_code=400, detail="Search cursor does not match this query")
    return int(payload["page"])


def _serialize_hit(item: dict[str, object]) -> dict[str, object]:
    session_id = str(item["session_id"])
    encoded_session_id = quote(session_id, safe="")
    turn_number = int(item["turn_number"] or 0)
    return {
        "project": {
            "id": item.get("project_id"),
            "key": item.get("project_key"),
            "label": item.get("project_label"),
            "host": item.get("host"),
        },
        "repository": item.get("repository"),
        "session_id": session_id,
        "turn_number": turn_number,
        "timestamp": item.get("timestamp"),
        "matched_field": item.get("matched_field"),
        "match_source": item.get("match_source"),
        "chunk": item.get("chunk"),
        "snippet": item.get("snippet"),
        "score": item.get("score"),
        "prompt_excerpt": item.get("prompt_excerpt"),
        "response_excerpt": item.get("response_excerpt"),
        "response_state": item.get("response_state"),
        "stats": {
            "commands": item.get("command_count", 0),
            "patches": item.get("patch_count", 0),
            "failures": item.get("failure_count", 0),
            "files_touched": item.get("files_touched_count", 0),
        },
        "signals": item.get("signal_badges", []),
        "links": {
            "turn": f"/api/v1/sessions/{encoded_session_id}/turns/{turn_number}",
            "conversation": f"/sessions/{encoded_session_id}?turn={turn_number}",
            "audit": f"/sessions/{encoded_session_id}?view=audit&turn={turn_number}&focus=1",
        },
    }


def _turn_events(
    connection: sqlite3.Connection,
    session_id: str,
    start_event_index: int,
    end_event_index: int,
) -> list[sqlite3.Row]:
    return connection.execute(
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
          AND event_index BETWEEN ? AND ?
        ORDER BY event_index ASC
        """,
        (session_id, start_event_index, end_event_index),
    ).fetchall()


def _serialize_command(event: dict[str, object]) -> dict[str, object]:
    return {
        "event_index": event.get("event_index"),
        "timestamp": event.get("timestamp"),
        "call_id": event.get("call_id"),
        "command": event.get("command_text") or event.get("display_text"),
        "cwd": event.get("command_cwd"),
        "output": event.get("output_text") or "",
        "exit_code": event.get("exit_code"),
        "status": event.get("command_status"),
        "duration_seconds": event.get("duration_seconds"),
        "parsed_commands": event.get("parsed_commands") or [],
    }


def _serialize_patch(event: dict[str, object]) -> dict[str, object]:
    return {
        "event_index": event.get("event_index"),
        "timestamp": event.get("timestamp"),
        "call_id": event.get("call_id"),
        "status": event.get("patch_status"),
        "success": event.get("patch_success"),
        "patch": event.get("raw_patch_text") or event.get("display_text") or "",
        "files": event.get("patch_manifest") or event.get("patch_files") or [],
        "output": event.get("tool_output_text") or "",
    }


def _serialize_activity(event: dict[str, object]) -> dict[str, object]:
    return {
        "event_index": event.get("event_index"),
        "timestamp": event.get("timestamp"),
        "record_type": event.get("record_type"),
        "payload_type": event.get("payload_type"),
        "kind": event.get("kind"),
        "role": event.get("role"),
        "title": event.get("title"),
        "tool_name": event.get("tool_name"),
        "call_id": event.get("call_id"),
        "display_text": event.get("display_text") or "",
        "detail_text": event.get("detail_text") or "",
        "command_text": event.get("command_text"),
        "exit_code": event.get("exit_code"),
    }


def _serialize_turn(
    turn: dict[str, object],
    *,
    target_turn_number: int,
    include_activity: bool,
) -> dict[str, object]:
    commands = turn.get("audit_command_events")
    patches = turn.get("audit_patch_events")
    activity = turn.get("merged_detail_events")
    payload: dict[str, object] = {
        "turn_number": int(turn.get("number") or 0),
        "turn_id": turn.get("turn_id"),
        "is_target": int(turn.get("number") or 0) == target_turn_number,
        "prompt": {
            "text": turn.get("prompt_text") or "",
            "timestamp": turn.get("prompt_timestamp"),
        },
        "response": {
            "text": turn.get("response_text") or "",
            "timestamp": turn.get("response_timestamp"),
            "state": turn.get("response_state") or "missing",
            "abort_reason": turn.get("abort_reason"),
        },
        "duration_seconds": turn.get("duration_seconds"),
        "agent": {
            "model": turn.get("agent_model"),
            "effort": turn.get("agent_effort"),
        },
        "execution_context": turn.get("audit_execution_context") or {},
        "commands": (
            [_serialize_command(item) for item in commands if isinstance(item, dict)]
            if isinstance(commands, list)
            else []
        ),
        "patches": (
            [_serialize_patch(item) for item in patches if isinstance(item, dict)]
            if isinstance(patches, list)
            else []
        ),
        "files": turn.get("audit_file_manifest") or [],
        "stats": turn.get("audit_summary") or {},
    }
    if include_activity:
        payload["activity"] = (
            [_serialize_activity(item) for item in activity if isinstance(item, dict)]
            if isinstance(activity, list)
            else []
        )
    return payload


def _parse_turn_includes(value: str | None) -> set[str]:
    includes = {
        item.strip().lower()
        for item in str(value or "").split(",")
        if item.strip()
    }
    unsupported = includes - {"activity"}
    if unsupported:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported include value: {sorted(unsupported)[0]}",
        )
    return includes


@router.get("/api/v1/search", response_class=JSONResponse)
def search_api(
    request: Request,
    q: str = Query(..., min_length=1, max_length=500),
    project_id: str | None = Query(default=None, max_length=128),
    host: str | None = Query(default=None, max_length=255),
    from_date: datetime | None = Query(default=None, alias="from"),
    to_date: datetime | None = Query(default=None, alias="to"),
    sort: Literal["relevance", "time_asc", "time_desc"] = Query(default="relevance"),
    group_by: Literal["none", "session"] = Query(default="none"),
    max_hits_per_session: int = Query(default=3, ge=1, le=100),
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = Query(default=None, max_length=2048),
) -> JSONResponse:
    context = get_app_context(request)
    if bool(getattr(request.state, "auth_enabled", False)):
        require_authenticated_user(request)

    search_query = q.strip()
    if not search_query:
        raise HTTPException(status_code=422, detail="Search query cannot be empty")
    normalized_project_id = str(project_id or "").strip() or None
    normalized_host = str(host or "").strip() or None
    from_timestamp = _timestamp_param(from_date)
    to_timestamp = _timestamp_param(to_date)
    if from_date is not None and to_date is not None:
        normalized_from = from_date.replace(tzinfo=UTC) if from_date.tzinfo is None else from_date.astimezone(UTC)
        normalized_to = to_date.replace(tzinfo=UTC) if to_date.tzinfo is None else to_date.astimezone(UTC)
        if normalized_from > normalized_to:
            raise HTTPException(status_code=422, detail="from must be earlier than or equal to to")

    fingerprint = _cursor_fingerprint(
        q=search_query,
        project_id=normalized_project_id,
        host=normalized_host,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        limit=limit,
        sort=sort,
        group_by=group_by,
        max_hits_per_session=max_hits_per_session,
    )
    page = _decode_cursor(cursor, fingerprint)

    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        search_page = search_turn_hits_raw(
            connection,
            search_query,
            page=page,
            page_size=limit,
            project_id=normalized_project_id,
            host=normalized_host,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            sort=sort,
            group_by=group_by,
            max_hits_per_session=max_hits_per_session,
            project_access=project_access,
        )

    next_cursor = (
        _encode_cursor(int(search_page["page"]) + 1, fingerprint)
        if bool(search_page["has_next"])
        else None
    )
    serialized_groups = [
        {
            "session_id": group["session_id"],
            "match_count": int(group["match_count"]),
            "returned_hit_count": len(group["items"]),
            "hits": [_serialize_hit(item) for item in group["items"]],
        }
        for group in search_page["groups"]
    ]
    serialized_hits = (
        []
        if group_by == "session"
        else [_serialize_hit(item) for item in search_page["items"]]
    )
    return JSONResponse(
        {
            "query": search_query,
            "filters": {
                "project_id": normalized_project_id,
                "host": normalized_host,
                "from": from_timestamp,
                "to": to_timestamp,
            },
            "retrieval": search_page["retrieval"],
            "coverage": search_page["coverage"],
            "sort": sort,
            "group_by": group_by,
            "max_hits_per_session": max_hits_per_session,
            "hits": serialized_hits,
            "groups": serialized_groups,
            "total_count": int(search_page["total_count"]),
            "session_count": int(search_page["session_count"]),
            "pagination": {
                "unit": search_page["pagination_unit"],
                "total_count": int(search_page["pagination_total"]),
                "returned_count": (
                    len(serialized_groups)
                    if group_by == "session"
                    else len(serialized_hits)
                ),
            },
            "limit": int(search_page["page_size"]),
            "next_cursor": next_cursor,
        },
        headers={"Cache-Control": "private, no-store"},
    )


@router.get(
    "/api/v1/sessions/{session_id}/turns/{turn_number}",
    response_class=JSONResponse,
)
def session_turn_api(
    request: Request,
    session_id: str,
    turn_number: int = Path(..., ge=1),
    context_turns: int = Query(default=0, alias="context", ge=0, le=10),
    include: str | None = Query(default=None, max_length=100),
) -> JSONResponse:
    app_context = get_app_context(request)
    if bool(getattr(request.state, "auth_enabled", False)):
        require_authenticated_user(request)
    includes = _parse_turn_includes(include)

    with connect(app_context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        session = fetch_session_with_project(connection, session_id)
        if session is None or not row_is_visible_to_project_access(session, project_access):
            raise HTTPException(status_code=404, detail="Session not found")

        target = connection.execute(
            """
            SELECT *
            FROM session_turns
            WHERE session_id = ? AND turn_number = ?
            """,
            (session_id, turn_number),
        ).fetchone()
        if target is None:
            raise HTTPException(status_code=404, detail="Turn not found")

        bounds = connection.execute(
            """
            SELECT MIN(turn_number) AS first_turn, MAX(turn_number) AS last_turn
            FROM session_turns
            WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()
        session_first_turn = int(bounds["first_turn"] or turn_number)
        session_last_turn = int(bounds["last_turn"] or turn_number)
        first_turn = max(session_first_turn, turn_number - context_turns)
        last_turn = min(session_last_turn, turn_number + context_turns)
        turn_rows = connection.execute(
            """
            SELECT *
            FROM session_turns
            WHERE session_id = ? AND turn_number BETWEEN ? AND ?
            ORDER BY turn_number ASC
            """,
            (session_id, first_turn, last_turn),
        ).fetchall()
        events = _turn_events(
            connection,
            session_id,
            int(turn_rows[0]["start_event_index"]),
            int(turn_rows[-1]["end_event_index"]),
        )
        project = effective_project_fields(session)

    turns = build_turns(
        events,
        cwd=str(session["cwd"] or "").strip() or None,
        starting_turn_number=first_turn,
    )
    return JSONResponse(
        {
            "session_id": session_id,
            "requested_turn": turn_number,
            "context": {
                "requested": context_turns,
                "first_turn": first_turn,
                "last_turn": last_turn,
            },
            "include": sorted(includes),
            "project": {
                "id": project["project_id"],
                "key": project["effective_group_key"],
                "label": project["display_label"],
                "host": project["source_host"],
            },
            "repository": {
                "remote": str(
                    session["git_repository_url"] or session["github_remote_url"] or ""
                ).strip()
                or None,
                "root": project["cwd"],
                "branch": str(session["git_branch"] or "").strip() or None,
                "head": str(session["git_commit_hash"] or "").strip() or None,
                "dirty": None,
            },
            "turns": [
                _serialize_turn(
                    turn,
                    target_turn_number=turn_number,
                    include_activity="activity" in includes,
                )
                for turn in turns
            ],
            "links": {
                "conversation": f"/sessions/{quote(session_id, safe='')}?turn={turn_number}",
                "audit": f"/sessions/{quote(session_id, safe='')}?view=audit&turn={turn_number}&focus=1",
            },
        },
        headers={"Cache-Control": "private, no-store"},
    )
