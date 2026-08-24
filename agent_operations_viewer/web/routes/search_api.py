from __future__ import annotations

import base64
import hashlib
import json
from datetime import UTC, datetime
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from ...db import connect
from ...projects import build_project_access_context
from ...search import search_turn_hits_raw
from ..auth import require_authenticated_user
from ..context import get_app_context


router = APIRouter()
CURSOR_VERSION = 1


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
) -> str:
    payload = json.dumps(
        [q, project_id, host, from_timestamp, to_timestamp, limit],
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
        "session_id": session_id,
        "turn_number": turn_number,
        "timestamp": item.get("timestamp"),
        "matched_field": item.get("matched_field"),
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
            "conversation": f"/sessions/{encoded_session_id}?turn={turn_number}",
            "audit": f"/sessions/{encoded_session_id}?view=audit&turn={turn_number}&focus=1",
        },
    }


@router.get("/api/v1/search", response_class=JSONResponse)
def search_api(
    request: Request,
    q: str = Query(..., min_length=1, max_length=500),
    project_id: str | None = Query(default=None, max_length=128),
    host: str | None = Query(default=None, max_length=255),
    from_date: datetime | None = Query(default=None, alias="from"),
    to_date: datetime | None = Query(default=None, alias="to"),
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
            project_access=project_access,
        )

    next_cursor = (
        _encode_cursor(int(search_page["page"]) + 1, fingerprint)
        if bool(search_page["has_next"])
        else None
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
            "hits": [_serialize_hit(item) for item in search_page["items"]],
            "total_count": int(search_page["total_count"]),
            "limit": int(search_page["page_size"]),
            "next_cursor": next_cursor,
        },
        headers={"Cache-Control": "private, no-store"},
    )
