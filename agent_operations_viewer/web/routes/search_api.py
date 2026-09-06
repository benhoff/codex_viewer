from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Literal
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field

from ...projects import (
    effective_project_fields,
    fetch_session_with_project,
    row_is_visible_to_project_access,
)
from ...repositories import (
    list_repository_projects,
    normalize_repository_remote_filter,
    normalize_repository_root,
)
from ...search import (
    SEARCH_FACETS,
    SEARCH_FIELDS,
    normalize_search_values,
    search_turn_hits_raw,
    coverage_readiness,
)
from ...search_snapshots import (
    NORMALIZATION_VERSION,
    api_error,
    digest,
    evidence_snapshot,
    cursor_page,
    encode_cursor,
)
from ...turn_index import patch_line_ranges
from ...search_budget import SearchWorkTimeout, mark_search_stage
from ...session_view import build_turns, parse_timestamp
from ..auth import require_authenticated_user
from ..context import get_app_context


class StrictSearchRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()
        allowed = sorted(param.alias for param in self.dependant.query_params)

        async def strict_handler(request):
            errors = []
            for name in request.query_params:
                if name not in allowed:
                    errors.append(
                        {
                            "type": "extra_forbidden",
                            "loc": ["query", name],
                            "msg": "Unsupported query parameter",
                            "allowed": allowed,
                        }
                    )
                elif (
                    name != "exclude_session_id"
                    and len(request.query_params.getlist(name)) > 1
                ):
                    errors.append(
                        {
                            "type": "duplicate_parameter",
                            "loc": ["query", name],
                            "msg": "Parameter may only be supplied once",
                            "allowed": allowed,
                        }
                    )
            if errors:
                raise HTTPException(status_code=422, detail=errors)
            try:
                return await handler(request)
            except RequestValidationError as exc:
                errors = exc.errors()
                for error in errors:
                    if error["type"] == "extra_forbidden":
                        model = (
                            BatchSearchQuery
                            if "queries" in error["loc"]
                            else BatchSearchRequest
                        )
                        error["allowed"] = sorted(
                            field.alias or name
                            for name, field in model.model_fields.items()
                        )
                raise HTTPException(status_code=422, detail=errors) from exc

        return strict_handler


router = APIRouter(route_class=StrictSearchRoute)

SearchMode = Literal["all", "any", "phrase", "exact"]
SearchField = Literal[
    "prompt",
    "response",
    "activity",
    "commands",
    "paths",
    "commit_ids",
    "tool_output",
    "patches",
]
SearchFacet = Literal["project", "session", "date", "branch", "matched_field"]


class BatchSearchQuery(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    id: str | None = Field(default=None, max_length=128)
    q: str = Field(min_length=1, max_length=500)
    project_id: str | None = Field(default=None, max_length=128)
    repository_id: str | None = Field(default=None, max_length=128)
    remote: str | None = Field(default=None, max_length=2048)
    root: str | None = Field(default=None, max_length=2048)
    host: str | None = Field(default=None, max_length=255)
    exclude_session_id: list[str] = Field(default_factory=list, max_length=100)
    from_date: datetime | None = Field(default=None, alias="from")
    to_date: datetime | None = Field(default=None, alias="to")
    mode: SearchMode = "all"
    fields: list[SearchField] = Field(default_factory=list, max_length=8)
    facets: list[SearchFacet] = Field(default_factory=list, max_length=5)
    sort: Literal["relevance", "time_asc", "time_desc"] = "relevance"
    group_by: Literal["none", "session"] = "none"
    max_hits_per_session: int = Field(default=3, ge=1, le=100)
    limit: int = Field(default=20, ge=1, le=100)


class BatchSearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    queries: list[BatchSearchQuery] = Field(min_length=1, max_length=20)
    max_total_hits: int = Field(default=200, ge=1, le=500)
    snapshot_id: str | None = Field(default=None, min_length=1, max_length=2048)


def _timestamp_param(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = (
        value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    )
    return normalized.isoformat()


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


def _normalize_api_values(
    value: str | list[str] | tuple[str, ...] | None,
    *,
    supported: frozenset[str],
    label: str,
) -> tuple[str, ...]:
    try:
        return normalize_search_values(value, supported=supported, label=label)
    except ValueError as exc:
        raise api_error(
            422, "invalid_input", str(exc), name=label, allowed=sorted(supported)
        ) from exc


def _validate_date_range(
    from_date: datetime | None,
    to_date: datetime | None,
) -> tuple[str | None, str | None]:
    if from_date is not None and to_date is not None:
        normalized_from = (
            from_date.replace(tzinfo=UTC)
            if from_date.tzinfo is None
            else from_date.astimezone(UTC)
        )
        normalized_to = (
            to_date.replace(tzinfo=UTC)
            if to_date.tzinfo is None
            else to_date.astimezone(UTC)
        )
        if normalized_from > normalized_to:
            raise api_error(
                422, "invalid_input", "from must be earlier than or equal to to"
            )
    return _timestamp_param(from_date), _timestamp_param(to_date)


def _normalize_repository_filters(
    remote: str | None,
    root: str | None,
) -> tuple[str | None, str | None]:
    try:
        normalized_remote = normalize_repository_remote_filter(remote)
    except ValueError as exc:
        raise api_error(422, "invalid_input", str(exc), name="remote") from exc
    return normalized_remote, normalize_repository_root(root)


def _serialize_search_page(
    search_page: dict[str, object],
    *,
    query: str,
    project_id: str | None,
    repository_id: str | None,
    remote: str | None,
    root: str | None,
    host: str | None,
    from_timestamp: str | None,
    to_timestamp: str | None,
    mode: str,
    fields: tuple[str, ...],
    requested_facets: tuple[str, ...],
    sort: str,
    group_by: str,
    max_hits_per_session: int,
) -> dict[str, object]:
    groups = search_page.get("groups") or []
    items = search_page.get("items") or []
    serialized_groups = [
        {
            "session_id": group["session_id"],
            "match_count": int(group["match_count"]),
            "returned_hit_count": len(group["items"]),
            "hits": [_serialize_hit(item) for item in group["items"]],
        }
        for group in groups
    ]
    serialized_hits = (
        [] if group_by == "session" else [_serialize_hit(item) for item in items]
    )
    return {
        "query": query,
        "filters": {
            "project_id": project_id,
            "repository_id": repository_id,
            "remote": remote,
            "root": root,
            "host": host,
            "from": from_timestamp,
            "to": to_timestamp,
            "fields": list(fields),
        },
        "mode": mode,
        "retrieval": search_page["retrieval"],
        "coverage": search_page["coverage"],
        "facets": search_page.get("facets")
        or {facet: [] for facet in requested_facets},
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
        "next_cursor": None,
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
        item.strip().lower() for item in str(value or "").split(",") if item.strip()
    }
    unsupported = includes - {"activity"}
    if unsupported:
        raise api_error(
            422,
            "invalid_input",
            f"Unsupported include value: {sorted(unsupported)[0]}",
            name="include",
            allowed=["activity"],
        )
    return includes


@contextmanager
def _snapshot(request, snapshot_id=None, cursor=None):
    if bool(getattr(request.state, "auth_enabled", False)):
        require_authenticated_user(request)
    try:
        with evidence_snapshot(
            get_app_context(request).settings,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
            snapshot_id=snapshot_id,
            cursor=cursor,
        ) as snapshot:
            yield snapshot
    except SearchWorkTimeout as exc:
        raise api_error(
            503,
            "query_timeout",
            "Search work budget exceeded; narrow the query or reduce the requested hits",
            stage=exc.stage,
            elapsed_seconds=exc.elapsed_seconds,
            budget_seconds=exc.budget_seconds,
        ) from exc


def _normalized_query(query):
    search_query = query.q.strip()
    if not search_query:
        raise api_error(422, "invalid_input", "Search query cannot be empty")
    if query.group_by == "none" and "max_hits_per_session" in query.model_fields_set:
        raise api_error(
            422,
            "inapplicable_parameter",
            "max_hits_per_session requires group_by=session",
            name="max_hits_per_session",
        )
    excluded = sorted(set(value.strip() for value in query.exclude_session_id))
    if any(not value or len(value) > 128 for value in excluded):
        raise api_error(
            422,
            "invalid_input",
            "exclude_session_id values must contain 1–128 characters",
        )
    remote, root = _normalize_repository_filters(query.remote, query.root)
    start, end = _validate_date_range(query.from_date, query.to_date)
    return {
        "q": search_query,
        "project_id": str(query.project_id or "").strip() or None,
        "repository_id": str(query.repository_id or "").strip() or None,
        "remote": remote,
        "root": root,
        "host": str(query.host or "").strip() or None,
        "from": start,
        "to": end,
        "exclude_session_id": excluded,
        "mode": query.mode,
        "fields": sorted(set(query.fields)),
        "effective_fields": sorted(set(query.fields)) or sorted(SEARCH_FIELDS),
        "include_project_metadata": not query.fields,
        "facets": sorted(set(query.facets)),
        "sort": query.sort,
        "group_by": query.group_by,
        "max_hits_per_session": query.max_hits_per_session,
        "limit": query.limit,
    }


def _load_turn(connection, session, row):
    mark_search_stage("evidence_reconstruction")
    number = int(row["turn_number"])
    events = _turn_events(
        connection,
        str(session["id"]),
        int(row["start_event_index"]),
        int(row["end_event_index"]),
    )
    turns = build_turns(
        events,
        cwd=str(session["cwd"] or "").strip() or None,
        starting_turn_number=number,
    )
    if not turns:
        raise api_error(
            409, "incomplete_index", "Indexed turn has no reconstructable evidence"
        )
    mark_search_stage("evidence_serialization")
    payload = _serialize_turn(
        turns[0], target_turn_number=number, include_activity=True
    )
    # Evidence identity is independent of selection/context and HTTP presentation.
    canonical = {key: value for key, value in payload.items() if key != "is_target"}
    mark_search_stage("evidence_digest")
    identity = digest(
        {"normalization_version": NORMALIZATION_VERSION, "turn": canonical}
    )
    payload.update(
        {
            "content_digest": identity,
            "normalization_version": NORMALIZATION_VERSION,
            "content_version": identity.removeprefix("sha256:"),
            "activity_digest": digest(
                {
                    "normalization_version": NORMALIZATION_VERSION,
                    "activity": payload["activity"],
                }
            ),
        }
    )
    return payload


def _hit_identity(connection, hit, cache):
    mark_search_stage("hit_identity")
    key = (hit["session_id"], hit["turn_number"])
    if key not in cache:
        # The search already enforced visibility. Reconstruction needs only the
        # identity and cwd, not the session's potentially huge captured body.
        session = connection.execute(
            "SELECT id, cwd FROM sessions WHERE id = ?", (key[0],)
        ).fetchone()
        row = connection.execute(
            "SELECT turn_number, start_event_index, end_event_index FROM session_turns "
            "WHERE session_id = ? AND turn_number = ?",
            key,
        ).fetchone()
        turn = _load_turn(connection, session, row)
        cache[key] = {
            name: turn[name]
            for name in (
                "content_digest",
                "normalization_version",
                "content_version",
                "activity_digest",
            )
        }
    turn = cache[key]
    for name in (
        "content_digest",
        "normalization_version",
        "content_version",
        "activity_digest",
    ):
        hit[name] = turn[name]
    if hit.get("matched_field") == "patches" and hit.get("chunk"):
        mark_search_stage("patch_lookup")
        chunk = hit["chunk"]
        record = connection.execute(
            "SELECT f.content FROM session_search_chunks c "
            "JOIN session_search_chunk_fts f ON f.rowid = c.rowid WHERE c.chunk_id = ?",
            (chunk["id"],),
        ).fetchone()
        if record:
            chunk["lines"] = patch_line_ranges(
                record["content"], start_offset=chunk["start_offset"]
            )
            if chunk["start_offset"] and chunk["lines"]:
                chunk["lines"][0]["kind"] = "unknown"


def _execute_query(
    connection, access, snapshot, signer, query, cursor=None, *, evidence_cache=None
):
    normalized = _normalized_query(query)
    fingerprint = digest(normalized)
    page = cursor_page(cursor, fingerprint)
    # Preserve the existing unscoped search planner when fields were omitted.
    fields = tuple(sorted(set(query.fields)))
    facets = tuple(normalized["facets"])
    result = search_turn_hits_raw(
        connection,
        normalized["q"],
        page=page,
        page_size=query.limit,
        project_id=normalized["project_id"],
        repository_id=normalized["repository_id"],
        remote=normalized["remote"],
        root=normalized["root"],
        host=normalized["host"],
        from_timestamp=normalized["from"],
        to_timestamp=normalized["to"],
        sort=query.sort,
        group_by=query.group_by,
        max_hits_per_session=query.max_hits_per_session,
        mode=query.mode,
        fields=fields,
        facets=facets,
        project_access=access,
        exclude_session_ids=normalized["exclude_session_id"],
    )
    payload = _serialize_search_page(
        result,
        query=normalized["q"],
        project_id=normalized["project_id"],
        repository_id=normalized["repository_id"],
        remote=normalized["remote"],
        root=normalized["root"],
        host=normalized["host"],
        from_timestamp=normalized["from"],
        to_timestamp=normalized["to"],
        mode=query.mode,
        fields=fields,
        requested_facets=facets,
        sort=query.sort,
        group_by=query.group_by,
        max_hits_per_session=query.max_hits_per_session,
    )
    payload.update(
        {
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot": snapshot,
            "normalized_query": normalized,
            "excluded_session_ids": normalized["exclude_session_id"],
            "coverage": coverage_readiness(result["coverage"]),
            "next_cursor": encode_cursor(
                signer, page + 1, fingerprint, snapshot["snapshot_id"]
            )
            if result["has_next"]
            else None,
        }
    )
    payload["filters"]["exclude_session_id"] = normalized["exclude_session_id"]
    cache = evidence_cache if evidence_cache is not None else {}
    for hit in payload["hits"]:
        _hit_identity(connection, hit, cache)
    for group in payload["groups"]:
        for hit in group["hits"]:
            _hit_identity(connection, hit, cache)
    mark_search_stage("response")
    return payload


@router.get("/api/v1/search", response_class=JSONResponse)
def search_api(
    request: Request,
    q: str = Query(..., min_length=1, max_length=500),
    project_id: str | None = Query(default=None, max_length=128),
    repository_id: str | None = Query(default=None, max_length=128),
    remote: str | None = Query(default=None, max_length=2048),
    root: str | None = Query(default=None, max_length=2048),
    host: str | None = Query(default=None, max_length=255),
    from_date: datetime | None = Query(default=None, alias="from"),
    to_date: datetime | None = Query(default=None, alias="to"),
    exclude_session_id: list[str] = Query(default=[], max_length=100),
    mode: SearchMode = Query(default="all"),
    fields: str | None = Query(default=None, max_length=200),
    facets: str | None = Query(default=None, max_length=200),
    sort: Literal["relevance", "time_asc", "time_desc"] = Query(default="relevance"),
    group_by: Literal["none", "session"] = Query(default="none"),
    max_hits_per_session: int = Query(default=3, ge=1, le=100),
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = Query(default=None, min_length=1, max_length=2048),
    snapshot_id: str | None = Query(default=None, min_length=1, max_length=2048),
) -> JSONResponse:
    query = BatchSearchQuery(
        q=q,
        project_id=project_id,
        repository_id=repository_id,
        remote=remote,
        root=root,
        host=host,
        from_date=from_date,
        to_date=to_date,
        exclude_session_id=exclude_session_id,
        mode=mode,
        fields=list(
            _normalize_api_values(fields, supported=SEARCH_FIELDS, label="field")
        ),
        facets=list(
            _normalize_api_values(facets, supported=SEARCH_FACETS, label="facet")
        ),
        sort=sort,
        group_by=group_by,
        max_hits_per_session=max_hits_per_session,
        limit=limit,
    )
    if "max_hits_per_session" not in request.query_params:
        query.model_fields_set.discard("max_hits_per_session")
    _normalized_query(query)
    with _snapshot(request, snapshot_id, cursor) as (
        connection,
        access,
        snapshot,
        signer,
        cursor_data,
    ):
        payload = _execute_query(
            connection, access, snapshot, signer, query, cursor_data
        )
    return JSONResponse(payload, headers={"Cache-Control": "private, no-store"})


@router.post("/api/v1/search/batch", response_class=JSONResponse)
def search_batch_api(request: Request, body: BatchSearchRequest) -> JSONResponse:
    budget = sum(
        query.limit * (query.max_hits_per_session if query.group_by == "session" else 1)
        for query in body.queries
    )
    if budget > body.max_total_hits:
        raise api_error(
            422,
            "invalid_input",
            "Batch hit budget exceeded: reduce per-query limits or increase max_total_hits",
        )
    for query in body.queries:
        _normalized_query(query)
    with _snapshot(request, body.snapshot_id) as (
        connection,
        access,
        snapshot,
        signer,
        _,
    ):
        evidence_cache = {}
        results = [
            {
                "id": query.id,
                **_execute_query(
                    connection,
                    access,
                    snapshot,
                    signer,
                    query,
                    evidence_cache=evidence_cache,
                ),
            }
            for query in body.queries
        ]
    returned = sum(
        len(result["hits"]) + sum(len(group["hits"]) for group in result["groups"])
        for result in results
    )
    return JSONResponse(
        {
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot": snapshot,
            "normalized_request": {
                "snapshot_id": snapshot["snapshot_id"],
                "max_total_hits": body.max_total_hits,
                "queries": [
                    {"id": result["id"], **result["normalized_query"]}
                    for result in results
                ],
            },
            "query_count": len(results),
            "returned_hit_count": returned,
            "max_total_hits": body.max_total_hits,
            "results": results,
        },
        headers={"Cache-Control": "private, no-store"},
    )


@router.get("/api/v1/projects", response_class=JSONResponse)
def projects_api(
    request: Request,
    repository_id: str | None = Query(default=None, max_length=128),
    remote: str | None = Query(default=None, max_length=2048),
    root: str | None = Query(default=None, max_length=2048),
    host: str | None = Query(default=None, max_length=255),
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = Query(default=None, min_length=1, max_length=2048),
    snapshot_id: str | None = Query(default=None, min_length=1, max_length=2048),
) -> JSONResponse:
    remote, root = _normalize_repository_filters(remote, root)
    filters = {
        "repository_id": str(repository_id or "").strip() or None,
        "remote": remote,
        "root": root,
        "host": str(host or "").strip() or None,
    }
    normalized = {**filters, "limit": limit}
    fingerprint = digest({"endpoint": "projects", **normalized})
    with _snapshot(request, snapshot_id, cursor) as (
        connection,
        access,
        snapshot,
        signer,
        cursor_data,
    ):
        page = cursor_page(cursor_data, fingerprint)
        result = list_repository_projects(
            connection, **filters, page=page, page_size=limit, project_access=access
        )
    return JSONResponse(
        {
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot": snapshot,
            "normalized_request": {
                **normalized,
                "snapshot_id": snapshot["snapshot_id"],
            },
            "filters": filters,
            "projects": result["items"],
            "total_count": int(result["total_count"]),
            "limit": limit,
            "next_cursor": encode_cursor(
                signer, page + 1, fingerprint, snapshot["snapshot_id"]
            )
            if result["has_next"]
            else None,
        },
        headers={"Cache-Control": "private, no-store"},
    )


def _get_session(connection, access, session_id):
    session = fetch_session_with_project(connection, session_id)
    if session is None or not row_is_visible_to_project_access(session, access):
        raise api_error(404, "not_found", "Session not found")
    return session


def _conditional_response(request, payload, etag):
    quoted_etag = '"' + etag + '"'
    headers = {
        "Cache-Control": "private, no-store",
        # Snapshot/retrieval metadata may differ while the evidence is unchanged.
        "ETag": "W/" + quoted_etag,
        "X-Snapshot-ID": payload["snapshot_id"],
    }
    candidates = [
        value.strip().removeprefix("W/")
        for value in request.headers.get("if-none-match", "").split(",")
    ]
    if "*" in candidates or quoted_etag in candidates:
        return Response(status_code=304, headers=headers)
    return JSONResponse(payload, headers=headers)


@router.get(
    "/api/v1/sessions/{session_id}/turns/{turn_number}", response_class=JSONResponse
)
def session_turn_api(
    request: Request,
    session_id: str,
    turn_number: int = Path(..., ge=1),
    context_turns: int = Query(default=0, alias="context", ge=0, le=10),
    include: str | None = Query(default=None, max_length=100),
    snapshot_id: str | None = Query(default=None, min_length=1, max_length=2048),
):
    includes = _parse_turn_includes(include)
    with _snapshot(request, snapshot_id) as (connection, access, snapshot, _, _):
        session = _get_session(connection, access, session_id)
        rows = connection.execute(
            "SELECT * FROM session_turns WHERE session_id = ? AND turn_number BETWEEN ? AND ? ORDER BY turn_number",
            (
                session_id,
                max(1, turn_number - context_turns),
                turn_number + context_turns,
            ),
        ).fetchall()
        if not any(row["turn_number"] == turn_number for row in rows):
            raise api_error(404, "not_found", "Turn not found")
        turns = [_load_turn(connection, session, row) for row in rows]
        for turn in turns:
            turn["is_target"] = turn["turn_number"] == turn_number
            if "activity" not in includes:
                turn.pop("activity", None)
        project = effective_project_fields(session)
        target = next(turn for turn in turns if turn["is_target"])
        payload = {
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot": snapshot,
            "normalized_request": {
                "session_id": session_id,
                "turn_number": turn_number,
                "context": context_turns,
                "include": sorted(includes),
                "snapshot_id": snapshot["snapshot_id"],
            },
            "session_id": session_id,
            "requested_turn": turn_number,
            "context": {
                "requested": context_turns,
                "first_turn": rows[0]["turn_number"],
                "last_turn": rows[-1]["turn_number"],
            },
            "include": sorted(includes),
            "project": {
                "id": project["project_id"],
                "key": project["effective_group_key"],
                "label": project["display_label"],
                "host": project["source_host"],
            },
            "repository": {
                "id": str(session["repository_id"] or "").strip() or None,
                "remote": str(
                    session["git_repository_url"] or session["github_remote_url"] or ""
                ).strip()
                or None,
                "root": project["cwd"],
                "branch": str(session["git_branch"] or "").strip() or None,
                "head": str(session["git_commit_hash"] or "").strip() or None,
                "dirty": None,
            },
            "turns": turns,
            "links": {
                "conversation": f"/sessions/{quote(session_id, safe='')}?turn={turn_number}",
                "audit": f"/sessions/{quote(session_id, safe='')}?view=audit&turn={turn_number}&focus=1",
            },
            **{
                key: target[key]
                for key in (
                    "content_digest",
                    "content_version",
                    "normalization_version",
                    "activity_digest",
                )
            },
        }
    etag = (
        target["content_digest"]
        if not context_turns and "activity" in includes
        else digest(
            {
                "turns": [turn["content_digest"] for turn in turns],
                "include": sorted(includes),
            }
        )
    )
    return _conditional_response(request, payload, etag)


@router.get(
    "/api/v1/sessions/{session_id}/turns/{turn_number}/activity",
    response_class=JSONResponse,
)
def turn_activity_api(
    request: Request,
    session_id: str,
    turn_number: int = Path(..., ge=1),
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = Query(default=None, min_length=1, max_length=2048),
    snapshot_id: str | None = Query(default=None, min_length=1, max_length=2048),
    kind: str | None = Query(default=None, min_length=1, max_length=128),
    event_type: str | None = Query(default=None, min_length=1, max_length=128),
    tool_name: str | None = Query(default=None, min_length=1, max_length=128),
    from_event_index: int | None = Query(default=None, ge=0),
    to_event_index: int | None = Query(default=None, ge=0),
    from_date: datetime | None = Query(default=None, alias="from"),
    to_date: datetime | None = Query(default=None, alias="to"),
):
    start, end = _validate_date_range(from_date, to_date)
    if (
        from_event_index is not None
        and to_event_index is not None
        and from_event_index > to_event_index
    ):
        raise api_error(
            422, "invalid_input", "from_event_index must be <= to_event_index"
        )
    normalized = {
        "session_id": session_id,
        "turn_number": turn_number,
        "limit": limit,
        "kind": kind,
        "event_type": event_type,
        "tool_name": tool_name,
        "from_event_index": from_event_index,
        "to_event_index": to_event_index,
        "from": start,
        "to": end,
    }
    fingerprint = digest({"endpoint": "activity", **normalized})
    with _snapshot(request, snapshot_id, cursor) as (
        connection,
        access,
        snapshot,
        signer,
        cursor_data,
    ):
        page = cursor_page(cursor_data, fingerprint)
        session = _get_session(connection, access, session_id)
        row = connection.execute(
            "SELECT * FROM session_turns WHERE session_id = ? AND turn_number = ?",
            (session_id, turn_number),
        ).fetchone()
        if row is None:
            raise api_error(404, "not_found", "Turn not found")
        turn = _load_turn(connection, session, row)
        events = []
        from_time, to_time = parse_timestamp(start), parse_timestamp(end)
        for ordinal, event in enumerate(turn["activity"]):
            index = event["event_index"]
            if kind is not None and event["kind"] != kind:
                continue
            if event_type is not None and event["payload_type"] != event_type:
                continue
            if tool_name is not None and event["tool_name"] != tool_name:
                continue
            if from_event_index is not None and (
                index is None or index < from_event_index
            ):
                continue
            if to_event_index is not None and (index is None or index > to_event_index):
                continue
            timestamp = parse_timestamp(event["timestamp"])
            if from_time and (timestamp is None or timestamp < from_time):
                continue
            if to_time and (timestamp is None or timestamp > to_time):
                continue
            events.append(
                (
                    timestamp
                    if timestamp is not None
                    else datetime.min.replace(tzinfo=UTC),
                    index if index is not None else -1,
                    ordinal,
                    event,
                )
            )
        events.sort(key=lambda item: item[:3])
        total = len(events)
        selected = events[(page - 1) * limit : page * limit]
        items = [
            {
                **event,
                "activity_ordinal": ordinal,
                "activity_id": digest(
                    {"turn": turn["activity_digest"], "ordinal": ordinal}
                ),
            }
            for _, _, ordinal, event in selected
        ]
        payload = {
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot": snapshot,
            "normalized_request": {
                **normalized,
                "snapshot_id": snapshot["snapshot_id"],
            },
            "session_id": session_id,
            "turn_number": turn_number,
            "total_count": total,
            "returned_count": len(items),
            "limit": limit,
            "activity": items,
            "first_event_index": items[0]["event_index"] if items else None,
            "last_event_index": items[-1]["event_index"] if items else None,
            "next_cursor": encode_cursor(
                signer, page + 1, fingerprint, snapshot["snapshot_id"]
            )
            if page * limit < total
            else None,
            **{
                key: turn[key]
                for key in (
                    "content_digest",
                    "content_version",
                    "normalization_version",
                    "activity_digest",
                )
            },
        }
    return _conditional_response(
        request,
        payload,
        digest(
            {
                "activity_digest": turn["activity_digest"],
                "request": normalized,
                "page": page,
            }
        ),
    )
