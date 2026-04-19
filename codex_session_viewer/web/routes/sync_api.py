from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from ...agents import (
    fetch_pending_remote_actions,
    upsert_remote_agent_status,
)
from ...db import connect, write_transaction
from ...importer import (
    fetch_host_sync_manifest,
    parse_session_text,
    parsed_session_from_payload,
    upsert_parsed_session,
)
from ...projects import ignored_project_keys
from ..auth import require_sync_api_auth
from ..context import get_settings


router = APIRouter()


@router.get("/api/health")
def health(request: Request) -> dict[str, str]:
    settings = get_settings(request)
    return {
        "status": "ok",
        "app_version": settings.app_version,
        "sync_api_version": settings.sync_api_version,
        "expected_agent_version": settings.expected_agent_version,
    }


@router.get("/api/sync/manifest")
def sync_manifest(request: Request, host: str = Query(...)) -> JSONResponse:
    require_sync_api_auth(request)
    settings = get_settings(request)
    with connect(settings.database_path) as connection:
        sessions = fetch_host_sync_manifest(connection, host)
        ignored_keys = sorted(ignored_project_keys(connection))
        actions = fetch_pending_remote_actions(connection, host)
    return JSONResponse(
        {
            "host": host,
            "sessions": sessions,
            "ignored_project_keys": ignored_keys,
            "actions": actions,
            "server": {
                "app_version": settings.app_version,
                "sync_api_version": settings.sync_api_version,
                "expected_agent_version": settings.expected_agent_version,
            },
        }
    )


@router.post("/api/sync/heartbeat")
async def sync_heartbeat(request: Request) -> JSONResponse:
    require_sync_api_auth(request)
    settings = get_settings(request)
    try:
        payload = await request.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Heartbeat payload must be an object")

    source_host = str(payload.get("source_host") or "").strip()
    if not source_host:
        raise HTTPException(status_code=400, detail="Heartbeat payload is missing source_host")

    with connect(settings.database_path) as connection:
        with write_transaction(connection):
            upsert_remote_agent_status(
                connection,
                source_host=source_host,
                agent_version=str(payload.get("agent_version") or ""),
                sync_api_version=str(payload.get("sync_api_version") or ""),
                sync_mode=str(payload.get("sync_mode") or ""),
                update_state=str(payload.get("update_state") or ""),
                update_message=str(payload.get("update_message") or "") or None,
                server_version_seen=str(payload.get("server_version_seen") or "") or None,
                server_api_version_seen=str(payload.get("server_api_version_seen") or "") or None,
                last_sync_at=str(payload.get("last_sync_at") or "") or None,
                last_upload_count=int(payload.get("last_upload_count") or 0),
                last_skip_count=int(payload.get("last_skip_count") or 0),
                last_fail_count=int(payload.get("last_fail_count") or 0),
                last_error=str(payload.get("last_error") or "") or None,
                acknowledged_raw_resend_token=str(payload.get("acknowledged_raw_resend_token") or "") or None,
                last_raw_resend_at=str(payload.get("last_raw_resend_at") or "") or None,
            )
    return JSONResponse({"status": "ok", "source_host": source_host})


@router.post("/api/sync/session")
async def sync_session(request: Request) -> JSONResponse:
    require_sync_api_auth(request)
    settings = get_settings(request)
    try:
        payload = await request.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Sync payload must be an object")
    try:
        parsed = parsed_session_from_payload(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    with connect(settings.database_path) as connection:
        with write_transaction(connection):
            if parsed.inferred_project_key in ignored_project_keys(connection):
                return JSONResponse(
                    {
                        "status": "ignored",
                        "session_id": parsed.session_id,
                        "source_host": parsed.source_host,
                        "inferred_project_key": parsed.inferred_project_key,
                    }
                )
            upsert_parsed_session(connection, parsed)

    return JSONResponse(
        {
            "status": "ok",
            "session_id": parsed.session_id,
            "source_host": parsed.source_host,
            "event_count": parsed.event_count,
            "content_sha256": parsed.content_sha256,
        }
    )


@router.post("/api/sync/session-raw")
async def sync_session_raw(request: Request) -> JSONResponse:
    require_sync_api_auth(request)
    settings = get_settings(request)
    try:
        payload = await request.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Raw sync payload must be an object")

    source_host = str(payload.get("source_host") or "").strip()
    source_root = str(payload.get("source_root") or "").strip()
    source_path = str(payload.get("source_path") or "").strip()
    raw_jsonl = payload.get("raw_jsonl")
    file_size = payload.get("file_size")
    file_mtime_ns = payload.get("file_mtime_ns")
    header_host = request.headers.get("x-codex-viewer-host", "").strip()

    if not source_host or not source_root or not source_path or not isinstance(raw_jsonl, str):
        raise HTTPException(status_code=400, detail="Raw sync payload is missing required fields")
    if header_host and header_host != source_host:
        raise HTTPException(status_code=400, detail="Source host header did not match raw sync payload")
    if not isinstance(file_size, int) or not isinstance(file_mtime_ns, int):
        raise HTTPException(status_code=400, detail="Raw sync payload is missing file metadata")

    try:
        parsed = parse_session_text(
            raw_jsonl,
            Path(source_path),
            Path(source_root),
            source_host,
            file_size=file_size,
            file_mtime_ns=file_mtime_ns,
        )
    except (ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    with connect(settings.database_path) as connection:
        with write_transaction(connection):
            if parsed.inferred_project_key in ignored_project_keys(connection):
                return JSONResponse(
                    {
                        "status": "ignored",
                        "session_id": parsed.session_id,
                        "source_host": parsed.source_host,
                        "inferred_project_key": parsed.inferred_project_key,
                    }
                )
            upsert_parsed_session(connection, parsed)

    return JSONResponse(
        {
            "status": "ok",
            "mode": "raw",
            "session_id": parsed.session_id,
            "source_host": parsed.source_host,
            "event_count": parsed.event_count,
            "content_sha256": parsed.content_sha256,
        }
    )
