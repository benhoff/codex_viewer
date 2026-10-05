from __future__ import annotations

import codecs
from dataclasses import dataclass
import gzip
import hashlib
import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from ...agents import (
    fetch_pending_remote_actions,
    upsert_remote_agent_status,
)
from ...alerts import reconcile_remote_alerts_for_host
from ...config import Settings
from ...db import connection_scope, write_transaction
from ...importer import (
    append_parsed_session_tail,
    fetch_host_sync_manifest,
    upsert_parsed_session,
)
from ...session_parsing import (
    normalize_jsonl_line,
    parse_codex_session_tail_events,
    parse_session_text,
    parsed_session_from_payload,
    session_content_sha256,
)
from ...onboarding import (
    reconcile_onboarding_state,
    record_first_heartbeat,
    record_first_session_ingested,
)
from ...projects import ignored_project_keys, sync_project_registry
from ...session_artifacts import (
    iter_session_artifact_bytes,
    load_session_artifact_text,
    raw_session_sha256,
    store_session_artifact_tail,
    store_session_artifact,
    utc_now_iso,
)
from ..auth import require_sync_api_auth
from ..concurrency import (
    run_in_history_threadpool as run_in_threadpool,
    run_in_upload_threadpool,
)
from ..context import get_settings


router = APIRouter()
RAW_SYNC_BATCH_MAX_ITEMS = 25


def _upload_payload_identity(payload: object) -> tuple[object, ...]:
    if not isinstance(payload, dict):
        return ("invalid", type(payload).__name__)
    session = payload.get("session")
    source = session if isinstance(session, dict) else payload
    return (
        str(source.get("source_host") or ""),
        str(source.get("source_path") or ""),
        source.get("file_size") if isinstance(source.get("file_size"), int) else None,
        source.get("file_mtime_ns") if isinstance(source.get("file_mtime_ns"), int) else None,
        str(source.get("content_sha256") or ""),
        str(source.get("base_content_sha256") or ""),
    )


def _upload_work_key(kind: str, payload: object) -> tuple[object, ...]:
    if kind == "raw-batch" and isinstance(payload, list):
        return ("session-upload", kind, tuple(_upload_payload_identity(item) for item in payload))
    return ("session-upload", kind, _upload_payload_identity(payload))


async def _read_json_request_payload(request: Request) -> object:
    try:
        body = await request.body()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail="Unable to read request body") from exc

    content_encoding = request.headers.get("content-encoding", "").strip().lower()
    return await run_in_threadpool(_decode_json_request_body, body, content_encoding)


def _decode_json_request_body(body: bytes, content_encoding: str) -> object:
    if content_encoding == "gzip":
        try:
            body = gzip.decompress(body)
        except OSError as exc:
            raise HTTPException(status_code=400, detail="Invalid gzip request body") from exc
    elif content_encoding not in {"", "identity"}:
        raise HTTPException(status_code=400, detail="Unsupported content encoding")

    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc


@router.get("/api/health")
async def health(request: Request) -> dict[str, str]:
    settings = get_settings(request)
    return {
        "status": "ok",
        "app_version": settings.app_version,
        "sync_api_version": settings.sync_api_version,
        "expected_agent_version": settings.expected_agent_version,
    }


@router.get("/api/sync/manifest")
async def sync_manifest(request: Request, host: str = Query(...)) -> JSONResponse:
    await require_sync_api_auth(request)
    settings = get_settings(request)
    return JSONResponse(await run_in_threadpool(_build_sync_manifest, settings, host, False))


def _build_sync_manifest(settings: Settings, host: str, protocol_v2: bool) -> dict[str, object]:
    with connection_scope(settings.database_path) as connection:
        sessions = fetch_host_sync_manifest(connection, host)
        ignored_keys = sorted(ignored_project_keys(connection))
        actions = fetch_pending_remote_actions(connection, host)
    if protocol_v2:
        for session in sessions:
            session["accepted_size"] = int(session.get("file_size") or 0)
            session["accepted_event_count"] = int(session.get("event_count") or 0)
            session["append_supported"] = bool(session.get("has_raw_artifact"))
    result: dict[str, object] = {
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
    if protocol_v2:
        result["protocol"] = "manifest-v2"
    return result


@router.get("/api/sync/manifest-v2")
async def sync_manifest_v2(request: Request, host: str = Query(...)) -> JSONResponse:
    await require_sync_api_auth(request)
    settings = get_settings(request)
    return JSONResponse(await run_in_threadpool(_build_sync_manifest, settings, host, True))


@router.post("/api/sync/heartbeat")
async def sync_heartbeat(request: Request) -> JSONResponse:
    await require_sync_api_auth(request)
    settings = get_settings(request)
    payload = await _read_json_request_payload(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Heartbeat payload must be an object")

    source_host = str(payload.get("source_host") or "").strip()
    if not source_host:
        raise HTTPException(status_code=400, detail="Heartbeat payload is missing source_host")

    return JSONResponse(
        await run_in_threadpool(
            _process_sync_heartbeat,
            settings,
            payload,
            source_host,
        )
    )


def _process_sync_heartbeat(
    settings: Settings,
    payload: dict[str, object],
    source_host: str,
) -> dict[str, object]:
    with connection_scope(settings.database_path) as connection:
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
                last_failed_source_path=str(payload.get("last_failed_source_path") or "") or None,
                last_failure_detail=str(payload.get("last_failure_detail") or "") or None,
                acknowledged_raw_resend_token=str(payload.get("acknowledged_raw_resend_token") or "") or None,
                last_raw_resend_at=str(payload.get("last_raw_resend_at") or "") or None,
            )
            reconcile_remote_alerts_for_host(connection, settings, source_host)
            record_first_heartbeat(
                connection,
                source_host=source_host,
                seen_at=str(payload.get("last_seen_at") or "") or None,
            )
            reconcile_onboarding_state(connection, settings)
    return {"status": "ok", "source_host": source_host}


@router.post("/api/sync/session")
async def sync_session(request: Request) -> JSONResponse:
    await require_sync_api_auth(request)
    settings = get_settings(request)
    payload = await _read_json_request_payload(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Sync payload must be an object")
    return JSONResponse(
        await run_in_upload_threadpool(
            _process_sync_session,
            settings,
            payload,
            dedupe_key=_upload_work_key("parsed", payload),
        )
    )


def _process_sync_session(
    settings: Settings,
    payload: dict[str, object],
) -> dict[str, object]:
    try:
        parsed = parsed_session_from_payload(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    with connection_scope(settings.database_path) as connection:
        with write_transaction(connection):
            if parsed.inferred_project_key in ignored_project_keys(connection):
                return {
                    "status": "ignored",
                    "session_id": parsed.session_id,
                    "source_host": parsed.source_host,
                    "inferred_project_key": parsed.inferred_project_key,
                }
            upsert_parsed_session(connection, parsed)
            sync_project_registry(connection)
            record_first_session_ingested(
                connection,
                source_host=parsed.source_host,
                imported_at=parsed.imported_at,
            )
            reconcile_onboarding_state(connection, settings)


    return {
        "status": "ok",
        "session_id": parsed.session_id,
        "source_host": parsed.source_host,
        "event_count": parsed.event_count,
        "content_sha256": parsed.content_sha256,
    }


@router.post("/api/sync/session-raw")
async def sync_session_raw(request: Request) -> JSONResponse:
    await require_sync_api_auth(request)
    settings = get_settings(request)
    payload = await _read_json_request_payload(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Raw sync payload must be an object")

    header_host = request.headers.get("x-codex-viewer-host", "").strip()
    result = await run_in_upload_threadpool(
        _process_raw_sync_session,
        settings,
        payload,
        dedupe_key=_upload_work_key("raw", payload),
        header_host=header_host,
    )
    result["mode"] = "raw"
    return JSONResponse(result)


def _process_raw_sync_session(
    settings: Settings,
    payload: dict[str, object],
    *,
    header_host: str,
) -> dict[str, object]:
    parsed, raw_jsonl = _parse_raw_sync_payload(
        payload,
        header_host=header_host,
    )

    with connection_scope(settings.database_path) as connection:
        with write_transaction(connection):
            results = store_raw_sync_sessions_batch(connection, settings, [(parsed, raw_jsonl)])

    return results[0]


@router.post("/api/sync/sessions-raw")
async def sync_sessions_raw_batch(request: Request) -> JSONResponse:
    await require_sync_api_auth(request)
    settings = get_settings(request)
    payload = await _read_json_request_payload(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Raw sync batch payload must be an object")

    sessions = payload.get("sessions")
    if not isinstance(sessions, list) or not sessions:
        raise HTTPException(status_code=400, detail="Raw sync batch payload must include a non-empty sessions list")
    if len(sessions) > RAW_SYNC_BATCH_MAX_ITEMS:
        raise HTTPException(
            status_code=400,
            detail=f"Raw sync batch payload exceeded max size of {RAW_SYNC_BATCH_MAX_ITEMS}",
        )

    header_host = request.headers.get("x-codex-viewer-host", "").strip()
    return JSONResponse(
        await run_in_upload_threadpool(
            _process_raw_sync_sessions_batch,
            settings,
            sessions,
            dedupe_key=_upload_work_key("raw-batch", sessions),
            header_host=header_host,
        )
    )


def _process_raw_sync_sessions_batch(
    settings: Settings,
    sessions: list[object],
    *,
    header_host: str,
) -> dict[str, object]:
    results: list[dict[str, object]] = []
    with connection_scope(settings.database_path) as connection:
        # Bound each transaction to one session. A large batch can then make
        # durable progress without holding the SQLite writer and WAL for every
        # item in the request at once.
        for item in sessions:
            parsed_item = _parse_raw_sync_payload(item, header_host=header_host)
            with write_transaction(connection):
                results.extend(
                    store_raw_sync_sessions_batch(connection, settings, [parsed_item])
                )

    return {
        "status": "ok",
        "mode": "raw_batch",
        "results": results,
        "processed_count": len(results),
    }


@router.post("/api/sync/session-tail")
async def sync_session_tail(request: Request) -> JSONResponse:
    await require_sync_api_auth(request)
    settings = get_settings(request)
    payload = await _read_json_request_payload(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Tail sync payload must be an object")

    header_host = request.headers.get("x-codex-viewer-host", "").strip()

    result = await run_in_upload_threadpool(
        _process_raw_sync_session_tail,
        settings,
        payload,
        dedupe_key=_upload_work_key("raw-tail", payload),
        header_host=header_host,
    )
    result["mode"] = "raw_tail"
    return JSONResponse(result)


def _process_raw_sync_session_tail(
    settings: Settings,
    payload: dict[str, object],
    *,
    header_host: str,
) -> dict[str, object]:
    with connection_scope(settings.database_path) as connection:
        # Historical artifact reads and whole-transcript hashes do not need
        # the SQLite writer. Revalidate the base after acquiring it below.
        prepared_base = _prepare_tail_base(connection, settings, payload)
        with write_transaction(connection):
            result = store_raw_sync_session_tail(
                connection,
                settings,
                payload,
                header_host=header_host,
                prepared_base=prepared_base,
            )
    return result


def _parse_raw_sync_payload(
    payload: object,
    *,
    header_host: str,
) -> tuple[object, str]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Raw sync payload must be an object")

    source_host = str(payload.get("source_host") or "").strip()
    source_root = str(payload.get("source_root") or "").strip()
    source_path = str(payload.get("source_path") or "").strip()
    raw_jsonl = payload.get("raw_jsonl")
    file_size = payload.get("file_size")
    file_mtime_ns = payload.get("file_mtime_ns")

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
    return parsed, raw_jsonl


@dataclass(frozen=True)
class PreparedTailBase:
    artifact_sha256: str
    base_size: int
    base_line_count: int
    base_ends_newline: bool
    content_sha256: str
    combined_sha256: str


def _prepare_tail_base(connection, settings, payload):
    if not isinstance(payload.get("tail_jsonl"), str):
        return None
    existing = connection.execute("""
        SELECT file_size, content_sha256, raw_artifact_sha256, raw_meta_json FROM sessions
        WHERE source_host = ? AND source_path = ?
    """, (str(payload.get("source_host") or ""), str(payload.get("source_path") or ""))).fetchone()
    if existing is None or existing["file_size"] != payload.get("base_file_size") or existing["content_sha256"] != payload.get("base_content_sha256"):
        return None
    try:
        if json.loads(existing["raw_meta_json"]).get("transcript_format") == "claude":
            return None  # Claude retains the full-import compatibility path.
    except (ValueError, AttributeError):
        pass
    sha = str(existing["raw_artifact_sha256"] or "")
    if not sha:
        return None
    raw_hash, content_hash = hashlib.sha256(), hashlib.sha256()
    decoder = codecs.getincrementaldecoder("utf-8")()
    pending = ""
    line_count = 0
    base_size = 0
    last_byte = b""

    def feed(block):
        nonlocal pending, line_count
        raw_hash.update(block)
        lines = (pending + decoder.decode(block)).splitlines(keepends=True)
        pending = lines.pop() if lines else ""
        for line in lines:
            line_count += 1
            normalized = normalize_jsonl_line(line, line_count)
            if normalized is not None:
                content_hash.update(normalized.encode("utf-8"))

    try:
        for block in iter_session_artifact_bytes(connection, settings, sha):
            base_size += len(block)
            last_byte = block[-1:]
            feed(block)
    except FileNotFoundError:
        return None
    base_line_count = line_count + bool(pending)
    feed(payload["tail_jsonl"].encode("utf-8"))
    pending += decoder.decode(b"", final=True)
    for line in pending.splitlines(keepends=True):
        line_count += 1
        normalized = normalize_jsonl_line(line, line_count)
        if normalized is not None:
            content_hash.update(normalized.encode("utf-8"))
    return PreparedTailBase(sha, base_size, base_line_count,
                            last_byte in (b"\n", b"\r"),
                            content_hash.hexdigest(), raw_hash.hexdigest())


def store_raw_sync_session_tail(
    connection,
    settings,
    payload: dict[str, object],
    *,
    header_host: str = "",
    prepared_base: PreparedTailBase | None = None,
) -> dict[str, object]:
    source_host = str(payload.get("source_host") or "").strip()
    source_root = str(payload.get("source_root") or "").strip()
    source_path = str(payload.get("source_path") or "").strip()
    base_file_size = payload.get("base_file_size")
    base_content_sha256 = str(payload.get("base_content_sha256") or "").strip()
    tail_jsonl = payload.get("tail_jsonl")
    file_size = payload.get("file_size")
    file_mtime_ns = payload.get("file_mtime_ns")

    if not source_host or not source_root or not source_path or not isinstance(tail_jsonl, str):
        raise HTTPException(status_code=400, detail="Tail sync payload is missing required fields")
    if header_host and header_host != source_host:
        raise HTTPException(status_code=400, detail="Source host header did not match tail sync payload")
    if not isinstance(base_file_size, int) or not base_content_sha256:
        raise HTTPException(status_code=400, detail="Tail sync payload is missing base metadata")
    if not isinstance(file_size, int) or not isinstance(file_mtime_ns, int):
        raise HTTPException(status_code=400, detail="Tail sync payload is missing file metadata")
    existing = connection.execute(
        """
        SELECT
            id,
            file_size,
            content_sha256,
            raw_artifact_sha256,
            raw_meta_json,
            inferred_project_key
        FROM sessions
        WHERE source_host = ?
          AND source_path = ?
        """,
        (source_host, source_path),
    ).fetchone()
    if existing is None:
        return {"status": "base_mismatch", "reason": "missing-session"}
    if int(existing["file_size"] or 0) != base_file_size:
        return {"status": "base_mismatch", "reason": "size-mismatch"}
    if str(existing["content_sha256"] or "") != base_content_sha256:
        return {"status": "base_mismatch", "reason": "content-hash-mismatch"}
    if file_size != base_file_size + len(tail_jsonl.encode("utf-8")):
        return {"status": "base_mismatch", "reason": "tail-size-mismatch"}

    artifact_sha256 = str(existing["raw_artifact_sha256"] or "").strip()
    if not artifact_sha256:
        return {"status": "base_mismatch", "reason": "missing-raw-artifact"}
    if prepared_base is not None and prepared_base.artifact_sha256 == artifact_sha256:
        base_size = prepared_base.base_size
        base_line_count = prepared_base.base_line_count
        base_ends_newline = prepared_base.base_ends_newline
        combined_raw_jsonl = None
    else:
        prepared_base = None
        base_raw_jsonl = load_session_artifact_text(connection, settings, artifact_sha256)
        if base_raw_jsonl is None:
            return {"status": "base_mismatch", "reason": "missing-raw-artifact"}
        base_size = len(base_raw_jsonl.encode("utf-8"))
        base_line_count = len(base_raw_jsonl.splitlines(keepends=True))
        base_ends_newline = base_raw_jsonl.endswith(("\n", "\r"))
        combined_raw_jsonl = base_raw_jsonl + tail_jsonl
    if base_size != base_file_size:
        return {"status": "base_mismatch", "reason": "artifact-size-mismatch"}
    try:
        raw_meta = json.loads(str(existing["raw_meta_json"] or "{}"))
    except json.JSONDecodeError:
        raw_meta = {}
    if isinstance(raw_meta, dict) and raw_meta.get("transcript_format") == "claude":
        parsed = parse_session_text(
            combined_raw_jsonl,
            Path(source_path),
            Path(source_root),
            source_host,
            file_size=file_size,
            file_mtime_ns=file_mtime_ns,
        )
        result = store_raw_sync_sessions_batch(
            connection,
            settings,
            [(parsed, combined_raw_jsonl)],
        )[0]
        result["incremental"] = False
        return result

    if str(existing["inferred_project_key"] or "") in ignored_project_keys(connection):
        return {
            "status": "ignored",
            "session_id": str(existing["id"]),
            "source_host": source_host,
            "inferred_project_key": str(existing["inferred_project_key"] or ""),
        }

    tail_parse_jsonl = tail_jsonl
    if base_size and not base_ends_newline:
        if tail_parse_jsonl.startswith("\r\n"):
            tail_parse_jsonl = tail_parse_jsonl[2:]
        elif tail_parse_jsonl.startswith(("\n", "\r")):
            tail_parse_jsonl = tail_parse_jsonl[1:]
    try:
        tail_events = parse_codex_session_tail_events(
            tail_parse_jsonl,
            Path(source_path),
            start_line_index=base_line_count,
        )
    except ValueError:
        return {"status": "base_mismatch", "reason": "non-append-tail"}
    combined_content_sha256 = prepared_base.content_sha256 if prepared_base is not None else session_content_sha256(combined_raw_jsonl)
    combined_sha256 = prepared_base.combined_sha256 if prepared_base is not None else raw_session_sha256(combined_raw_jsonl)
    combined_artifact_sha256 = store_session_artifact_tail(
        connection, settings, base_sha256=artifact_sha256,
        tail_jsonl=tail_jsonl, combined_sha256=combined_sha256,
        original_size=file_size,
    )
    return append_parsed_session_tail(
        connection,
        session_id=str(existing["id"]),
        events=tail_events,
        file_size=file_size,
        file_mtime_ns=file_mtime_ns,
        content_sha256=combined_content_sha256,
        raw_artifact_sha256=combined_artifact_sha256,
        updated_at=utc_now_iso(),
    )


def store_raw_sync_sessions_batch(
    connection,
    settings,
    parsed_items: list[tuple[object, str]],
) -> list[dict[str, object]]:
    ignored_keys = ignored_project_keys(connection)
    project_registry_changed = False
    results: list[dict[str, object]] = []

    for parsed, raw_jsonl in parsed_items:
        if parsed.inferred_project_key in ignored_keys:
            results.append(
                {
                    "status": "ignored",
                    "session_id": parsed.session_id,
                    "source_host": parsed.source_host,
                    "inferred_project_key": parsed.inferred_project_key,
                }
            )
            continue

        parsed.raw_artifact_sha256 = store_session_artifact(connection, settings, raw_jsonl)
        upsert_parsed_session(connection, parsed)
        record_first_session_ingested(
            connection,
            source_host=parsed.source_host,
            imported_at=parsed.imported_at,
        )
        project_registry_changed = True
        results.append(
            {
                "status": "ok",
                "session_id": parsed.session_id,
                "source_host": parsed.source_host,
                "event_count": parsed.event_count,
                "content_sha256": parsed.content_sha256,
            }
        )

    if project_registry_changed:
        sync_project_registry(connection)
        reconcile_onboarding_state(connection, settings)

    return results
