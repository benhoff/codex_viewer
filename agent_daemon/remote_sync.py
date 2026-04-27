from __future__ import annotations

import gzip
import hashlib
import json
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .agent_state import (
    connect_agent_state,
    fetch_agent_file_states,
    mark_agent_file_deleted,
    mark_agent_file_uploaded,
    mark_missing_agent_files_deleted,
    upsert_agent_file_state,
)
from .local_machine import load_machine_identity
from .session_source import read_session_source_tail, read_session_source_text
from agent_operations_viewer.config import Settings
from agent_operations_viewer.session_parsing import (
    iter_session_files,
    parse_session_text,
    prescan_session_source,
    SessionParseError,
)
from agent_operations_viewer.machine_auth import build_machine_auth_headers

INVALID_SESSION_CACHE: dict[tuple[str, int, int], str] = {}


class RemoteSyncError(RuntimeError):
    pass


class RestartRequired(RuntimeError):
    pass


@dataclass(slots=True)
class SessionFileCandidate:
    source_root: Path
    path: Path
    file_size: int
    file_mtime_ns: int
    reason: str
    upload_mode: str = "raw"
    base_file_size: int | None = None
    base_content_sha256: str | None = None
    remote_session_id: str | None = None


@dataclass(slots=True)
class PreparedUpload:
    payload: dict[str, object]
    session_id: str
    path: Path
    source_root: Path
    file_size: int
    file_mtime_ns: int
    reason: str
    upload_mode: str
    session_format: str | None
    inferred_project_key: str | None
    inferred_project_label: str | None
    last_line_hash: str | None


@dataclass(slots=True)
class PreparedSkip:
    path: Path
    source_root: Path
    file_size: int
    file_mtime_ns: int
    reason: str
    session_format: str | None
    session_id: str | None
    inferred_project_key: str | None
    inferred_project_label: str | None


@dataclass(slots=True)
class UploadOutcome:
    session_id: str
    path: Path
    reason: str
    status: str
    error: str | None = None
    content_sha256: str | None = None
    event_count: int | None = None


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def exception_summary(exc: BaseException, *, max_length: int = 500) -> str:
    raw = " ".join(part for part in [exc.__class__.__name__, str(exc).strip()] if part).strip()
    summary = " ".join(raw.split())
    if len(summary) > max_length:
        return summary[: max_length - 1].rstrip() + "…"
    return summary


def last_line_hash(raw_jsonl: str) -> str | None:
    stripped = raw_jsonl.rstrip("\r\n")
    if not stripped:
        return None
    line = stripped.splitlines()[-1]
    return hashlib.sha256(line.encode("utf-8")).hexdigest()


def build_headers(
    settings: Settings,
    *,
    method: str,
    path: str,
    raw_body: bytes | None = None,
) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "X-Codex-Viewer-Agent-Version": settings.app_version,
        "X-Codex-Viewer-Api-Version": settings.sync_api_version,
        "X-Codex-Viewer-Sync-Mode": settings.sync_mode,
        "X-Codex-Viewer-Host": settings.source_host,
    }
    identity = load_machine_identity(settings)
    if identity is not None:
        if settings.server_base_url and identity.server_base_url.rstrip("/") != settings.server_base_url.rstrip("/"):
            raise RemoteSyncError(
                "Configured server URL does not match the paired machine credential. "
                "Run `python -m agent_daemon repair --re-pair` to re-pair this machine."
            )
        headers.update(
            build_machine_auth_headers(
                private_key=identity.private_key,
                machine_id=identity.machine_id,
                method=method,
                path=path,
                raw_body=raw_body,
                source_host=settings.source_host,
            )
        )
    elif settings.sync_api_token:
        headers["Authorization"] = f"Bearer {settings.sync_api_token}"
    else:
        raise RemoteSyncError(
            "Remote sync requires either CODEX_VIEWER_SYNC_API_TOKEN or a paired machine credential. "
            "Run `python -m agent_daemon pair` to authorize this machine."
        )
    return headers


def require_server_url(settings: Settings) -> str:
    if settings.server_base_url:
        return settings.server_base_url
    identity = load_machine_identity(settings)
    if identity is not None and identity.server_base_url:
        return identity.server_base_url.rstrip("/")
    raise RemoteSyncError("CODEX_VIEWER_SERVER_URL must be configured for remote sync mode")


def json_request(
    settings: Settings,
    method: str,
    path: str,
    payload: dict[str, object] | None = None,
) -> dict[str, Any]:
    base_url = require_server_url(settings)
    body = None
    if payload is not None:
        raw_body = json.dumps(payload).encode("utf-8")
        if len(raw_body) >= settings.remote_gzip_min_bytes:
            body = gzip.compress(raw_body, compresslevel=6)
        else:
            body = raw_body
    headers = build_headers(
        settings,
        method=method,
        path=path,
        raw_body=body,
    )
    if payload is not None:
        if body is not None and body is not raw_body:
            headers["Content-Encoding"] = "gzip"
        headers["Content-Type"] = "application/json"
    request = Request(f"{base_url}{path}", data=body, method=method.upper(), headers=headers)
    try:
        with urlopen(request, timeout=settings.remote_timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RemoteSyncError(f"Remote sync request failed: {exc.code} {detail}") from exc
    except URLError as exc:
        raise RemoteSyncError(f"Remote sync request failed: {exc.reason}") from exc


def fetch_remote_manifest(
    settings: Settings,
) -> tuple[dict[str, dict[str, object]], set[str], dict[str, Any], dict[str, Any]]:
    manifest_path = f"/api/sync/manifest-v2?host={quote(settings.source_host, safe='')}"
    try:
        payload = json_request(settings, "GET", manifest_path)
    except RemoteSyncError as exc:
        if "404" not in str(exc):
            raise
        payload = json_request(
            settings,
            "GET",
            f"/api/sync/manifest?host={quote(settings.source_host, safe='')}",
        )
    sessions = payload.get("sessions")
    if not isinstance(sessions, list):
        raise RemoteSyncError("Remote manifest response was missing a sessions list")
    by_path: dict[str, dict[str, object]] = {}
    for entry in sessions:
        if not isinstance(entry, dict):
            continue
        source_path = entry.get("source_path")
        if isinstance(source_path, str):
            by_path[source_path] = entry
    ignored_keys = {
        key
        for key in payload.get("ignored_project_keys", [])
        if isinstance(key, str) and key.strip()
    }
    server = payload.get("server")
    if not isinstance(server, dict):
        server = {}
    actions = payload.get("actions")
    if not isinstance(actions, dict):
        actions = {}
    return by_path, ignored_keys, server, actions


def remote_entry_needs_upload(
    entry: dict[str, object] | None,
    source_path: Path,
    file_size: int,
    file_mtime_ns: int,
    force: bool = False,
) -> tuple[bool, str]:
    if force:
        return True, "force"
    if entry is None:
        return True, "missing"

    if entry.get("source_path") != str(source_path):
        return True, "path-mismatch"

    remote_event_count = entry.get("event_count")
    stored_event_count = entry.get("stored_event_count")
    if not isinstance(remote_event_count, int) or not isinstance(stored_event_count, int):
        return True, "invalid-manifest"
    if remote_event_count != stored_event_count:
        return True, "partial-remote"
    if not bool(entry.get("has_raw_artifact")):
        return True, "missing-raw-artifact"

    if entry.get("content_sha256") in {None, ""}:
        return True, "missing-hash"
    if entry.get("file_size") != file_size:
        return True, "size-mismatch"
    if entry.get("file_mtime_ns") != file_mtime_ns:
        return True, "mtime-mismatch"

    return False, "current"


def upload_raw_session(settings: Settings, payload: dict[str, object]) -> dict[str, Any]:
    return json_request(settings, "POST", "/api/sync/session-raw", payload)


def upload_raw_session_tail(settings: Settings, payload: dict[str, object]) -> dict[str, Any]:
    return json_request(settings, "POST", "/api/sync/session-tail", payload)


def upload_raw_sessions_batch(settings: Settings, payloads: list[dict[str, object]]) -> dict[str, Any]:
    return json_request(settings, "POST", "/api/sync/sessions-raw", {"sessions": payloads})


def send_remote_heartbeat(
    settings: Settings,
    *,
    update_state: str,
    update_message: str | None,
    server_meta: dict[str, Any],
    stats: dict[str, int] | None = None,
    last_error: str | None = None,
    last_failed_source_path: str | None = None,
    last_failure_detail: str | None = None,
    last_sync_at: str | None = None,
    acknowledged_raw_resend_token: str | None = None,
    last_raw_resend_at: str | None = None,
) -> None:
    payload = {
        "source_host": settings.source_host,
        "agent_version": settings.app_version,
        "sync_api_version": settings.sync_api_version,
        "sync_mode": settings.sync_mode,
        "update_state": update_state,
        "update_message": update_message,
        "server_version_seen": server_meta.get("expected_agent_version"),
        "server_api_version_seen": server_meta.get("sync_api_version"),
        "last_sync_at": last_sync_at,
        "last_upload_count": int((stats or {}).get("uploaded", 0)),
        "last_skip_count": int((stats or {}).get("skipped", 0)),
        "last_fail_count": int((stats or {}).get("failed", 0)),
        "last_error": last_error,
        "last_failed_source_path": last_failed_source_path,
        "last_failure_detail": last_failure_detail,
        "acknowledged_raw_resend_token": acknowledged_raw_resend_token,
        "last_raw_resend_at": last_raw_resend_at,
    }
    json_request(settings, "POST", "/api/sync/heartbeat", payload)


def evaluate_server_compatibility(settings: Settings, server_meta: dict[str, Any]) -> dict[str, object]:
    expected_agent_version = str(server_meta.get("expected_agent_version") or "").strip() or settings.expected_agent_version
    server_api_version = str(server_meta.get("sync_api_version") or "").strip() or settings.sync_api_version
    version_mismatch = expected_agent_version != settings.app_version
    api_mismatch = server_api_version != settings.sync_api_version

    if api_mismatch:
        return {
            "state": "protocol_mismatch",
            "message": f"Agent API {settings.sync_api_version} does not match server API {server_api_version}",
            "should_sync": False,
            "needs_update": True,
        }
    if version_mismatch:
        return {
            "state": "manual_update_required",
            "message": f"Agent version {settings.app_version} does not match server target {expected_agent_version}",
            "should_sync": True,
            "needs_update": True,
        }
    return {
        "state": "current",
        "message": f"Agent version {settings.app_version} matches server target",
        "should_sync": True,
        "needs_update": False,
    }


def run_local_update_command(settings: Settings) -> str:
    command = settings.agent_update_command
    if not command:
        raise RemoteSyncError("No CODEX_VIEWER_AGENT_UPDATE_COMMAND configured")
    completed = subprocess.run(
        command,
        shell=True,
        cwd=str(settings.project_root),
        text=True,
        capture_output=True,
        timeout=1800,
    )
    output = "\n".join(part for part in [completed.stdout.strip(), completed.stderr.strip()] if part).strip()
    if completed.returncode != 0:
        raise RemoteSyncError(output or f"Update command exited with code {completed.returncode}")
    return output or "Update command completed successfully"


def build_raw_upload_payload(
    settings: Settings,
    *,
    source_root: Path,
    path: Path,
    raw_jsonl: str,
    file_size: int,
    file_mtime_ns: int,
    ) -> dict[str, object]:
    return {
        "source_host": settings.source_host,
        "source_root": str(source_root),
        "source_path": str(path),
        "file_size": file_size,
        "file_mtime_ns": file_mtime_ns,
        "raw_jsonl": raw_jsonl,
    }


def build_raw_tail_upload_payload(
    settings: Settings,
    *,
    source_root: Path,
    path: Path,
    tail_jsonl: str,
    base_file_size: int,
    base_content_sha256: str,
    file_size: int,
    file_mtime_ns: int,
) -> dict[str, object]:
    return {
        "source_host": settings.source_host,
        "source_root": str(source_root),
        "source_path": str(path),
        "base_file_size": base_file_size,
        "base_content_sha256": base_content_sha256,
        "file_size": file_size,
        "file_mtime_ns": file_mtime_ns,
        "tail_jsonl": tail_jsonl,
    }


def _same_cached_file_version(cached_state: dict[str, object] | None, *, file_size: int, file_mtime_ns: int) -> bool:
    if cached_state is None:
        return False
    return cached_state["file_size"] == file_size and cached_state["file_mtime_ns"] == file_mtime_ns


def _int_value(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value)
    return None


def _can_upload_tail(
    cached_state: dict[str, object] | None,
    remote_entry: dict[str, object] | None,
    *,
    local_file_size: int,
) -> tuple[bool, int | None, str | None]:
    if cached_state is None or remote_entry is None:
        return False, None, None
    if not bool(remote_entry.get("has_raw_artifact")):
        return False, None, None

    remote_file_size = _int_value(remote_entry.get("file_size"))
    if remote_file_size is None or local_file_size <= remote_file_size:
        return False, None, None

    remote_content_sha256 = str(remote_entry.get("content_sha256") or "").strip()
    if not remote_content_sha256:
        return False, None, None

    cached_synced_size = _int_value(cached_state["last_synced_size"] if "last_synced_size" in cached_state.keys() else None)
    cached_synced_hash = (
        str(cached_state["last_synced_content_sha256"] or "").strip()
        if "last_synced_content_sha256" in cached_state.keys()
        else ""
    )
    if cached_synced_size != remote_file_size or cached_synced_hash != remote_content_sha256:
        return False, None, None

    return True, remote_file_size, remote_content_sha256


def _match_source_root(path: Path, roots: list[Path]) -> Path | None:
    expanded_path = path.expanduser()
    matches: list[Path] = []
    for root in roots:
        expanded_root = root.expanduser()
        try:
            expanded_path.relative_to(expanded_root)
        except ValueError:
            continue
        matches.append(expanded_root)
    if not matches:
        return None
    return max(matches, key=lambda item: len(item.parts))


def _iter_candidate_session_files(
    session_roots: list[Path],
    candidate_paths: list[Path] | None,
) -> list[tuple[Path, Path]]:
    if candidate_paths is None:
        return list(iter_session_files(session_roots))

    resolved_candidates: list[tuple[Path, Path]] = []
    seen_paths: set[str] = set()
    for raw_path in sorted(candidate_paths, key=lambda item: str(item)):
        path = raw_path.expanduser()
        if str(path) in seen_paths:
            continue
        seen_paths.add(str(path))
        if path.suffix.lower() != ".jsonl" or not path.exists() or not path.is_file():
            continue
        source_root = _match_source_root(path, session_roots)
        if source_root is None:
            continue
        resolved_candidates.append((source_root, path))
    return resolved_candidates


def _prepare_upload_candidate(
    settings: Settings,
    candidate: SessionFileCandidate,
    ignored_keys: set[str],
) -> PreparedUpload | PreparedSkip:
    if candidate.upload_mode == "tail":
        if candidate.base_file_size is None or not candidate.base_content_sha256:
            raise RemoteSyncError("Tail upload candidate is missing base metadata")
        tail_jsonl = read_session_source_tail(candidate.path, candidate.base_file_size)
        if not tail_jsonl:
            raise RemoteSyncError("Tail upload candidate had no appended bytes")
        return PreparedUpload(
            payload=build_raw_tail_upload_payload(
                settings,
                source_root=candidate.source_root,
                path=candidate.path,
                tail_jsonl=tail_jsonl,
                base_file_size=candidate.base_file_size,
                base_content_sha256=candidate.base_content_sha256,
                file_size=candidate.file_size,
                file_mtime_ns=candidate.file_mtime_ns,
            ),
            session_id=candidate.remote_session_id or candidate.path.stem,
            path=candidate.path,
            source_root=candidate.source_root,
            file_size=candidate.file_size,
            file_mtime_ns=candidate.file_mtime_ns,
            reason=candidate.reason,
            upload_mode="tail",
            session_format=None,
            inferred_project_key=None,
            inferred_project_label=None,
            last_line_hash=last_line_hash(tail_jsonl),
        )

    prescanned = prescan_session_source(
        candidate.path,
        candidate.source_root,
        settings.source_host,
    )
    if prescanned is not None and prescanned.inferred_project_key in ignored_keys:
        return PreparedSkip(
            path=candidate.path,
            source_root=candidate.source_root,
            file_size=candidate.file_size,
            file_mtime_ns=candidate.file_mtime_ns,
            reason=candidate.reason,
            session_format=prescanned.session_format,
            session_id=prescanned.session_id,
            inferred_project_key=prescanned.inferred_project_key,
            inferred_project_label=prescanned.inferred_project_label,
        )

    raw_jsonl = read_session_source_text(candidate.path)
    raw_last_line_hash = last_line_hash(raw_jsonl)
    parsed = parse_session_text(
        raw_jsonl,
        candidate.path,
        candidate.source_root,
        settings.source_host,
        file_size=candidate.file_size,
        file_mtime_ns=candidate.file_mtime_ns,
    )
    if parsed.inferred_project_key in ignored_keys:
        return PreparedSkip(
            path=candidate.path,
            source_root=candidate.source_root,
            file_size=candidate.file_size,
            file_mtime_ns=candidate.file_mtime_ns,
            reason=candidate.reason,
            session_format=prescanned.session_format if prescanned is not None else None,
            session_id=parsed.session_id,
            inferred_project_key=parsed.inferred_project_key,
            inferred_project_label=parsed.inferred_project_label,
        )

    return PreparedUpload(
        payload=build_raw_upload_payload(
            settings,
            source_root=candidate.source_root,
            path=candidate.path,
            raw_jsonl=raw_jsonl,
            file_size=candidate.file_size,
            file_mtime_ns=candidate.file_mtime_ns,
        ),
        session_id=parsed.session_id,
        path=candidate.path,
        source_root=candidate.source_root,
        file_size=candidate.file_size,
        file_mtime_ns=candidate.file_mtime_ns,
        reason=candidate.reason,
        upload_mode="raw",
        session_format=prescanned.session_format if prescanned is not None else None,
        inferred_project_key=parsed.inferred_project_key,
        inferred_project_label=parsed.inferred_project_label,
        last_line_hash=raw_last_line_hash,
    )


def _validate_upload_status(response: dict[str, Any]) -> str:
    status = str(response.get("status") or "").strip().lower() or "ok"
    if status not in {"ok", "ignored", "base_mismatch"}:
        raise RemoteSyncError(f"Remote sync response had unexpected status {status!r}")
    return status


def _upload_one_prepared(settings: Settings, item: PreparedUpload) -> UploadOutcome:
    if item.upload_mode == "tail":
        response = upload_raw_session_tail(settings, item.payload)
        status = _validate_upload_status(response)
        if status != "base_mismatch":
            return UploadOutcome(
                session_id=item.session_id,
                path=item.path,
                reason=item.reason,
                status=status,
                content_sha256=str(response.get("content_sha256") or "") or None,
                event_count=_int_value(response.get("event_count")),
            )

        full_item = _prepare_upload_candidate(
            settings,
            SessionFileCandidate(
                source_root=item.source_root,
                path=item.path,
                file_size=item.file_size,
                file_mtime_ns=item.file_mtime_ns,
                reason="tail-base-mismatch",
            ),
            ignored_keys=set(),
        )
        if not isinstance(full_item, PreparedUpload):
            return UploadOutcome(
                session_id=item.session_id,
                path=item.path,
                reason=item.reason,
                status="ignored",
            )
        response = upload_raw_session(settings, full_item.payload)
        status = _validate_upload_status(response)
        return UploadOutcome(
            session_id=full_item.session_id,
            path=full_item.path,
            reason=full_item.reason,
            status=status,
            content_sha256=str(response.get("content_sha256") or "") or None,
            event_count=_int_value(response.get("event_count")),
        )

    response = upload_raw_session(settings, item.payload)
    status = _validate_upload_status(response)
    return UploadOutcome(
        session_id=item.session_id,
        path=item.path,
        reason=item.reason,
        status=status,
        content_sha256=str(response.get("content_sha256") or "") or None,
        event_count=_int_value(response.get("event_count")),
    )


def _upload_prepared_chunk(
    settings: Settings,
    chunk: list[PreparedUpload],
    *,
    batch_uploads_enabled: bool,
) -> list[UploadOutcome]:
    if batch_uploads_enabled and len(chunk) > 1 and all(item.upload_mode == "raw" for item in chunk):
        try:
            response = upload_raw_sessions_batch(settings, [item.payload for item in chunk])
            results = response.get("results")
            if not isinstance(results, list) or len(results) != len(chunk):
                raise RemoteSyncError("Remote batch sync response was missing per-session results")
            outcomes: list[UploadOutcome] = []
            for result, item in zip(results, chunk):
                if not isinstance(result, dict):
                    raise RemoteSyncError("Remote batch sync response contained an invalid item")
                outcomes.append(
                    UploadOutcome(
                        session_id=item.session_id,
                        path=item.path,
                        reason=item.reason,
                        status=_validate_upload_status(result),
                        content_sha256=str(result.get("content_sha256") or "") or None,
                        event_count=_int_value(result.get("event_count")),
                    )
                )
            return outcomes
        except Exception:
            pass

    outcomes: list[UploadOutcome] = []
    for item in chunk:
        try:
            outcomes.append(_upload_one_prepared(settings, item))
        except Exception as exc:
            outcomes.append(
                UploadOutcome(
                    session_id=item.session_id,
                    path=item.path,
                    reason=item.reason,
                    status="failed",
                    error=exception_summary(exc),
                )
            )
    return outcomes


def invalid_session_cache_key(path: Path, file_size: int, file_mtime_ns: int) -> tuple[str, int, int]:
    return (str(path), file_size, file_mtime_ns)


def remember_invalid_session(path: Path, file_size: int, file_mtime_ns: int, message: str) -> None:
    path_str = str(path)
    stale_keys = [key for key in INVALID_SESSION_CACHE if key[0] == path_str and key[1:] != (file_size, file_mtime_ns)]
    for key in stale_keys:
        INVALID_SESSION_CACHE.pop(key, None)
    INVALID_SESSION_CACHE[invalid_session_cache_key(path, file_size, file_mtime_ns)] = message


def sync_sessions_remote(
    settings: Settings,
    force: bool = False,
    *,
    candidate_paths: list[Path] | None = None,
) -> dict[str, int]:
    logger = logging.getLogger("agent_operations_viewer.remote_sync")
    manifest, ignored_keys, server_meta, actions = ({}, set(), {}, {}) if force else fetch_remote_manifest(settings)
    uploaded = 0
    skipped = 0
    failed = 0
    last_failed_source_path: str | None = None
    last_failure_detail: str | None = None
    batch_uploads_enabled = settings.remote_batch_size > 1
    compatibility = evaluate_server_compatibility(settings, server_meta)
    resend_raw_action = actions.get("resend_raw") if isinstance(actions.get("resend_raw"), dict) else None
    raw_resend_token = (
        str(resend_raw_action.get("token") or "").strip()
        if resend_raw_action is not None
        else ""
    )

    if compatibility["needs_update"] and settings.agent_update_command:
        try:
            send_remote_heartbeat(
                settings,
                update_state="updating",
                update_message=str(compatibility["message"]),
                server_meta=server_meta,
            )
            output = run_local_update_command(settings)
            send_remote_heartbeat(
                settings,
                update_state="updated_restart_required",
                update_message=output,
                server_meta=server_meta,
            )
            raise RestartRequired(output)
        except RestartRequired:
            raise
        except Exception as exc:
            logger.exception("Automatic agent update failed")
            send_remote_heartbeat(
                settings,
                update_state="update_failed",
                update_message=str(exc),
                server_meta=server_meta,
                last_error=str(exc),
            )
            compatibility["state"] = "update_failed"
            compatibility["message"] = str(exc)
            compatibility["should_sync"] = False

    if not compatibility["should_sync"]:
        send_remote_heartbeat(
            settings,
            update_state=str(compatibility["state"]),
            update_message=str(compatibility["message"]),
            server_meta=server_meta,
        )
        return {"uploaded": 0, "skipped": 0, "failed": 0}

    with connect_agent_state(settings.agent_state_db_path()) as state_connection:
        cached_states = fetch_agent_file_states(state_connection, roots=settings.session_roots)
        seen_paths: set[str] = set()
        candidates: list[SessionFileCandidate] = []

        explicit_paths = [path.expanduser() for path in candidate_paths or []]
        if candidate_paths is not None:
            deleted_at = utc_now_iso()
            for path in explicit_paths:
                if path.exists() or str(path) not in cached_states:
                    continue
                mark_agent_file_deleted(
                    state_connection,
                    source_path=path,
                    deleted_at=deleted_at,
                )

        for source_root, path in _iter_candidate_session_files(settings.session_roots, candidate_paths):
            stat = path.stat()
            seen_paths.add(str(path))
            cached_state = cached_states.get(str(path))
            cached_parse_error = INVALID_SESSION_CACHE.get(
                invalid_session_cache_key(path, stat.st_size, stat.st_mtime_ns)
            )
            if cached_parse_error is not None:
                skipped += 1
                upsert_agent_file_state(
                    state_connection,
                    source_root=source_root,
                    source_path=path,
                    file_size=stat.st_size,
                    file_mtime_ns=stat.st_mtime_ns,
                    last_seen_at=utc_now_iso(),
                    state="invalid",
                    invalid_reason=cached_parse_error,
                )
                continue

            if _same_cached_file_version(cached_state, file_size=stat.st_size, file_mtime_ns=stat.st_mtime_ns):
                cached_project_key = str(cached_state["inferred_project_key"] or "").strip()
                cached_state_name = str(cached_state["state"] or "").strip()
                if cached_state_name == "invalid":
                    skipped += 1
                    continue
                if cached_project_key and cached_project_key in ignored_keys:
                    skipped += 1
                    continue

            if raw_resend_token:
                candidates.append(
                    SessionFileCandidate(
                        source_root=source_root,
                        path=path,
                        file_size=stat.st_size,
                        file_mtime_ns=stat.st_mtime_ns,
                        reason=f"raw-resend:{raw_resend_token}",
                    )
                )
                continue

            remote_entry = manifest.get(str(path))
            remote_project_key = (
                str(remote_entry.get("inferred_project_key") or "").strip()
                if isinstance(remote_entry, dict)
                else ""
            )
            if remote_project_key and remote_project_key in ignored_keys:
                skipped += 1
                upsert_agent_file_state(
                    state_connection,
                    source_root=source_root,
                    source_path=path,
                    file_size=stat.st_size,
                    file_mtime_ns=stat.st_mtime_ns,
                    last_seen_at=utc_now_iso(),
                    state="ignored",
                    inferred_project_key=remote_project_key,
                    inferred_project_label=(
                        str(remote_entry.get("inferred_project_label") or "").strip() or None
                        if isinstance(remote_entry, dict)
                        else None
                    ),
                )
                continue

            can_tail, base_file_size, base_content_sha256 = _can_upload_tail(
                cached_state,
                remote_entry,
                local_file_size=stat.st_size,
            )
            if can_tail and base_file_size is not None and base_content_sha256 is not None:
                candidates.append(
                    SessionFileCandidate(
                        source_root=source_root,
                        path=path,
                        file_size=stat.st_size,
                        file_mtime_ns=stat.st_mtime_ns,
                        reason="append",
                        upload_mode="tail",
                        base_file_size=base_file_size,
                        base_content_sha256=base_content_sha256,
                        remote_session_id=str(remote_entry.get("id") or "").strip()
                        if isinstance(remote_entry, dict)
                        else None,
                    )
                )
                continue

            needs_upload, reason = remote_entry_needs_upload(
                remote_entry,
                source_path=path,
                file_size=stat.st_size,
                file_mtime_ns=stat.st_mtime_ns,
                force=force,
            )
            if not needs_upload:
                skipped += 1
                upsert_agent_file_state(
                    state_connection,
                    source_root=source_root,
                    source_path=path,
                    file_size=stat.st_size,
                    file_mtime_ns=stat.st_mtime_ns,
                    last_seen_at=utc_now_iso(),
                    state="uploaded",
                    session_format=str(cached_state["session_format"]) if cached_state and cached_state["session_format"] else None,
                    session_id=str(cached_state["session_id"]) if cached_state and cached_state["session_id"] else None,
                    inferred_project_key=(
                        str(cached_state["inferred_project_key"])
                        if cached_state and cached_state["inferred_project_key"]
                        else None
                    ),
                    inferred_project_label=(
                        str(cached_state["inferred_project_label"])
                        if cached_state and cached_state["inferred_project_label"]
                        else None
                    ),
                )
                continue

            candidates.append(
                SessionFileCandidate(
                    source_root=source_root,
                    path=path,
                    file_size=stat.st_size,
                    file_mtime_ns=stat.st_mtime_ns,
                    reason=reason,
                )
            )

        if candidate_paths is None:
            mark_missing_agent_files_deleted(
                state_connection,
                roots=settings.session_roots,
                seen_paths=seen_paths,
                deleted_at=utc_now_iso(),
            )

        prepared_uploads: list[PreparedUpload] = []
        if candidates:
            with ThreadPoolExecutor(max_workers=settings.remote_prepare_workers) as executor:
                future_map = {
                    executor.submit(_prepare_upload_candidate, settings, candidate, ignored_keys): candidate
                    for candidate in candidates
                }
                for future in as_completed(future_map):
                    candidate = future_map[future]
                    try:
                        prepared = future.result()
                    except SessionParseError as exc:
                        skipped += 1
                        remember_invalid_session(
                            candidate.path,
                            candidate.file_size,
                            candidate.file_mtime_ns,
                            str(exc),
                        )
                        upsert_agent_file_state(
                            state_connection,
                            source_root=candidate.source_root,
                            source_path=candidate.path,
                            file_size=candidate.file_size,
                            file_mtime_ns=candidate.file_mtime_ns,
                            last_seen_at=utc_now_iso(),
                            state="invalid",
                            invalid_reason=str(exc),
                        )
                        logger.warning("Skipping malformed session file %s", exc)
                        continue
                    except Exception as exc:
                        failed += 1
                        last_failed_source_path = str(candidate.path)
                        last_failure_detail = exception_summary(exc)
                        logger.exception("Failed to prepare session from %s", candidate.path)
                        upsert_agent_file_state(
                            state_connection,
                            source_root=candidate.source_root,
                            source_path=candidate.path,
                            file_size=candidate.file_size,
                            file_mtime_ns=candidate.file_mtime_ns,
                            last_seen_at=utc_now_iso(),
                            state="failed",
                        )
                        continue

                    if isinstance(prepared, PreparedSkip):
                        skipped += 1
                        upsert_agent_file_state(
                            state_connection,
                            source_root=prepared.source_root,
                            source_path=prepared.path,
                            file_size=prepared.file_size,
                            file_mtime_ns=prepared.file_mtime_ns,
                            last_seen_at=utc_now_iso(),
                            state="ignored",
                            session_format=prepared.session_format,
                            session_id=prepared.session_id,
                            inferred_project_key=prepared.inferred_project_key,
                            inferred_project_label=prepared.inferred_project_label,
                        )
                        logger.info(
                            "Skipped ignored session %s from %s (%s)",
                            prepared.session_id or prepared.path.stem,
                            prepared.path,
                            prepared.reason,
                        )
                        continue

                    prepared_uploads.append(prepared)
                    upsert_agent_file_state(
                        state_connection,
                        source_root=prepared.source_root,
                        source_path=prepared.path,
                        file_size=prepared.file_size,
                        file_mtime_ns=prepared.file_mtime_ns,
                        last_seen_at=utc_now_iso(),
                        state="prepared",
                        session_format=prepared.session_format,
                        session_id=prepared.session_id,
                        inferred_project_key=prepared.inferred_project_key,
                        inferred_project_label=prepared.inferred_project_label,
                    )

        if prepared_uploads:
            prepared_uploads.sort(key=lambda item: str(item.path))
            prepared_by_path = {str(item.path): item for item in prepared_uploads}
            chunk_size = max(1, settings.remote_batch_size)
            chunks = [
                prepared_uploads[index : index + chunk_size]
                for index in range(0, len(prepared_uploads), chunk_size)
            ]
            with ThreadPoolExecutor(max_workers=settings.remote_upload_workers) as executor:
                future_map = {
                    executor.submit(
                        _upload_prepared_chunk,
                        settings,
                        chunk,
                        batch_uploads_enabled=batch_uploads_enabled,
                    ): chunk
                    for chunk in chunks
                }
                for future in as_completed(future_map):
                    chunk = future_map[future]
                    try:
                        outcomes = future.result()
                    except Exception as exc:
                        summary = exception_summary(exc)
                        for item in chunk:
                            failed += 1
                            last_failed_source_path = str(item.path)
                            last_failure_detail = summary
                            logger.exception("Failed to upload session from %s", item.path)
                        continue

                    for outcome in outcomes:
                        prepared_item = prepared_by_path.get(str(outcome.path))
                        if outcome.status == "ignored":
                            skipped += 1
                            logger.info(
                                "Skipped ignored session %s from %s (%s)",
                                outcome.session_id,
                                outcome.path,
                                outcome.reason,
                            )
                            upsert_agent_file_state(
                                state_connection,
                                source_root=(
                                    prepared_item.source_root
                                    if prepared_item is not None
                                    else _match_source_root(outcome.path, settings.session_roots) or outcome.path.parent
                                ),
                                source_path=outcome.path,
                                file_size=prepared_item.file_size if prepared_item is not None else 0,
                                file_mtime_ns=prepared_item.file_mtime_ns if prepared_item is not None else 0,
                                last_seen_at=utc_now_iso(),
                                state="ignored",
                            )
                            continue
                        if outcome.status == "ok":
                            uploaded += 1
                            logger.info(
                                "Uploaded session %s from %s (%s)",
                                outcome.session_id,
                                outcome.path,
                                outcome.reason,
                            )
                            mark_agent_file_uploaded(
                                state_connection,
                                source_path=outcome.path,
                                uploaded_at=utc_now_iso(),
                                file_size=prepared_item.file_size if prepared_item is not None else None,
                                content_sha256=outcome.content_sha256,
                                last_line_hash=prepared_item.last_line_hash if prepared_item is not None else None,
                                event_count=outcome.event_count,
                            )
                            continue

                        failed += 1
                        last_failed_source_path = str(outcome.path)
                        last_failure_detail = outcome.error
                        logger.error(
                            "Failed to upload session from %s: %s",
                            outcome.path,
                            outcome.error or "unknown error",
                        )

    stats = {"uploaded": uploaded, "skipped": skipped, "failed": failed}
    sync_completed_at = utc_now_iso()
    send_remote_heartbeat(
        settings,
        update_state=str(compatibility["state"]),
        update_message=str(compatibility["message"]),
        server_meta=server_meta,
        stats=stats,
        last_error="Upload failures occurred" if failed else None,
        last_failed_source_path=last_failed_source_path if failed else None,
        last_failure_detail=last_failure_detail if failed else None,
        last_sync_at=sync_completed_at,
        acknowledged_raw_resend_token=raw_resend_token if raw_resend_token and failed == 0 else None,
        last_raw_resend_at=sync_completed_at if raw_resend_token and failed == 0 else None,
    )
    return stats
