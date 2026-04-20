from __future__ import annotations

import json
import logging
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .config import Settings
from .importer import (
    iter_session_files,
    parse_session_text,
    SessionParseError,
)
from .session_artifacts import read_session_source_text

INVALID_SESSION_CACHE: dict[tuple[str, int, int], str] = {}


class RemoteSyncError(RuntimeError):
    pass


class RestartRequired(RuntimeError):
    pass


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def exception_summary(exc: BaseException, *, max_length: int = 500) -> str:
    raw = " ".join(part for part in [exc.__class__.__name__, str(exc).strip()] if part).strip()
    summary = " ".join(raw.split())
    if len(summary) > max_length:
        return summary[: max_length - 1].rstrip() + "…"
    return summary


def build_headers(settings: Settings) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "X-Codex-Viewer-Agent-Version": settings.app_version,
        "X-Codex-Viewer-Api-Version": settings.sync_api_version,
        "X-Codex-Viewer-Sync-Mode": settings.sync_mode,
        "X-Codex-Viewer-Host": settings.source_host,
    }
    if settings.sync_api_token:
        headers["Authorization"] = f"Bearer {settings.sync_api_token}"
    return headers


def require_server_url(settings: Settings) -> str:
    if settings.server_base_url:
        return settings.server_base_url
    raise RemoteSyncError("CODEX_VIEWER_SERVER_URL must be configured for remote sync mode")


def json_request(
    settings: Settings,
    method: str,
    path: str,
    payload: dict[str, object] | None = None,
) -> dict[str, Any]:
    base_url = require_server_url(settings)
    body = None
    headers = build_headers(settings)
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
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


def invalid_session_cache_key(path: Path, file_size: int, file_mtime_ns: int) -> tuple[str, int, int]:
    return (str(path), file_size, file_mtime_ns)


def remember_invalid_session(path: Path, file_size: int, file_mtime_ns: int, message: str) -> None:
    path_str = str(path)
    stale_keys = [key for key in INVALID_SESSION_CACHE if key[0] == path_str and key[1:] != (file_size, file_mtime_ns)]
    for key in stale_keys:
        INVALID_SESSION_CACHE.pop(key, None)
    INVALID_SESSION_CACHE[invalid_session_cache_key(path, file_size, file_mtime_ns)] = message


def sync_sessions_remote(settings: Settings, force: bool = False) -> dict[str, int]:
    logger = logging.getLogger("codex_session_viewer.remote_sync")
    manifest, ignored_keys, server_meta, actions = ({}, set(), {}, {}) if force else fetch_remote_manifest(settings)
    uploaded = 0
    skipped = 0
    failed = 0
    last_failed_source_path: str | None = None
    last_failure_detail: str | None = None
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

    for source_root, path in iter_session_files(settings.session_roots):
        stat = path.stat()
        cached_parse_error = INVALID_SESSION_CACHE.get(
            invalid_session_cache_key(path, stat.st_size, stat.st_mtime_ns)
        )
        if cached_parse_error is not None:
            skipped += 1
            continue
        try:
            if raw_resend_token:
                raw_jsonl = read_session_source_text(path)
                parsed = parse_session_text(
                    raw_jsonl,
                    path,
                    source_root,
                    settings.source_host,
                    file_size=stat.st_size,
                    file_mtime_ns=stat.st_mtime_ns,
                )
                if parsed.inferred_project_key in ignored_keys:
                    skipped += 1
                    continue
                response = upload_raw_session(
                    settings,
                    build_raw_upload_payload(
                        settings,
                        source_root=source_root,
                        path=path,
                        raw_jsonl=raw_jsonl,
                        file_size=stat.st_size,
                        file_mtime_ns=stat.st_mtime_ns,
                    ),
                )
                reason = f"raw-resend:{raw_resend_token}"
            else:
                needs_upload, reason = remote_entry_needs_upload(
                    manifest.get(str(path)),
                    source_path=path,
                    file_size=stat.st_size,
                    file_mtime_ns=stat.st_mtime_ns,
                    force=force,
                )
                if not needs_upload:
                    skipped += 1
                    continue

                raw_jsonl = read_session_source_text(path)
                parsed = parse_session_text(
                    raw_jsonl,
                    path,
                    source_root,
                    settings.source_host,
                    file_size=stat.st_size,
                    file_mtime_ns=stat.st_mtime_ns,
                )
                if parsed.inferred_project_key in ignored_keys:
                    skipped += 1
                    continue
                response = upload_raw_session(
                    settings,
                    build_raw_upload_payload(
                        settings,
                        source_root=source_root,
                        path=path,
                        raw_jsonl=raw_jsonl,
                        file_size=stat.st_size,
                        file_mtime_ns=stat.st_mtime_ns,
                    ),
                )
            if response.get("status") == "ignored":
                skipped += 1
                continue
            uploaded += 1
            logger.info("Uploaded session %s from %s (%s)", parsed.session_id, path, reason)
        except SessionParseError as exc:
            skipped += 1
            remember_invalid_session(path, stat.st_size, stat.st_mtime_ns, str(exc))
            logger.warning("Skipping malformed session file %s", exc)
        except Exception as exc:
            failed += 1
            last_failed_source_path = str(path)
            last_failure_detail = exception_summary(exc)
            logger.exception("Failed to upload session from %s", path)

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
