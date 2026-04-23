from __future__ import annotations

from agent_daemon.service_manager import (
    install_service,
    service_status,
    start_service,
    stop_service,
    uninstall_service,
)

import json
import secrets
import time
import webbrowser
from dataclasses import asdict
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .config import Settings
from .local_machine import (
    LocalMachineIdentity,
    delete_machine_identity,
    load_machine_identity,
    store_machine_identity,
)
from .machine_auth import build_machine_auth_headers, generate_machine_keypair
from .machine_credentials import generate_pairing_session_id, hash_pairing_secret


def _clean_url(raw: str | None) -> str | None:
    if raw is None:
        return None
    stripped = raw.strip().rstrip("/")
    return stripped or None


def require_server_base_url(settings: Settings) -> str:
    server_base_url = _clean_url(settings.server_base_url)
    if not server_base_url:
        raise SystemExit("CODEX_VIEWER_SERVER_URL must be configured before pairing a machine.")
    return server_base_url


def _json_request(
    *,
    base_url: str,
    method: str,
    path: str,
    payload: dict[str, object] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 15,
) -> dict[str, Any]:
    request_headers = {
        "Accept": "application/json",
        **(headers or {}),
    }
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = Request(
        f"{base_url}{path}",
        data=body,
        method=method.upper(),
        headers=request_headers,
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            raw_text = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{exc.code} {detail}") from exc
    except URLError as exc:
        raise RuntimeError(str(exc.reason)) from exc
    if not raw_text:
        return {}
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Server returned invalid JSON: {raw_text}") from exc


def _build_pairing_approval_url(base_url: str, session_id: str, secret: str) -> str:
    return f"{base_url}/machine-pairing/{quote(session_id, safe='')}?secret={quote(secret, safe='')}"


def _build_pairing_start_url(
    *,
    base_url: str,
    session_id: str,
    secret: str,
    public_key: str,
    source_host: str,
    label: str,
) -> str:
    return (
        f"{base_url}/machine-pairing/start?"
        f"session_id={quote(session_id, safe='')}"
        f"&secret={quote(secret, safe='')}"
        f"&public_key={quote(public_key, safe='')}"
        f"&source_host={quote(source_host, safe='')}"
        f"&label={quote(label, safe='')}"
    )


def _signed_headers_for_identity(identity: LocalMachineIdentity, method: str, path: str) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "X-Codex-Viewer-Host": identity.source_host,
    }
    headers.update(
        build_machine_auth_headers(
            private_key=identity.private_key,
            machine_id=identity.machine_id,
            method=method,
            path=path,
            raw_body=None,
            source_host=identity.source_host,
        )
    )
    return headers


def pair_machine(
    settings: Settings,
    *,
    label: str | None = None,
    open_browser: bool = True,
    timeout_seconds: int = 900,
    poll_interval_seconds: float = 2.0,
    force: bool = False,
) -> dict[str, object]:
    server_base_url = require_server_base_url(settings)
    existing_identity = load_machine_identity(settings)
    if existing_identity is not None and not force:
        raise SystemExit(
            "This machine is already paired. Run `python -m codex_session_viewer machine unpair` "
            "or `python -m codex_session_viewer machine repair --re-pair` first."
        )

    keypair = generate_machine_keypair()
    pairing_secret = secrets.token_urlsafe(24)
    session_id = generate_pairing_session_id()
    approval_url = _build_pairing_start_url(
        base_url=server_base_url,
        session_id=session_id,
        secret=pairing_secret,
        public_key=keypair.public_key,
        source_host=settings.source_host,
        label=label or settings.source_host,
    )
    if open_browser:
        try:
            webbrowser.open(approval_url, new=2)
        except Exception:
            pass

    deadline = time.monotonic() + max(10, timeout_seconds)
    status_payload: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        try:
            status_payload = _json_request(
                base_url=server_base_url,
                method="GET",
                path=f"/api/machine-pairing/sessions/{quote(session_id, safe='')}?secret={quote(pairing_secret, safe='')}",
                timeout=settings.remote_timeout_seconds,
            )
        except RuntimeError as exc:
            if str(exc).startswith("404"):
                time.sleep(max(0.5, poll_interval_seconds))
                continue
            raise SystemExit(f"Unable to poll the pairing session: {exc}") from exc
        status = str(status_payload.get("status") or "pending")
        if status == "approved":
            break
        if status in {"declined", "expired"}:
            raise SystemExit(f"Pairing request {status}. Open this URL to inspect it: {approval_url}")
        time.sleep(max(0.5, poll_interval_seconds))
    else:
        raise SystemExit(f"Pairing timed out. Complete approval in the browser: {approval_url}")

    try:
        finalized = _json_request(
            base_url=server_base_url,
            method="POST",
            path=f"/api/machine-pairing/sessions/{quote(session_id, safe='')}/finalize",
            payload={"secret": pairing_secret},
            timeout=settings.remote_timeout_seconds,
        )
    except RuntimeError as exc:
        raise SystemExit(f"Unable to finalize pairing: {exc}") from exc
    machine = finalized.get("machine")
    if not isinstance(machine, dict):
        raise SystemExit("Server did not return finalized machine credential details.")

    identity = LocalMachineIdentity(
        machine_id=str(machine["id"]),
        label=str(machine["label"]),
        source_host=settings.source_host,
        server_base_url=server_base_url,
        public_key=keypair.public_key,
        private_key=keypair.private_key,
        paired_at=str(machine["created_at"]),
        created_by_user_id=str(machine.get("created_by_user_id") or "") or None,
    )
    store_machine_identity(settings, identity)
    return {
        "paired": True,
        "approval_url": approval_url,
        "identity_path": str(settings.data_dir / "machine-identity.json"),
        "machine": machine,
    }


def _machine_auth_probe(settings: Settings, identity: LocalMachineIdentity) -> dict[str, object]:
    server_base_url = _clean_url(identity.server_base_url) or require_server_base_url(settings)
    path = f"/api/sync/manifest?host={quote(identity.source_host, safe='')}"
    try:
        payload = _json_request(
            base_url=server_base_url,
            method="GET",
            path=path,
            headers=_signed_headers_for_identity(identity, "GET", path),
            timeout=settings.remote_timeout_seconds,
        )
        return {
            "ok": True,
            "manifest_session_count": len(payload.get("sessions", []))
            if isinstance(payload.get("sessions"), list)
            else None,
        }
    except RuntimeError as exc:
        return {
            "ok": False,
            "error": str(exc),
        }


def machine_status(settings: Settings) -> dict[str, object]:
    identity = load_machine_identity(settings)
    server_base_url = _clean_url(settings.server_base_url) or (
        identity.server_base_url if identity is not None else None
    )
    health: dict[str, object] | None = None
    if server_base_url:
        try:
            health = _json_request(
                base_url=server_base_url,
                method="GET",
                path="/api/health",
                timeout=settings.remote_timeout_seconds,
            )
        except RuntimeError as exc:
            health = {"ok": False, "error": str(exc)}
    auth_probe = _machine_auth_probe(settings, identity) if identity is not None else None
    return {
        "paired": identity is not None,
        "identity": asdict(identity) if identity is not None else None,
        "server_health": health,
        "auth_probe": auth_probe,
        "service": service_status(),
    }


def machine_unpair(
    settings: Settings,
    *,
    uninstall: bool = False,
) -> dict[str, object]:
    identity = load_machine_identity(settings)
    revoke_result: dict[str, object] | None = None
    if identity is not None:
        try:
            revoke_result = _json_request(
                base_url=identity.server_base_url,
                method="POST",
                path="/api/machine-auth/revoke",
                headers=_signed_headers_for_identity(identity, "POST", "/api/machine-auth/revoke"),
                timeout=settings.remote_timeout_seconds,
            )
        except RuntimeError as exc:
            revoke_result = {"ok": False, "error": str(exc)}
    deleted_identity = delete_machine_identity(settings)
    service_result = None
    if uninstall:
        try:
            stop_service()
        except Exception:
            pass
        service_result = uninstall_service()
    return {
        "identity_deleted": deleted_identity,
        "server_revoke": revoke_result,
        "service": service_result,
    }


def machine_repair(
    settings: Settings,
    *,
    re_pair: bool = False,
    reinstall_service: bool = False,
    open_browser: bool = True,
) -> dict[str, object]:
    actions: list[str] = []
    identity = load_machine_identity(settings)
    if identity is None and re_pair:
        pair_machine(settings, open_browser=open_browser, force=True)
        identity = load_machine_identity(settings)
        actions.append("paired_machine")
    elif identity is None:
        raise SystemExit("No machine credential is configured. Run `python -m codex_session_viewer pair`.")

    if reinstall_service:
        uninstall_service()
        actions.append("uninstalled_service")
    current_service = service_status()
    if reinstall_service or not bool(current_service.get("installed")):
        install_service(settings)
        actions.append("installed_service")
        current_service = service_status()
    if not bool(current_service.get("running")):
        start_service(settings)
        actions.append("started_service")

    return {
        "actions": actions,
        "status": machine_status(settings),
    }


def machine_setup(
    settings: Settings,
    *,
    label: str | None = None,
    open_browser: bool = True,
) -> dict[str, object]:
    pair_result = pair_machine(settings, label=label, open_browser=open_browser, force=False)
    install_result = install_service(settings)
    start_result = start_service(settings)
    return {
        "pair": pair_result,
        "service_install": install_result,
        "service_start": start_result,
        "status": machine_status(settings),
    }
