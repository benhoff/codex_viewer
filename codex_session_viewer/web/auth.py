from __future__ import annotations

from typing import Any
from urllib.parse import quote

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware

from ..api_tokens import find_active_api_token, touch_api_token_usage
from ..config import Settings
from ..db import connect, write_transaction
from ..machine_auth import (
    MACHINE_BODY_SHA256_HEADER,
    MACHINE_ID_HEADER,
    MACHINE_NONCE_HEADER,
    MACHINE_SIGNATURE_HEADER,
    MACHINE_TIMESTAMP_HEADER,
    timestamp_is_fresh,
    verify_machine_request_signature,
)
from ..machine_credentials import (
    fetch_active_machine_credential,
    record_machine_auth_nonce,
    touch_machine_credential_usage,
)
from ..local_auth import fetch_auth_status, fetch_user_by_id, touch_user_seen, upsert_proxy_user
from ..onboarding import effective_bootstrap_required
from .context import get_settings, request_return_to


PUBLIC_PATHS = {
    "/api/health",
    "/login",
    "/logout",
}
PUBLIC_PREFIXES = (
    "/static/",
    "/api/sync/",
    "/api/machine-auth/",
    "/api/machine-pairing/",
)
BOOTSTRAP_PUBLIC_PATHS = {
    "/api/health",
    "/setup",
    "/setup/claim-admin",
    "/setup/status",
}


def request_bearer_token(request: Request) -> str | None:
    authorization = request.headers.get("authorization", "").strip()
    if not authorization.lower().startswith("bearer "):
        return None
    token = authorization[7:].strip()
    return token or None


async def require_sync_api_auth(request: Request) -> dict[str, object]:
    bearer_token = request_bearer_token(request)
    settings = get_settings(request)
    source_host = request.headers.get("x-codex-viewer-host", "").strip() or None
    raw_body = await request.body()
    path = request.url.path
    if request.url.query:
        path = f"{path}?{request.url.query}"
    machine_id = request.headers.get(MACHINE_ID_HEADER, "").strip()
    machine_timestamp = request.headers.get(MACHINE_TIMESTAMP_HEADER, "").strip()
    machine_nonce = request.headers.get(MACHINE_NONCE_HEADER, "").strip()
    machine_signature = request.headers.get(MACHINE_SIGNATURE_HEADER, "").strip()
    machine_body_sha256 = request.headers.get(MACHINE_BODY_SHA256_HEADER, "").strip()
    with connect(settings.database_path) as connection:
        if bearer_token:
            token_row = find_active_api_token(connection, bearer_token)
            if token_row is not None:
                with write_transaction(connection):
                    touch_api_token_usage(connection, token_row["id"], source_host)
                return {
                    "auth_type": "api_token",
                    "token_id": str(token_row["id"]),
                }
        if machine_id and machine_timestamp and machine_nonce and machine_signature and machine_body_sha256:
            machine_row = fetch_active_machine_credential(connection, machine_id)
            if (
                machine_row is not None
                and timestamp_is_fresh(machine_timestamp)
                and verify_machine_request_signature(
                    public_key=str(machine_row["public_key"]),
                    machine_id=machine_id,
                    method=request.method,
                    path=path,
                    raw_body=raw_body,
                    source_host=source_host,
                    timestamp=machine_timestamp,
                    nonce=machine_nonce,
                    signature=machine_signature,
                    body_sha256=machine_body_sha256,
                )
            ):
                with write_transaction(connection):
                    if not record_machine_auth_nonce(
                        connection,
                        machine_id=machine_id,
                        nonce=machine_nonce,
                        created_at=machine_timestamp,
                    ):
                        raise HTTPException(status_code=401, detail="Unauthorized")
                    touch_machine_credential_usage(connection, machine_id)
                return {
                    "auth_type": "machine_credential",
                    "machine_id": machine_id,
                }
    raise HTTPException(status_code=401, detail="Unauthorized")


def validate_auth_settings(settings: Settings) -> None:
    if not settings.auth_enabled():
        return
    if not settings.session_secret:
        raise ValueError("Browser auth could not initialize a session secret")
    if settings.auth_mode not in {"password", "proxy", "password_or_proxy"}:
        raise ValueError(
            "CODEX_VIEWER_AUTH_MODE must be one of none, password, proxy, or password_or_proxy"
        )


def safe_next_path(value: str | None) -> str:
    candidate = (value or "").strip()
    if not candidate.startswith("/") or candidate.startswith("//"):
        return "/"
    return candidate


def is_public_path(path: str) -> bool:
    if path in PUBLIC_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in PUBLIC_PREFIXES)


def is_bootstrap_public_path(path: str) -> bool:
    if path in BOOTSTRAP_PUBLIC_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in PUBLIC_PREFIXES)


def wants_html_response(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept or "*/*" in accept or not accept


def build_auth_user(
    *,
    username: str,
    display_name: str | None = None,
    email: str | None = None,
    auth_source: str,
    user_id: str | None = None,
    role: str = "viewer",
    is_admin: bool = False,
) -> dict[str, object]:
    clean_username = username.strip()
    normalized_role = (role or "").strip().lower() or ("admin" if is_admin else "viewer")
    return {
        "user_id": (user_id or "").strip() or None,
        "username": clean_username,
        "display_name": (display_name or clean_username).strip() or clean_username,
        "email": (email or "").strip(),
        "auth_source": auth_source,
        "role": normalized_role,
        "is_admin": normalized_role == "admin" or bool(is_admin),
    }


def proxy_auth_user(
    request: Request,
    settings: Settings,
    connection: Any,
) -> dict[str, object] | None:
    if not settings.auth_allows_proxy():
        return None
    username = request.headers.get(settings.auth_proxy_user_header or "", "").strip()
    if not username:
        return None
    display_name = request.headers.get(settings.auth_proxy_name_header or "", "").strip() or username
    email = request.headers.get(settings.auth_proxy_email_header or "", "").strip()
    user = upsert_proxy_user(
        connection,
        external_subject=username,
        username=username,
        display_name=display_name,
        email=email,
    )
    if user.get("disabled_at"):
        raise ValueError("This account has been disabled.")
    return build_auth_user(
        user_id=str(user["id"]),
        username=str(user["username"]),
        display_name=str(user["display_name"]),
        email=str(user.get("email") or ""),
        auth_source="proxy",
        role=str(user["role"]),
        is_admin=bool(user["is_admin"]),
    )


def session_auth_user(request: Request, settings: Settings, connection: Any) -> dict[str, object] | None:
    payload = request.session.get("auth_user")
    if not isinstance(payload, dict):
        return None
    user_id = str(payload.get("user_id") or "").strip()
    if not user_id:
        return None
    user = fetch_user_by_id(connection, user_id)
    if user is None:
        clear_auth_session(request)
        return None
    if str(user["disabled_at"] or "").strip():
        clear_auth_session(request)
        return None
    touch_user_seen(connection, user_id)
    return build_auth_user(
        user_id=user["id"],
        username=str(user["username"] or "").strip(),
        display_name=str(user["display_name"] or user["username"] or "").strip(),
        email=str(user["email"] or "").strip(),
        auth_source="password",
        role=str(user["role"] or "viewer"),
        is_admin=bool(user["is_admin"]),
    )


def set_password_session(request: Request, user: dict[str, object]) -> None:
    request.session["auth_user"] = {
        "user_id": user.get("user_id"),
        "username": user["username"],
        "display_name": user["display_name"],
        "email": user.get("email", ""),
        "auth_source": user["auth_source"],
        "role": user.get("role", "viewer"),
        "is_admin": bool(user.get("is_admin")),
    }


def clear_auth_session(request: Request) -> None:
    request.session.clear()


def require_authenticated_user(request: Request) -> dict[str, object]:
    user = getattr(request.state, "auth_user", None)
    if not isinstance(user, dict) or not str(user.get("user_id") or "").strip():
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def require_admin_user(request: Request) -> dict[str, object]:
    if not bool(getattr(request.state, "auth_enabled", False)):
        return build_auth_user(
            username="local-operator",
            display_name="Local operator",
            auth_source="none",
            role="admin",
            is_admin=True,
        )
    user = require_authenticated_user(request)
    if str(user.get("role") or "").strip().lower() != "admin" and not bool(user.get("is_admin")):
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: Any, *, settings: Settings) -> None:
        super().__init__(app)
        self.settings = settings

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        settings = self.settings
        request.state.auth_enabled = settings.auth_enabled()
        request.state.auth_mode = settings.auth_mode
        request.state.auth_user = None
        request.state.bootstrap_required = False
        request.state.bootstrap_completed_at = None
        request.state.local_admin = None

        if not settings.auth_enabled():
            return await call_next(request)

        try:
            with connect(settings.database_path) as connection:
                auth_status = fetch_auth_status(connection)
                user = proxy_auth_user(request, settings, connection) or session_auth_user(request, settings, connection)
        except ValueError as exc:
            if wants_html_response(request):
                return Response(content=str(exc), status_code=403)
            return JSONResponse({"detail": str(exc)}, status_code=403)

        request.state.bootstrap_required = effective_bootstrap_required(settings, auth_status)
        request.state.bootstrap_completed_at = auth_status.bootstrap_completed_at
        request.state.local_admin = auth_status.local_admin
        request.state.auth_user = user

        if request.state.bootstrap_required:
            if is_bootstrap_public_path(request.url.path):
                return await call_next(request)
            if wants_html_response(request):
                return RedirectResponse(url="/setup", status_code=303)
            return JSONResponse({"detail": "Initial setup required"}, status_code=403)

        if is_public_path(request.url.path):
            return await call_next(request)

        if user is not None:
            return await call_next(request)

        if wants_html_response(request):
            if settings.auth_allows_proxy() and not settings.auth_allows_password() and settings.auth_proxy_login_url:
                return RedirectResponse(url=settings.auth_proxy_login_url, status_code=303)
            next_path = safe_next_path(request_return_to(request))
            return RedirectResponse(url=f"/login?next={quote(next_path, safe='/?=&')}", status_code=303)

        return JSONResponse({"detail": "Unauthorized"}, status_code=401)


def install_auth(app: Any, settings: Settings) -> None:
    validate_auth_settings(settings)
    app.add_middleware(AuthMiddleware, settings=settings)
    if settings.auth_enabled():
        app.add_middleware(
            SessionMiddleware,
            secret_key=settings.session_secret or "disabled",
            session_cookie="codex_viewer_session",
            same_site="lax",
            https_only=settings.auth_cookie_secure,
            max_age=60 * 60 * 24 * 14,
        )
