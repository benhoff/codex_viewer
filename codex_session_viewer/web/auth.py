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
from ..local_auth import fetch_auth_status, fetch_user_by_id
from .context import get_settings, request_return_to


PUBLIC_PATHS = {
    "/api/health",
    "/login",
    "/logout",
}
PUBLIC_PREFIXES = (
    "/static/",
    "/api/sync/",
)
BOOTSTRAP_PUBLIC_PATHS = {
    "/api/health",
    "/setup",
}


def request_bearer_token(request: Request) -> str | None:
    authorization = request.headers.get("authorization", "").strip()
    if not authorization.lower().startswith("bearer "):
        return None
    token = authorization[7:].strip()
    return token or None


def require_sync_api_auth(request: Request) -> None:
    bearer_token = request_bearer_token(request)
    if not bearer_token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    settings = get_settings(request)
    source_host = request.headers.get("x-codex-viewer-host", "").strip() or None
    with connect(settings.database_path) as connection:
        token_row = find_active_api_token(connection, bearer_token)
        if token_row is not None:
            with write_transaction(connection):
                touch_api_token_usage(connection, token_row["id"], source_host)
            return
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
    is_admin: bool = False,
) -> dict[str, object]:
    clean_username = username.strip()
    return {
        "user_id": (user_id or "").strip() or None,
        "username": clean_username,
        "display_name": (display_name or clean_username).strip() or clean_username,
        "email": (email or "").strip(),
        "auth_source": auth_source,
        "is_admin": bool(is_admin),
    }


def proxy_auth_user(request: Request, settings: Settings) -> dict[str, object] | None:
    if not settings.auth_allows_proxy():
        return None
    username = request.headers.get(settings.auth_proxy_user_header or "", "").strip()
    if not username:
        return None
    display_name = request.headers.get(settings.auth_proxy_name_header or "", "").strip() or username
    email = request.headers.get(settings.auth_proxy_email_header or "", "").strip()
    return build_auth_user(
        username=username,
        display_name=display_name,
        email=email,
        auth_source="sso",
    )


def session_auth_user(request: Request, settings: Settings) -> dict[str, object] | None:
    payload = request.session.get("auth_user")
    if not isinstance(payload, dict):
        return None
    user_id = str(payload.get("user_id") or "").strip()
    if not user_id:
        return None
    with connect(settings.database_path) as connection:
        user = fetch_user_by_id(connection, user_id)
    if user is None:
        clear_auth_session(request)
        return None
    return build_auth_user(
        user_id=user["id"],
        username=str(user["username"] or "").strip(),
        display_name=str(user["username"] or "").strip(),
        auth_source="password",
        is_admin=bool(user["is_admin"]),
    )


def set_password_session(request: Request, user: dict[str, object]) -> None:
    request.session["auth_user"] = {
        "user_id": user.get("user_id"),
        "username": user["username"],
        "display_name": user["display_name"],
        "email": user.get("email", ""),
        "auth_source": user["auth_source"],
        "is_admin": bool(user.get("is_admin")),
    }


def clear_auth_session(request: Request) -> None:
    request.session.clear()


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

        with connect(settings.database_path) as connection:
            auth_status = fetch_auth_status(connection)

        request.state.bootstrap_required = auth_status.bootstrap_required
        request.state.bootstrap_completed_at = auth_status.bootstrap_completed_at
        request.state.local_admin = auth_status.local_admin

        if auth_status.bootstrap_required:
            if is_bootstrap_public_path(request.url.path):
                return await call_next(request)
            if wants_html_response(request):
                return RedirectResponse(url="/setup", status_code=303)
            return JSONResponse({"detail": "Initial setup required"}, status_code=403)

        user = proxy_auth_user(request, settings) or session_auth_user(request, settings)
        request.state.auth_user = user

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
