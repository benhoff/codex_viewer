from __future__ import annotations

from fastapi import HTTPException, Request

from ..api_tokens import find_active_api_token, touch_api_token_usage
from ..db import connect, write_transaction
from .context import get_settings


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
