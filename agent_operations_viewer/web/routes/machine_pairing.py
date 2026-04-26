from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from ...db import connect, write_transaction
from ...machine_credentials import (
    approve_pairing_session,
    create_pairing_session,
    decline_pairing_session,
    ensure_pairing_session,
    fetch_pairing_session_for_secret,
    finalize_pairing_session,
    hash_pairing_secret,
    revoke_machine_credential,
)
from ..auth import require_admin_user, require_sync_api_auth
from ..context import get_app_context
from ..forms import parse_form_fields


router = APIRouter()


async def _read_json_payload(request: Request) -> object:
    try:
        raw_body = await request.body()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail="Unable to read request body") from exc
    try:
        return json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc


def _lookup_pairing_session(
    request: Request,
    *,
    session_id: str,
    secret: str,
):
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        row = fetch_pairing_session_for_secret(connection, session_id, secret)
    if row is None:
        raise HTTPException(status_code=404, detail="Pairing request not found")
    return row


def render_machine_pairing_page(
    request: Request,
    *,
    session_id: str,
    secret: str,
    error: str | None = None,
    success: str | None = None,
) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        pairing = fetch_pairing_session_for_secret(connection, session_id, secret)
    if pairing is None:
        raise HTTPException(status_code=404, detail="Pairing request not found")
    return context.templates.TemplateResponse(
        request,
        name="machine_pairing.html",
        context={
            "request": request,
            "search_query": "",
            "pairing": dict(pairing),
            "pairing_secret": secret,
            "error": error,
            "success": success,
        },
    )


@router.post("/api/machine-pairing/sessions")
async def create_machine_pairing_session(request: Request) -> JSONResponse:
    require_admin_user(request)
    if bool(getattr(request.state, "bootstrap_required", False)):
        raise HTTPException(status_code=403, detail="Initial setup required")
    context = get_app_context(request)
    payload = await _read_json_payload(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Pairing payload must be an object")
    public_key = str(payload.get("public_key") or "").strip()
    secret_hash = str(payload.get("secret_hash") or "").strip()
    source_host = str(payload.get("source_host") or "").strip()
    label = str(payload.get("label") or "").strip() or source_host or "Paired machine"
    if not public_key or not secret_hash:
        raise HTTPException(status_code=400, detail="Pairing payload is missing required fields")
    with connect(context.settings.database_path) as connection:
        with write_transaction(connection):
            pairing = create_pairing_session(
                connection,
                label=label,
                source_host=source_host,
                public_key=public_key,
                secret_hash=secret_hash,
            )
    return JSONResponse(pairing)


@router.get("/machine-pairing/start")
def machine_pairing_start(
    request: Request,
    *,
    session_id: str,
    secret: str,
    public_key: str,
    source_host: str = "",
    label: str = "",
) -> RedirectResponse:
    require_admin_user(request)
    context = get_app_context(request)
    clean_session_id = session_id.strip()
    clean_secret = secret.strip()
    clean_public_key = public_key.strip()
    clean_source_host = source_host.strip()
    clean_label = label.strip() or clean_source_host or "Paired machine"
    if not clean_session_id or not clean_secret or not clean_public_key:
        raise HTTPException(status_code=400, detail="Missing pairing start parameters")
    try:
        with connect(context.settings.database_path) as connection:
            with write_transaction(connection):
                ensure_pairing_session(
                    connection,
                    session_id=clean_session_id,
                    label=clean_label,
                    source_host=clean_source_host,
                    public_key=clean_public_key,
                    secret_hash=hash_pairing_secret(clean_secret),
                )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return RedirectResponse(
        url=f"/machine-pairing/{clean_session_id}?secret={clean_secret}",
        status_code=303,
    )


@router.get("/api/machine-pairing/sessions/{session_id}")
def machine_pairing_session_status(request: Request, session_id: str, secret: str) -> JSONResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        row = fetch_pairing_session_for_secret(connection, session_id, secret)
    if row is None:
        raise HTTPException(status_code=404, detail="Pairing request not found")
    payload = {
        "id": str(row["id"]),
        "label": str(row["label"] or ""),
        "source_host": str(row["source_host"] or ""),
        "status": str(row["status"] or "pending"),
        "expires_at": str(row["expires_at"] or ""),
        "completed_at": str(row["completed_at"] or "") or None,
        "used_at": str(row["used_at"] or "") or None,
        "machine_credential_id": str(row["machine_credential_id"] or "") or None,
        "approval_url": f"{context.settings.server_base_url or ''}/machine-pairing/{session_id}?secret={secret}",
    }
    return JSONResponse(payload)


@router.post("/api/machine-pairing/sessions/{session_id}/finalize")
async def machine_pairing_session_finalize(request: Request, session_id: str) -> JSONResponse:
    context = get_app_context(request)
    payload = await _read_json_payload(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Finalize payload must be an object")
    secret = str(payload.get("secret") or "").strip()
    if not secret:
        raise HTTPException(status_code=400, detail="Finalize payload is missing secret")
    with connect(context.settings.database_path) as connection:
        try:
            with write_transaction(connection):
                pairing = finalize_pairing_session(
                    connection,
                    session_id=session_id,
                    secret=secret,
                )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(pairing)


@router.post("/api/machine-auth/revoke")
async def revoke_machine_auth(request: Request) -> JSONResponse:
    auth_result = await require_sync_api_auth(request)
    if str(auth_result.get("auth_type") or "") != "machine_credential":
        raise HTTPException(status_code=403, detail="Machine credential required")
    context = get_app_context(request)
    machine_id = str(auth_result["machine_id"])
    with connect(context.settings.database_path) as connection:
        with write_transaction(connection):
            revoke_machine_credential(connection, machine_id)
    return JSONResponse({"status": "ok", "machine_id": machine_id})


@router.get("/machine-pairing/{session_id}", response_class=HTMLResponse)
def machine_pairing_page(request: Request, session_id: str, secret: str) -> Response:
    require_admin_user(request)
    return render_machine_pairing_page(request, session_id=session_id, secret=secret)


@router.post("/machine-pairing/{session_id}/approve")
async def machine_pairing_approve(request: Request, session_id: str) -> HTMLResponse:
    current_user = require_admin_user(request)
    context = get_app_context(request)
    fields = await parse_form_fields(request)
    secret = fields.get("secret", "").strip()
    if not secret:
        raise HTTPException(status_code=400, detail="Missing pairing secret")
    try:
        with connect(context.settings.database_path) as connection:
            with write_transaction(connection):
                approve_pairing_session(
                    connection,
                    session_id=session_id,
                    secret=secret,
                    approver_user_id=str(current_user.get("user_id") or "") or None,
                )
    except ValueError as exc:
        return render_machine_pairing_page(request, session_id=session_id, secret=secret, error=str(exc))
    return render_machine_pairing_page(
        request,
        session_id=session_id,
        secret=secret,
        success="Machine approved. You can return to the CLI to finish pairing.",
    )


@router.post("/machine-pairing/{session_id}/decline")
async def machine_pairing_decline(request: Request, session_id: str) -> HTMLResponse:
    require_admin_user(request)
    context = get_app_context(request)
    fields = await parse_form_fields(request)
    secret = fields.get("secret", "").strip()
    if not secret:
        raise HTTPException(status_code=400, detail="Missing pairing secret")
    try:
        with connect(context.settings.database_path) as connection:
            with write_transaction(connection):
                decline_pairing_session(
                    connection,
                    session_id=session_id,
                    secret=secret,
                )
    except ValueError as exc:
        return render_machine_pairing_page(request, session_id=session_id, secret=secret, error=str(exc))
    return render_machine_pairing_page(
        request,
        session_id=session_id,
        secret=secret,
        success="Machine pairing request declined.",
    )
