from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from ...db import connect
from ...projects import (
    effective_project_fields,
    fetch_session_with_project,
    project_edit_href,
    resolve_project_detail_href,
)
from ...runtime import export_markdown, get_events
from ...session_status import terminal_turn_summary
from ...session_view import build_turns
from ..context import get_app_context


router = APIRouter()


@router.get("/sessions/{session_id}", response_class=HTMLResponse)
def session_detail(request: Request, session_id: str) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        events = get_events(connection, session_id)
        group_href = resolve_project_detail_href(connection, effective_project_fields(session)["effective_group_key"])
    project = effective_project_fields(session)
    turns = list(reversed(build_turns(events)))
    session_display_summary = terminal_turn_summary(events) or str(session["summary"])

    return context.templates.TemplateResponse(
        request,
        name="session.html",
        context={
            "request": request,
            "session": session,
            "session_display_summary": session_display_summary,
            "project": project,
            "group_href": group_href,
            "edit_href": project_edit_href(group_href),
            "turns": turns,
            "source_roots": [str(path) for path in context.settings.session_roots],
        },
    )


@router.get("/sessions/{session_id}/export/raw")
def export_raw(request: Request, session_id: str) -> PlainTextResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
    source_path = Path(session["source_path"])
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="Source rollout file is no longer available")
    content = source_path.read_text(encoding="utf-8")
    headers = {"Content-Disposition": f'attachment; filename="{session_id}.jsonl"'}
    return PlainTextResponse(content=content, media_type="application/x-ndjson", headers=headers)


@router.get("/sessions/{session_id}/export/json")
def export_json(request: Request, session_id: str) -> JSONResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        events = get_events(connection, session_id)
    payload = {
        "session": dict(session),
        "events": [dict(event) for event in events],
    }
    headers = {"Content-Disposition": f'attachment; filename="{session_id}.json"'}
    return JSONResponse(content=payload, headers=headers)


@router.get("/sessions/{session_id}/export/markdown")
def export_session_markdown(request: Request, session_id: str) -> PlainTextResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        events = get_events(connection, session_id)
    headers = {"Content-Disposition": f'attachment; filename="{session_id}.md"'}
    return PlainTextResponse(export_markdown(session, events), media_type="text/markdown", headers=headers)
