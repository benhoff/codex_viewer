from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from ...db import connect
from ...projects import (
    effective_project_fields,
    fetch_session_with_project,
    project_edit_href,
)
from ...runtime import export_markdown, get_events
from ...saved_turns import fetch_session_saved_turn_states, owner_scope_from_request
from ...session_status import terminal_turn_summary
from ...session_view import build_turns
from ..context import get_app_context


router = APIRouter()


def get_session_view_events(connection, session_id: str):
    return connection.execute(
        """
        SELECT
            event_index,
            timestamp,
            record_type,
            payload_type,
            kind,
            role,
            title,
            display_text,
            detail_text,
            tool_name,
            call_id,
            command_text,
            exit_code
        FROM events
        WHERE session_id = ?
        ORDER BY event_index ASC
        """,
        (session_id,),
    ).fetchall()


@router.get("/sessions/{session_id}", response_class=HTMLResponse)
def session_detail(request: Request, session_id: str) -> HTMLResponse:
    context = get_app_context(request)
    owner_scope = owner_scope_from_request(request)
    with connect(context.settings.database_path) as connection:
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        events = get_session_view_events(connection, session_id)
        saved_turn_states = fetch_session_saved_turn_states(connection, owner_scope, session_id)
    project = effective_project_fields(session)
    group_key = str(project["effective_group_key"])
    group_href = f"/projects/key/{quote(group_key, safe='')}"
    turns_chrono = build_turns(events)
    for index, turn in enumerate(turns_chrono):
        previous_turn = turns_chrono[index - 1] if index > 0 else None
        turn["previous_response_text"] = (
            str(previous_turn.get("response_text") or "").strip()
            if previous_turn is not None
            else ""
        )
        turn["previous_response_state"] = (
            str(previous_turn.get("response_state") or "").strip()
            if previous_turn is not None
            else ""
        )
        turn["previous_turn_number"] = (
            int(previous_turn.get("number") or 0)
            if previous_turn is not None
            else None
        )
    turns = list(reversed(turns_chrono))
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
            "edit_href": f"/projects/edit?key={quote(group_key, safe='')}",
            "turns": turns,
            "saved_turn_states": saved_turn_states,
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
