from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
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
from ...session_view import build_session_audit_summary, build_turns
from ...turn_index import fetch_session_turn_window, turn_window_size
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
            exit_code,
            record_json
        FROM events
        WHERE session_id = ?
        ORDER BY event_index ASC
        """,
        (session_id,),
    ).fetchall()


def get_session_view_events_range(
    connection,
    session_id: str,
    start_event_index: int,
    end_event_index: int,
):
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
            exit_code,
            record_json
        FROM events
        WHERE session_id = ?
          AND event_index BETWEEN ? AND ?
        ORDER BY event_index ASC
        """,
        (session_id, start_event_index, end_event_index),
    ).fetchall()


@router.get("/sessions/{session_id}", response_class=HTMLResponse)
def session_detail(
    request: Request,
    session_id: str,
    view: str = Query(default="conversation"),
    turn: int | None = Query(default=None),
    before_turn: int | None = Query(default=None),
    focus: int | None = Query(default=None),
) -> HTMLResponse:
    context = get_app_context(request)
    owner_scope = owner_scope_from_request(request)
    session_view_mode = "audit" if str(view or "").strip().lower() == "audit" else "conversation"
    requested_turn = turn
    with connect(context.settings.database_path) as connection:
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        window = fetch_session_turn_window(
            connection,
            session_id,
            window_size=turn_window_size(session_view_mode),
            turn_number=requested_turn,
            before_turn=before_turn,
        )
        if window["event_start_index"] is not None and window["event_end_index"] is not None:
            events = get_session_view_events_range(
                connection,
                session_id,
                int(window["event_start_index"]),
                int(window["event_end_index"]),
            )
        else:
            events = get_session_view_events(connection, session_id)
        saved_turn_states = fetch_session_saved_turn_states(connection, owner_scope, session_id)
    project = effective_project_fields(session)
    group_key = str(project["effective_group_key"])
    group_href = f"/projects/key/{quote(group_key, safe='')}"
    starting_turn_number = int(window["context_turn"]["turn_number"]) if window.get("context_turn") else int(window.get("oldest_turn") or 1)
    turns_chrono = build_turns(
        events,
        cwd=str(session["cwd"] or "").strip() or None,
        starting_turn_number=starting_turn_number,
    )
    previous_context_turn = None
    if window.get("context_turn") and turns_chrono and int(turns_chrono[0].get("number") or 0) == int(window["context_turn"]["turn_number"]):
        previous_context_turn = turns_chrono[0]
        turns_chrono = turns_chrono[1:]
    for index, turn_data in enumerate(turns_chrono):
        previous_turn = turns_chrono[index - 1] if index > 0 else previous_context_turn
        turn_data["previous_response_text"] = (
            str(previous_turn.get("response_text") or "").strip()
            if previous_turn is not None
            else ""
        )
        turn_data["previous_response_state"] = (
            str(previous_turn.get("response_state") or "").strip()
            if previous_turn is not None
            else ""
        )
        turn_data["previous_turn_number"] = (
            int(previous_turn.get("number") or 0)
            if previous_turn is not None
            else None
        )
    turns = list(reversed(turns_chrono))
    session_display_summary = terminal_turn_summary(events) or str(session["summary"])
    audit_summary = build_session_audit_summary(session, turns_chrono)
    audit_focus_turn = int(requested_turn) if session_view_mode == "audit" and focus and requested_turn else None
    pagination = {
        "total_turns": int(window["total_turns"] or 0),
        "window_size": int(window["window_size"] or 0),
        "oldest_turn": window["oldest_turn"],
        "newest_turn": window["newest_turn"],
        "has_older": bool(window["has_older"]),
        "has_newer": bool(window["has_newer"]),
        "older_href": (
            f"{request.url.path}?view={quote(session_view_mode, safe='')}&before_turn={int(window['older_before_turn'])}"
            if window.get("older_before_turn")
            else None
        ),
        "newer_href": (
            f"{request.url.path}?view={quote(session_view_mode, safe='')}&turn={int(window['newer_turn'])}"
            if window.get("newer_turn")
            else None
        ),
        "latest_href": f"{request.url.path}?view={quote(session_view_mode, safe='')}",
    }
    audit_focus = None
    if audit_focus_turn:
        total_turns = int(window["total_turns"] or 0)
        audit_focus = {
            "turn_number": audit_focus_turn,
            "back_to_conversation_href": f"{request.url.path}?view=conversation&turn={audit_focus_turn}",
            "full_audit_href": f"{request.url.path}?view=audit&turn={audit_focus_turn}",
            "prev_href": (
                f"{request.url.path}?view=audit&turn={audit_focus_turn - 1}&focus=1"
                if audit_focus_turn > 1
                else None
            ),
            "next_href": (
                f"{request.url.path}?view=audit&turn={audit_focus_turn + 1}&focus=1"
                if total_turns and audit_focus_turn < total_turns
                else None
            ),
        }

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
            "session_view_mode": session_view_mode,
            "audit_summary": audit_summary,
            "audit_focus": audit_focus,
            "audit_focus_turn": audit_focus_turn,
            "turns": turns,
            "pagination": pagination,
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
