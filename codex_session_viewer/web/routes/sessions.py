from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response

from ...db import connect
from ...projects import (
    build_project_access_context,
    effective_project_fields,
    fetch_session_with_project,
    row_is_visible_to_project_access,
)
from ...runtime import export_markdown, get_events
from ...saved_turns import fetch_session_saved_turn_states, owner_scope_from_request
from ...session_artifacts import resolve_session_raw_text
from ...session_exports import build_session_bundle, export_json_payload
from ...session_insights import session_agent_snapshot, usage_pressure_snapshot
from ...session_status import terminal_turn_summary
from ...session_view import build_session_audit_summary, build_turn_review_focus, build_turns
from ...turn_index import fetch_session_turn_window, turn_window_size
from ..context import get_app_context


router = APIRouter()


def build_pagination_tokens(current_page: int, total_pages: int) -> list[int | None]:
    if total_pages <= 7:
        return list(range(1, total_pages + 1))

    pages = {1, total_pages, current_page - 1, current_page, current_page + 1}
    if current_page <= 3:
        pages.update({2, 3, 4})
    if current_page >= total_pages - 2:
        pages.update({total_pages - 3, total_pages - 2, total_pages - 1})

    valid_pages = sorted(page for page in pages if 1 <= page <= total_pages)
    tokens: list[int | None] = []
    previous = None
    for page in valid_pages:
        if previous is not None and page - previous > 1:
            tokens.append(None)
        tokens.append(page)
        previous = page
    return tokens


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
    page: int | None = Query(default=None),
    focus: int | None = Query(default=None),
    review: str | None = Query(default=None),
) -> HTMLResponse:
    context = get_app_context(request)
    owner_scope = owner_scope_from_request(request)
    session_view_mode = "audit" if str(view or "").strip().lower() == "audit" else "conversation"
    requested_turn = turn
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        if not row_is_visible_to_project_access(session, project_access):
            raise HTTPException(status_code=404, detail="Session not found")
        agent_snapshot = session_agent_snapshot(session)
        usage_pressure = usage_pressure_snapshot(session)
        parent_session_href = None
        parent_session_title = None
        if agent_snapshot["forked_from_id"]:
            parent_session = fetch_session_with_project(connection, str(agent_snapshot["forked_from_id"]))
            if parent_session is not None and row_is_visible_to_project_access(parent_session, project_access):
                parent_session_href = f"/sessions/{quote(str(parent_session['id']), safe='')}"
                parent_session_title = str(
                    parent_session["last_user_message"]
                    or parent_session["latest_turn_summary"]
                    or parent_session["summary"]
                    or parent_session["id"]
                )
        window = fetch_session_turn_window(
            connection,
            session_id,
            window_size=turn_window_size(session_view_mode),
            turn_number=requested_turn,
            before_turn=before_turn,
            page_number=page,
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
    edit_href = f"/projects/edit?key={quote(group_key, safe='')}" if project_access.bypass else ""
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
    review_focus = None
    if audit_focus_turn:
        focused_turn_data = next(
            (item for item in turns_chrono if int(item.get("number") or 0) == audit_focus_turn),
            None,
        )
        review_focus = build_turn_review_focus(focused_turn_data, review)
    current_page = int(window["current_page"] or 1)
    total_pages = int(window["total_pages"] or 1)

    def page_href(page_number: int) -> str:
        if page_number <= 1:
            return f"{request.url.path}?view={quote(session_view_mode, safe='')}"
        return f"{request.url.path}?view={quote(session_view_mode, safe='')}&page={page_number}"

    pagination = {
        "total_turns": int(window["total_turns"] or 0),
        "window_size": int(window["window_size"] or 0),
        "current_page": current_page,
        "total_pages": total_pages,
        "oldest_turn": window["oldest_turn"],
        "newest_turn": window["newest_turn"],
        "has_older": bool(window["has_older"]),
        "has_newer": bool(window["has_newer"]),
        "older_href": (
            page_href(int(window["older_page"]))
            if window.get("older_page")
            else None
        ),
        "newer_href": (
            page_href(int(window["newer_page"]))
            if window.get("newer_page")
            else None
        ),
        "latest_href": f"{request.url.path}?view={quote(session_view_mode, safe='')}",
        "page_links": [
            {
                "kind": "gap" if page_token is None else "page",
                "page": page_token,
                "href": page_href(int(page_token)) if page_token is not None else None,
                "current": page_token == current_page,
            }
            for page_token in build_pagination_tokens(current_page, total_pages)
        ],
        "first_href": page_href(1) if current_page > 1 else None,
        "last_href": page_href(total_pages) if current_page < total_pages else None,
    }
    audit_focus = None
    if audit_focus_turn:
        total_turns = int(window["total_turns"] or 0)
        review_suffix = (
            f"&review={quote(str(review_focus.get('kind') or ''), safe='')}"
            if isinstance(review_focus, dict) and review_focus.get("kind")
            else ""
        )
        audit_focus = {
            "turn_number": audit_focus_turn,
            "back_to_conversation_href": f"{request.url.path}?view=conversation&turn={audit_focus_turn}",
            "full_audit_href": f"{request.url.path}?view=audit&turn={audit_focus_turn}",
            "prev_href": (
                f"{request.url.path}?view=audit&turn={audit_focus_turn - 1}&focus=1{review_suffix}"
                if audit_focus_turn > 1
                else None
            ),
            "next_href": (
                f"{request.url.path}?view=audit&turn={audit_focus_turn + 1}&focus=1{review_suffix}"
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
            "edit_href": edit_href,
            "session_view_mode": session_view_mode,
            "audit_summary": audit_summary,
            "audit_focus": audit_focus,
            "audit_focus_turn": audit_focus_turn,
            "review_focus": review_focus,
            "agent_snapshot": agent_snapshot,
            "usage_pressure": usage_pressure,
            "parent_session_href": parent_session_href,
            "parent_session_title": parent_session_title,
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
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        if not row_is_visible_to_project_access(session, project_access):
            raise HTTPException(status_code=404, detail="Session not found")
        raw_jsonl, _raw_export_info = resolve_session_raw_text(connection, context.settings, session)
    if raw_jsonl is None:
        raise HTTPException(status_code=404, detail="Source rollout file is no longer available")
    headers = {"Content-Disposition": f'attachment; filename="{session_id}.jsonl"'}
    return PlainTextResponse(content=raw_jsonl, media_type="application/x-ndjson", headers=headers)


@router.get("/sessions/{session_id}/export/json")
def export_json(request: Request, session_id: str) -> JSONResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        if not row_is_visible_to_project_access(session, project_access):
            raise HTTPException(status_code=404, detail="Session not found")
        events = get_events(connection, session_id)
    payload = export_json_payload(session, events)
    headers = {"Content-Disposition": f'attachment; filename="{session_id}.json"'}
    return JSONResponse(content=payload, headers=headers)


@router.get("/sessions/{session_id}/export/markdown")
def export_session_markdown(request: Request, session_id: str) -> PlainTextResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        if not row_is_visible_to_project_access(session, project_access):
            raise HTTPException(status_code=404, detail="Session not found")
        events = get_events(connection, session_id)
    headers = {"Content-Disposition": f'attachment; filename="{session_id}.md"'}
    return PlainTextResponse(export_markdown(session, events), media_type="text/markdown", headers=headers)


@router.get("/sessions/{session_id}/export/bundle")
def export_session_bundle(request: Request, session_id: str) -> Response:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        session = fetch_session_with_project(connection, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        if not row_is_visible_to_project_access(session, project_access):
            raise HTTPException(status_code=404, detail="Session not found")
        events = get_events(connection, session_id)
        raw_jsonl, raw_export_info = resolve_session_raw_text(connection, context.settings, session)
    if raw_jsonl is None:
        raise HTTPException(status_code=404, detail="Source rollout file is no longer available")
    bundle = build_session_bundle(
        session,
        events,
        raw_jsonl=raw_jsonl,
        raw_export_info=raw_export_info,
    )
    headers = {"Content-Disposition": f'attachment; filename="{session_id}.zip"'}
    return Response(content=bundle, media_type="application/zip", headers=headers)
