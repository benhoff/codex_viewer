from __future__ import annotations

import json
from urllib.parse import parse_qs, quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.concurrency import run_in_threadpool

from ...db import connect, write_transaction
from ...projects import build_project_access_context, fetch_session_with_project, row_is_visible_to_project_access
from ...saved_turns import owner_scope_from_request
from ...task_assessment import (
    DEFAULT_POLICY, DEMAND_FIELDS, JUDGMENTS, TEXT_FIELDS, empty_review, read_revision,
    report_for_source, revisions, save_revision, task_source, validate_policy, validate_review,
)
from ..context import get_app_context

router = APIRouter()
PRIVATE_HEADERS = {"Cache-Control": "private, no-store"}


def visible_session(connection, request: Request, session_id: str) -> dict:
    access = build_project_access_context(connection, auth_user=getattr(request.state, "auth_user", None),
                                          auth_enabled=bool(getattr(request.state, "auth_enabled", False)))
    session = fetch_session_with_project(connection, session_id)
    if session is None or not row_is_visible_to_project_access(session, access):
        raise HTTPException(404, "Session not found")
    return dict(session)


def load_assessment(request: Request, session_id: str, start: int, end: int, revision: int | None) -> dict:
    context = get_app_context(request)
    owner = owner_scope_from_request(request)
    try:
        with connect(context.settings.database_path) as connection:
            session = visible_session(connection, request, session_id)
            with connection:  # Pin source and review reads to one SQLite snapshot.
                connection.execute("BEGIN")
                history = revisions(connection, owner, session_id, start, end)
                selected = revision if revision is not None else history[0]["id"] if history else None
                saved = read_revision(connection, owner, session_id, start, end, selected) if selected is not None else None
                try:
                    source = task_source(connection, session, start, end)
                except LookupError:
                    if revision is None:
                        raise
                    # A pinned review remains inspectable after turn removal or
                    # resegmentation, provided the session is still accessible.
                    source = None
                policy = saved["policy"] if saved else validate_policy(DEFAULT_POLICY)
                report = saved["snapshot"] if revision is not None else report_for_source(source, policy)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    current_digest = source["evidence_digest"] if source else None
    return {"report": report, "review": saved["review"] if saved else empty_review(),
            "saved": {key: saved[key] for key in ("id", "created_at", "evidence_digest")} if saved else None,
            "history": history, "stale": bool(saved and saved["evidence_digest"] != current_digest),
            "historical": revision is not None, "session_title": session.get("summary") or session_id,
            "current_evidence_digest": current_digest}


@router.get("/sessions/{session_id}/assessment", response_class=HTMLResponse)
def assessment_page(request: Request, session_id: str, start_turn: int = Query(1, ge=1),
                    end_turn: int | None = Query(None, ge=1), revision: int | None = Query(None, ge=1)):
    data = load_assessment(request, session_id, start_turn, end_turn if end_turn is not None else start_turn, revision)
    return get_app_context(request).templates.TemplateResponse(
        request, name="assessment.html", context={"request": request, **data,
        "judgments": JUDGMENTS, "demand_fields": DEMAND_FIELDS, "text_fields": TEXT_FIELDS,
        "policy_text": json.dumps(data["report"]["policy"], indent=2),
        "session_href": f"/sessions/{quote(session_id, safe='')}",
        }, headers=PRIVATE_HEADERS)


@router.get("/sessions/{session_id}/assessment.json")
def assessment_json(request: Request, session_id: str, start_turn: int = Query(1, ge=1),
                    end_turn: int | None = Query(None, ge=1), revision: int | None = Query(None, ge=1)):
    return JSONResponse(load_assessment(request, session_id, start_turn, end_turn if end_turn is not None else start_turn, revision),
                        headers=PRIVATE_HEADERS)


def persist_assessment(request: Request, session_id: str, fields: dict):
    try:
        start = int(fields.get("start_turn", "0"))
        end = int(fields.get("end_turn", "0"))
        policy = validate_policy(json.loads(fields.get("policy", "{}")))
        context = get_app_context(request)
        owner = owner_scope_from_request(request)
        with connect(context.settings.database_path) as connection:
            with write_transaction(connection):
                session = visible_session(connection, request, session_id)
                source = task_source(connection, session, start, end)
                if fields.get("evidence_digest") != source["evidence_digest"]:
                    raise HTTPException(409, "Task evidence changed. Reload the assessment before saving.")
                review = validate_review(fields, {event["event_index"] for event in source["events"]})
                report = report_for_source(source, policy)
                save_revision(connection, owner, report, review)
    except (ValueError, TypeError, OverflowError) as exc:
        raise HTTPException(400, str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    return RedirectResponse(f"/sessions/{quote(session_id, safe='')}/assessment?start_turn={start}&end_turn={end}",
                            status_code=303, headers=PRIVATE_HEADERS)


@router.post("/sessions/{session_id}/assessment")
async def assessment_save(request: Request, session_id: str):
    origin = request.headers.get("origin")
    if origin and origin.rstrip("/") != str(request.base_url).rstrip("/"):
        raise HTTPException(403, "Cross-origin review writes are not allowed.")
    if request.headers.get("sec-fetch-site") == "cross-site":
        raise HTTPException(403, "Cross-site review writes are not allowed.")
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 131_072:
            raise HTTPException(413, "Assessment form exceeds 128 KiB.")
    try:
        parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True, max_num_fields=50)
        fields = {key: values[-1] for key, values in parsed.items()}
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(400, "Invalid assessment form.") from exc
    return await run_in_threadpool(persist_assessment, request, session_id, fields)
