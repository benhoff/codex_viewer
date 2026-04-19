from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ...agents import (
    fetch_remote_agent_health,
    request_remote_raw_resend,
)
from ...api_tokens import (
    create_api_token,
    delete_api_token,
    list_api_tokens,
    revoke_api_token,
)
from ...db import connect, write_transaction
from ...importer import sync_sessions
from ...projects import (
    build_grouped_projects,
    dashboard_stats,
    fetch_session_stream_summaries,
    query_group_rows,
)
from ..context import get_app_context, request_return_to
from ..forms import parse_form_fields


router = APIRouter()


def render_settings_page(
    request: Request,
    *,
    created_token: dict[str, str] | None = None,
) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        api_tokens = list_api_tokens(connection)
    return context.templates.TemplateResponse(
        request,
        name="settings.html",
        context={
            "request": request,
            "settings": context.settings,
            "api_tokens": api_tokens,
            "created_token": created_token,
            "search_query": "",
        },
    )


@router.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    q: str | None = Query(default=None),
    host: str | None = Query(default=None),
) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        all_rows = query_group_rows(connection)
        has_filters = bool((q or "").strip() or (host or "").strip())
        rows = query_group_rows(connection, q=q, host=host) if has_filters else all_rows
        summary_overrides = fetch_session_stream_summaries(connection, [row["id"] for row in rows])
        repo_groups = build_grouped_projects(
            rows,
            route_rows=all_rows if rows is not all_rows else rows,
            summary_overrides=summary_overrides,
        )
        groups = repo_groups[: context.settings.page_size]
        stats = dashboard_stats(rows)
    return context.templates.TemplateResponse(
        request,
        name="index.html",
        context={
            "request": request,
            "settings": context.settings,
            "groups": groups,
            "repo_groups": sorted(
                repo_groups,
                key=lambda item: (item.display_label or "").lower(),
            ),
            "group_total": len(repo_groups),
            "stats": stats,
            "q": q or "",
            "host": host or "",
            "has_filters": has_filters,
            "return_to": request_return_to(request),
            "search_query": "",
        },
    )


@router.get("/search", response_class=HTMLResponse)
def search_results(request: Request, q: str | None = Query(default=None)) -> HTMLResponse:
    context = get_app_context(request)
    search_query = (q or "").strip()
    if not search_query:
        return RedirectResponse(url="/", status_code=303)

    with connect(context.settings.database_path) as connection:
        all_rows = query_group_rows(connection)
        rows = query_group_rows(connection, q=search_query)
        summary_overrides = fetch_session_stream_summaries(connection, [row["id"] for row in rows])
        groups = build_grouped_projects(
            rows,
            route_rows=all_rows,
            summary_overrides=summary_overrides,
        )

    return context.templates.TemplateResponse(
        request,
        name="search.html",
        context={
            "request": request,
            "groups": groups,
            "group_total": len(groups),
            "q": search_query,
            "return_to": request_return_to(request),
            "search_query": search_query,
        },
    )


@router.get("/remotes", response_class=HTMLResponse)
def remotes_health(request: Request) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        remotes = fetch_remote_agent_health(connection, context.settings)
    return context.templates.TemplateResponse(
        request,
        name="remotes.html",
        context={
            "request": request,
            "settings": context.settings,
            "remotes": remotes,
            "return_to": request_return_to(request),
            "search_query": "",
        },
    )


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request) -> HTMLResponse:
    return render_settings_page(request)


@router.post("/settings/api-tokens")
async def create_settings_api_token(request: Request) -> HTMLResponse:
    context = get_app_context(request)
    fields = await parse_form_fields(request)
    label = fields.get("label", "")
    with connect(context.settings.database_path) as connection:
        with write_transaction(connection):
            created_token = create_api_token(connection, label)
    return render_settings_page(request, created_token=created_token)


@router.post("/settings/api-tokens/actions")
async def settings_api_token_action(request: Request) -> RedirectResponse:
    context = get_app_context(request)
    fields = await parse_form_fields(request)
    token_id = fields.get("token_id", "").strip()
    action = fields.get("action", "").strip()
    if not token_id:
        raise HTTPException(status_code=400, detail="Missing token id")
    if action not in {"revoke", "delete"}:
        raise HTTPException(status_code=400, detail="Unsupported token action")

    with connect(context.settings.database_path) as connection:
        with write_transaction(connection):
            if action == "revoke":
                revoke_api_token(connection, token_id)
            else:
                delete_api_token(connection, token_id)

    return RedirectResponse(url="/settings", status_code=303)


@router.post("/remotes/actions")
async def remote_action(request: Request) -> RedirectResponse:
    context = get_app_context(request)
    fields = await parse_form_fields(request)
    source_host = fields.get("source_host", "").strip()
    action = fields.get("action", "").strip()
    if not source_host:
        raise HTTPException(status_code=400, detail="Missing remote host")
    if action != "request_raw_resend":
        raise HTTPException(status_code=400, detail="Unsupported remote action")

    with connect(context.settings.database_path) as connection:
        with write_transaction(connection):
            request_remote_raw_resend(
                connection,
                source_host,
                note="Requested from remotes view",
            )

    return RedirectResponse(url=fields.get("return_to") or "/remotes", status_code=303)


@router.post("/refresh")
def refresh(request: Request) -> RedirectResponse:
    context = get_app_context(request)
    sync_sessions(context.settings)
    return RedirectResponse(url="/", status_code=303)
