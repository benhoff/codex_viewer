from __future__ import annotations

from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ...db import connect, write_transaction
from ...git_utils import normalize_github_remote
from ...projects import (
    delete_sessions_for_project_keys,
    fetch_group_detail,
    fetch_group_source_project_keys,
    ignore_project_keys,
    project_edit_href,
    resolve_group_key_from_detail_path,
    resolve_project_detail_href,
    upsert_project_override,
)
from ...saved_turns import count_saved_turns_by_status, owner_scope_from_request
from ..context import get_app_context, request_return_to
from ..forms import parse_form_fields


router = APIRouter()


def render_group_detail(request: Request, key: str) -> HTMLResponse:
    context = get_app_context(request)
    owner_scope = owner_scope_from_request(request)
    with connect(context.settings.database_path) as connection:
        detail = fetch_group_detail(connection, key)
        if detail is None:
            raise HTTPException(status_code=404, detail="Project group not found")
        queue_counts = count_saved_turns_by_status(connection, owner_scope, project_key=key)
    return context.templates.TemplateResponse(
        request,
        name="group.html",
        context={
            "request": request,
            "group": detail["group"],
            "source_groups": detail["source_groups"],
            "signal_summary": detail["signal_summary"],
            "attention_sessions": detail["attention_sessions"],
            "edit_href": project_edit_href(str(request.url.path)),
            "queue_counts": queue_counts,
            "queue_href": f"{str(request.url.path).rstrip('/')}/queue",
        },
    )


def render_group_edit(request: Request, key: str) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        detail = fetch_group_detail(connection, key)
        if detail is None:
            raise HTTPException(status_code=404, detail="Project group not found")
    detail["group"].detail_href = str(request.url.path).rsplit("/edit", 1)[0]
    return context.templates.TemplateResponse(
        request,
        name="group_edit.html",
        context={
            "request": request,
            "group": detail["group"],
            "source_groups": detail["source_groups"],
            "edit_return_to": request_return_to(request),
        },
    )


@router.get("/groups", response_class=HTMLResponse)
def group_detail_legacy(request: Request, key: str = Query(...)) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        detail_href = resolve_project_detail_href(connection, key)
    return RedirectResponse(url=detail_href, status_code=308)


@router.get("/projects/key/{key:path}", response_class=HTMLResponse)
def group_detail_by_key(request: Request, key: str) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        detail_href = resolve_project_detail_href(connection, key)
    return RedirectResponse(url=detail_href, status_code=308)


@router.get("/projects/edit", response_class=HTMLResponse)
def group_edit_query_legacy(request: Request, key: str = Query(...)) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        detail_href = resolve_project_detail_href(connection, key)
    return RedirectResponse(url=project_edit_href(detail_href), status_code=308)


@router.get("/projects/github/{org}/{repo:path}", response_class=HTMLResponse)
def group_detail_github_legacy(request: Request, org: str, repo: str) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        group_key = f"github:{org}/{repo}".lower()
        detail_href = resolve_project_detail_href(connection, group_key)
    return RedirectResponse(url=detail_href, status_code=308)


@router.get("/projects/directory/{host}/{directory:path}", response_class=HTMLResponse)
def group_detail_directory_legacy(request: Request, host: str, directory: str) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        group_key = f"directory:{host}:/{directory.lstrip('/')}"
        detail_href = resolve_project_detail_href(connection, group_key)
    return RedirectResponse(url=detail_href, status_code=308)


@router.get("/projects/{owner_slug}/{project_slug}/edit", response_class=HTMLResponse)
def group_edit(request: Request, owner_slug: str, project_slug: str) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        group_key = resolve_group_key_from_detail_path(connection, owner_slug, project_slug)
    if group_key is None:
        raise HTTPException(status_code=404, detail="Project group not found")
    return render_group_edit(request, group_key)


@router.get("/projects/{owner_slug}/{project_slug}", response_class=HTMLResponse)
def group_detail(request: Request, owner_slug: str, project_slug: str) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        group_key = resolve_group_key_from_detail_path(connection, owner_slug, project_slug)
    if group_key is None:
        raise HTTPException(status_code=404, detail="Project group not found")
    return render_group_detail(request, group_key)


@router.get("/projects/{owner_slug}/{project_slug}/queue", response_class=HTMLResponse)
def group_queue(request: Request, owner_slug: str, project_slug: str) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        group_key = resolve_group_key_from_detail_path(connection, owner_slug, project_slug)
    if group_key is None:
        raise HTTPException(status_code=404, detail="Project group not found")
    return RedirectResponse(url=f"/queue?project={quote(group_key, safe='')}", status_code=308)


@router.post("/overrides")
async def save_override(request: Request) -> RedirectResponse:
    context = get_app_context(request)
    fields = await parse_form_fields(request)
    match_project_key = fields.get("match_project_key", "").strip()
    if not match_project_key:
        raise HTTPException(status_code=400, detail="Missing project key")

    with connect(context.settings.database_path) as connection:
        with write_transaction(connection):
            if fields.get("action") == "clear":
                upsert_project_override(connection, match_project_key, None, None, None, None, None)
            else:
                upsert_project_override(
                    connection=connection,
                    match_project_key=match_project_key,
                    override_group_key=fields.get("override_group_key"),
                    override_organization=fields.get("override_organization"),
                    override_repository=fields.get("override_repository"),
                    override_remote_url=fields.get("override_remote_url"),
                    override_display_label=fields.get("override_display_label"),
                )

    return RedirectResponse(url=fields.get("return_to") or "/", status_code=303)


@router.post("/projects/github-url")
async def save_project_github_url(request: Request) -> RedirectResponse:
    context = get_app_context(request)
    fields = await parse_form_fields(request)
    group_key = fields.get("group_key", "").strip()
    github_url = fields.get("github_url", "").strip()
    if not group_key:
        raise HTTPException(status_code=400, detail="Missing project group key")
    normalized = normalize_github_remote(github_url)
    if normalized is None:
        raise HTTPException(status_code=400, detail="GitHub URL must be an HTTPS or SSH github.com remote")

    with connect(context.settings.database_path) as connection:
        with write_transaction(connection):
            project_keys = fetch_group_source_project_keys(connection, group_key)
            if not project_keys:
                raise HTTPException(status_code=404, detail="Project group not found")
            for match_project_key in project_keys:
                upsert_project_override(
                    connection=connection,
                    match_project_key=match_project_key,
                    override_group_key=normalized["group_key"],
                    override_organization=normalized["org"],
                    override_repository=normalized["repo"],
                    override_remote_url=normalized["canonical_url"],
                    override_display_label=f"{normalized['org']}/{normalized['repo']}",
                )
            detail_href = resolve_project_detail_href(connection, normalized["group_key"])

    return RedirectResponse(url=detail_href, status_code=303)


@router.post("/projects/actions")
async def project_action(request: Request) -> RedirectResponse:
    context = get_app_context(request)
    fields = await parse_form_fields(request)
    group_key = fields.get("group_key", "").strip()
    action = fields.get("action", "").strip()
    if not group_key:
        raise HTTPException(status_code=400, detail="Missing project group key")
    if action not in {"delete", "ignore"}:
        raise HTTPException(status_code=400, detail="Unsupported project action")

    with connect(context.settings.database_path) as connection:
        with write_transaction(connection):
            project_keys = fetch_group_source_project_keys(connection, group_key)
            if not project_keys:
                raise HTTPException(status_code=404, detail="Project group not found")
            delete_sessions_for_project_keys(connection, project_keys)
            if action == "ignore":
                ignore_project_keys(connection, project_keys)

    return RedirectResponse(url=fields.get("return_to") or "/", status_code=303)
