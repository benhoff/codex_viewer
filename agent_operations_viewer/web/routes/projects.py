from __future__ import annotations

from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ...db import connect, write_transaction
from ...environment_audit import fetch_project_environment_audit
from ...git_utils import normalize_github_remote
from ...local_auth import list_users
from ...projects import (
    build_project_access_context,
    delete_sessions_for_project_keys,
    fetch_turn_stream,
    fetch_group_detail,
    fetch_group_source_project_keys,
    ignore_project_keys,
    list_project_acl_members,
    normalize_project_acl_role,
    normalize_project_visibility,
    project_edit_href,
    remove_project_acl_member,
    resolve_github_project_detail_href,
    resolve_group_key_from_detail_path,
    resolve_project_detail_href,
    sync_project_registry,
    update_project_visibility,
    upsert_project_override,
    upsert_project_acl_member,
)
from ...saved_turns import count_saved_turns_by_status, owner_scope_from_request
from ...turn_index import reindex_session_turn_search_for_project_keys
from ..auth import require_admin_user
from ..context import get_app_context, request_return_to
from ..forms import parse_form_fields


router = APIRouter()


def redirect_preserving_query(
    request: Request,
    path: str,
    *,
    status_code: int = 308,
) -> RedirectResponse:
    query = str(request.url.query or "").strip()
    target = path if not query else f"{path}?{query}"
    return RedirectResponse(url=target, status_code=status_code)


def render_group_detail(request: Request, key: str, *, sessions_page: int = 1) -> HTMLResponse:
    context = get_app_context(request)
    owner_scope = owner_scope_from_request(request)
    detail_path = str(request.url.path).rstrip("/")
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        detail = fetch_group_detail(
            connection,
            key,
            sessions_page=sessions_page,
            sessions_page_size=min(context.settings.page_size, 6),
            project_access=project_access,
            owner_scope=owner_scope,
            detail_href=detail_path,
        )
        if detail is None:
            raise HTTPException(status_code=404, detail="Project group not found")
        queue_counts = count_saved_turns_by_status(
            connection,
            owner_scope,
            project_key=key,
            project_access=project_access,
        )
        stream_preview = fetch_turn_stream(
            connection,
            page=1,
            page_size=10,
            group_key=key,
            detail_href_override=detail_path,
        )

    return context.templates.TemplateResponse(
        request,
        name="group.html",
        context={
            "request": request,
            "group": detail["group"],
            "signal_summary": detail["signal_summary"],
            "project_action_queue": detail["project_action_queue"],
            "project_action_groups": detail["project_action_groups"],
            "attention_sessions": detail["attention_sessions"],
            "attention_sessions_preview": detail["attention_sessions_preview"],
            "attention_sessions_remaining": detail["attention_sessions_remaining"],
            "recent_sessions": detail["recent_sessions"],
            "all_sessions_page": detail["all_sessions_page"],
            "host_summaries": detail["host_summaries"],
            "status_strip": detail["status_strip"],
            "edit_href": project_edit_href(str(request.url.path)),
            "can_manage_project_acl": bool(project_access.bypass),
            "queue_counts": queue_counts,
            "queue_href": f"{detail_path}/queue",
            "stream_href": f"{detail_path}/stream",
            "stream_preview": stream_preview,
            "environment_href": f"{detail_path}/environment",
            "detail_return_to": detail_path,
        },
    )


def render_group_edit(request: Request, key: str) -> HTMLResponse:
    require_admin_user(request)
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        detail = fetch_group_detail(connection, key, project_access=project_access)
        if detail is None:
            raise HTTPException(status_code=404, detail="Project group not found")
        can_manage_acl = bool(project_access.bypass and getattr(request.state, "auth_enabled", False))
        managed_users = list_users(connection) if can_manage_acl else []
        project_members = (
            list_project_acl_members(connection, str(detail["group"].project_id))
            if can_manage_acl and detail["group"].project_id
            else []
        )
    detail["group"].detail_href = str(request.url.path).rsplit("/edit", 1)[0]
    return context.templates.TemplateResponse(
        request,
        name="group_edit.html",
        context={
            "request": request,
            "group": detail["group"],
            "source_groups": detail["source_groups"],
            "edit_return_to": request_return_to(request),
            "can_manage_project_acl": can_manage_acl,
            "project_members": project_members,
            "available_acl_users": [user for user in managed_users if not user.get("disabled_at")],
        },
    )


@router.get("/groups", response_class=HTMLResponse)
def group_detail_legacy(request: Request, key: str = Query(...)) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        detail_href = resolve_project_detail_href(connection, key, project_access=project_access)
        if detail_href == f"/groups?key={quote(key, safe='')}":
            raise HTTPException(status_code=404, detail="Project group not found")
    return RedirectResponse(url=detail_href, status_code=308)


@router.get("/projects/key/{key:path}", response_class=HTMLResponse)
def group_detail_by_key(request: Request, key: str) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        detail_href = resolve_project_detail_href(connection, key, project_access=project_access)
        if detail_href == f"/groups?key={quote(key, safe='')}":
            raise HTTPException(status_code=404, detail="Project group not found")
    return RedirectResponse(url=detail_href, status_code=308)


@router.get("/projects/edit", response_class=HTMLResponse)
def group_edit_query_legacy(request: Request, key: str = Query(...)) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        detail_href = resolve_project_detail_href(connection, key, project_access=project_access)
        if detail_href == f"/groups?key={quote(key, safe='')}":
            raise HTTPException(status_code=404, detail="Project group not found")
    return RedirectResponse(url=project_edit_href(detail_href), status_code=308)


@router.get("/projects/github/{org}/{repo:path}", response_class=HTMLResponse)
def group_detail_github_legacy(request: Request, org: str, repo: str) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        detail_href = resolve_github_project_detail_href(
            connection,
            org,
            repo,
            project_access=project_access,
        )
        if detail_href is None:
            raise HTTPException(status_code=404, detail="Project group not found")
    return RedirectResponse(url=detail_href, status_code=308)


@router.get("/projects/directory/{host}/{directory:path}", response_class=HTMLResponse)
def group_detail_directory_legacy(request: Request, host: str, directory: str) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        group_key = f"directory:{host}:/{directory.lstrip('/')}"
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        detail_href = resolve_project_detail_href(connection, group_key, project_access=project_access)
        if detail_href == f"/groups?key={quote(group_key, safe='')}":
            raise HTTPException(status_code=404, detail="Project group not found")
    return RedirectResponse(url=detail_href, status_code=308)


@router.get("/projects/{owner_slug}/{project_slug}/edit", response_class=HTMLResponse)
def group_edit_legacy(request: Request, owner_slug: str, project_slug: str) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        group_key = resolve_group_key_from_detail_path(
            connection,
            owner_slug,
            project_slug,
            project_access=project_access,
        )
        detail_href = (
            resolve_project_detail_href(connection, group_key, project_access=project_access)
            if group_key is not None
            else ""
        )
    if group_key is None:
        raise HTTPException(status_code=404, detail="Project group not found")
    target_path = project_edit_href(detail_href)
    if target_path != str(request.url.path):
        return redirect_preserving_query(request, target_path)
    return render_group_edit(request, group_key)


@router.get("/projects/{owner_slug}/{project_slug}", response_class=HTMLResponse)
def group_detail_path_legacy(
    request: Request,
    owner_slug: str,
    project_slug: str,
    sessions_page: int = Query(default=1),
) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        group_key = resolve_group_key_from_detail_path(
            connection,
            owner_slug,
            project_slug,
            project_access=project_access,
        )
        detail_href = (
            resolve_project_detail_href(connection, group_key, project_access=project_access)
            if group_key is not None
            else ""
        )
    if group_key is None:
        raise HTTPException(status_code=404, detail="Project group not found")
    if detail_href != str(request.url.path):
        return redirect_preserving_query(request, detail_href)
    return render_group_detail(request, group_key, sessions_page=sessions_page)


@router.get("/projects/{owner_slug}/{project_slug}/queue", response_class=HTMLResponse)
def group_queue_legacy(request: Request, owner_slug: str, project_slug: str) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        group_key = resolve_group_key_from_detail_path(
            connection,
            owner_slug,
            project_slug,
            project_access=project_access,
        )
    if group_key is None:
        raise HTTPException(status_code=404, detail="Project group not found")
    return RedirectResponse(url=f"/queue?project={quote(group_key, safe='')}", status_code=308)


@router.get("/projects/{owner_slug}/{project_slug}/environment", response_class=HTMLResponse)
def group_environment_legacy(request: Request, owner_slug: str, project_slug: str) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        group_key = resolve_group_key_from_detail_path(
            connection,
            owner_slug,
            project_slug,
            project_access=project_access,
        )
        if group_key is None:
            raise HTTPException(status_code=404, detail="Project group not found")
        detail_href = resolve_project_detail_href(connection, group_key, project_access=project_access)
        target_path = f"{detail_href.rstrip('/')}/environment"
        if target_path != str(request.url.path):
            return redirect_preserving_query(request, target_path)
        audit = fetch_project_environment_audit(connection, group_key, project_access=project_access)
        if audit is None:
            raise HTTPException(status_code=404, detail="Project group not found")
    audit["group"].detail_href = str(request.url.path).rsplit("/environment", 1)[0]
    return context.templates.TemplateResponse(
        request,
        name="project_environment.html",
        context={
            "request": request,
            "audit": audit,
            "search_query": "",
        },
    )


@router.get("/projects/{owner_slug}/{project_slug}/stream", response_class=HTMLResponse)
def group_stream_legacy(request: Request, owner_slug: str, project_slug: str, page: int = Query(default=1)) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        group_key = resolve_group_key_from_detail_path(
            connection,
            owner_slug,
            project_slug,
            project_access=project_access,
        )
        if group_key is None:
            raise HTTPException(status_code=404, detail="Project group not found")
        detail_href = resolve_project_detail_href(connection, group_key, project_access=project_access)
        target_path = f"{detail_href.rstrip('/')}/stream"
        if target_path != str(request.url.path):
            return redirect_preserving_query(request, target_path)
        detail = fetch_group_detail(connection, group_key, project_access=project_access)
        if detail is None:
            raise HTTPException(status_code=404, detail="Project group not found")
        stream_data = fetch_turn_stream(
            connection,
            page=page,
            group_key=group_key,
            detail_href_override=str(request.url.path).rsplit("/stream", 1)[0],
        )
    return context.templates.TemplateResponse(
        request,
        name="stream.html",
        context={
            "request": request,
            "stream_data": stream_data,
            "stream_project": detail["group"],
            "stream_project_key": group_key,
            "search_query": "",
        },
    )


@router.get("/{owner_slug}/{project_slug}/edit", response_class=HTMLResponse)
def group_edit(request: Request, owner_slug: str, project_slug: str) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        group_key = resolve_group_key_from_detail_path(
            connection,
            owner_slug,
            project_slug,
            project_access=project_access,
        )
    if group_key is None:
        raise HTTPException(status_code=404, detail="Project group not found")
    return render_group_edit(request, group_key)


@router.get("/{owner_slug}/{project_slug}", response_class=HTMLResponse)
def group_detail(
    request: Request,
    owner_slug: str,
    project_slug: str,
    sessions_page: int = Query(default=1),
) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        group_key = resolve_group_key_from_detail_path(
            connection,
            owner_slug,
            project_slug,
            project_access=project_access,
        )
    if group_key is None:
        raise HTTPException(status_code=404, detail="Project group not found")
    return render_group_detail(request, group_key, sessions_page=sessions_page)


@router.get("/{owner_slug}/{project_slug}/queue", response_class=HTMLResponse)
def group_queue(request: Request, owner_slug: str, project_slug: str) -> RedirectResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        group_key = resolve_group_key_from_detail_path(
            connection,
            owner_slug,
            project_slug,
            project_access=project_access,
        )
    if group_key is None:
        raise HTTPException(status_code=404, detail="Project group not found")
    return RedirectResponse(url=f"/queue?project={quote(group_key, safe='')}", status_code=308)


@router.get("/{owner_slug}/{project_slug}/environment", response_class=HTMLResponse)
def group_environment(request: Request, owner_slug: str, project_slug: str) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        group_key = resolve_group_key_from_detail_path(
            connection,
            owner_slug,
            project_slug,
            project_access=project_access,
        )
        if group_key is None:
            raise HTTPException(status_code=404, detail="Project group not found")
        audit = fetch_project_environment_audit(connection, group_key, project_access=project_access)
        if audit is None:
            raise HTTPException(status_code=404, detail="Project group not found")
    audit["group"].detail_href = str(request.url.path).rsplit("/environment", 1)[0]
    return context.templates.TemplateResponse(
        request,
        name="project_environment.html",
        context={
            "request": request,
            "audit": audit,
            "search_query": "",
        },
    )


@router.get("/{owner_slug}/{project_slug}/stream", response_class=HTMLResponse)
def group_stream(request: Request, owner_slug: str, project_slug: str, page: int = Query(default=1)) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        group_key = resolve_group_key_from_detail_path(
            connection,
            owner_slug,
            project_slug,
            project_access=project_access,
        )
        if group_key is None:
            raise HTTPException(status_code=404, detail="Project group not found")
        detail = fetch_group_detail(connection, group_key, project_access=project_access)
        if detail is None:
            raise HTTPException(status_code=404, detail="Project group not found")
        stream_data = fetch_turn_stream(
            connection,
            page=page,
            group_key=group_key,
            detail_href_override=str(request.url.path).rsplit("/stream", 1)[0],
        )
    return context.templates.TemplateResponse(
        request,
        name="stream.html",
        context={
            "request": request,
            "stream_data": stream_data,
            "stream_project": detail["group"],
            "stream_project_key": group_key,
            "search_query": "",
        },
    )


@router.post("/overrides")
async def save_override(request: Request) -> RedirectResponse:
    require_admin_user(request)
    context = get_app_context(request)
    fields = await parse_form_fields(request)
    group_key = fields.get("group_key", "").strip()
    match_project_key = fields.get("match_project_key", "").strip()
    return_to = fields.get("return_to", "").strip()
    if not group_key:
        raise HTTPException(status_code=400, detail="Missing project group key")
    if not match_project_key:
        raise HTTPException(status_code=400, detail="Missing project key")

    with connect(context.settings.database_path) as connection:
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        detail = fetch_group_detail(connection, group_key, project_access=project_access)
        if detail is None:
            raise HTTPException(status_code=404, detail="Project group not found")
        with write_transaction(connection):
            if fields.get("action") == "clear":
                upsert_project_override(connection, match_project_key, None, None, None, None, None)
                target_group_key = group_key
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
                target_group_key = fields.get("override_group_key", "").strip() or group_key
            reindex_session_turn_search_for_project_keys(connection, [match_project_key])
            sync_project_registry(connection)
            detail_href = resolve_project_detail_href(
                connection,
                target_group_key,
                project_access=project_access,
            )

    if return_to.endswith("/edit"):
        return RedirectResponse(url=project_edit_href(detail_href), status_code=303)
    return RedirectResponse(url=detail_href if detail_href else (return_to or "/"), status_code=303)


@router.post("/projects/github-url")
async def save_project_github_url(request: Request) -> RedirectResponse:
    require_admin_user(request)
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
        project_access = build_project_access_context(
            connection,
            auth_user=getattr(request.state, "auth_user", None),
            auth_enabled=bool(getattr(request.state, "auth_enabled", False)),
        )
        detail = fetch_group_detail(connection, group_key, project_access=project_access)
        if detail is None:
            raise HTTPException(status_code=404, detail="Project group not found")
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
            reindex_session_turn_search_for_project_keys(connection, project_keys)
            sync_project_registry(connection)
            detail_href = resolve_project_detail_href(
                connection,
                normalized["group_key"],
                project_access=project_access,
            )

    return RedirectResponse(url=detail_href, status_code=303)


@router.post("/projects/access")
async def project_access_action(request: Request) -> RedirectResponse:
    current_user = require_admin_user(request)
    context = get_app_context(request)
    fields = await parse_form_fields(request)
    group_key = fields.get("group_key", "").strip()
    action = fields.get("action", "").strip()
    return_to = fields.get("return_to", "").strip() or "/"
    if not group_key:
        raise HTTPException(status_code=400, detail="Missing project group key")

    with connect(context.settings.database_path) as connection:
        detail = fetch_group_detail(connection, group_key)
        if detail is None or not detail["group"].project_id:
            raise HTTPException(status_code=404, detail="Project group not found")
        project_id = str(detail["group"].project_id)
        with write_transaction(connection):
            if action == "set_visibility":
                update_project_visibility(
                    connection,
                    project_id,
                    normalize_project_visibility(fields.get("visibility", "authenticated")),
                )
            elif action == "upsert_member":
                user_id = fields.get("user_id", "").strip()
                if not user_id:
                    raise HTTPException(status_code=400, detail="Missing user id")
                upsert_project_acl_member(
                    connection,
                    project_id=project_id,
                    user_id=user_id,
                    role=normalize_project_acl_role(fields.get("role", "viewer")),
                    granted_by_user_id=str(current_user.get("user_id") or "").strip() or None,
                )
            elif action == "remove_member":
                user_id = fields.get("user_id", "").strip()
                if not user_id:
                    raise HTTPException(status_code=400, detail="Missing user id")
                remove_project_acl_member(
                    connection,
                    project_id=project_id,
                    user_id=user_id,
                )
            else:
                raise HTTPException(status_code=400, detail="Unsupported project access action")
    return RedirectResponse(url=return_to, status_code=303)


@router.post("/projects/actions")
async def project_action(request: Request) -> RedirectResponse:
    require_admin_user(request)
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
            sync_project_registry(connection)

    return RedirectResponse(url=fields.get("return_to") or "/", status_code=303)
