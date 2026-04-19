from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

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
from ...local_auth import (
    create_initial_admin,
    fetch_auth_status,
    touch_user_login,
    update_user_password,
    validate_new_password,
    validate_username,
    verify_local_password_for_user,
    verify_local_password_login,
)
from ...projects import (
    build_grouped_projects,
    dashboard_stats,
    effective_project_fields,
    fetch_recent_session_turn_activity,
    fetch_session_issue_counts,
    fetch_session_stream_summaries,
    fetch_session_user_turn_metadata,
    query_group_rows,
)
from ..auth import (
    build_auth_user,
    clear_auth_session,
    safe_next_path,
    set_password_session,
)
from ..context import get_app_context, request_return_to
from ..forms import parse_form_fields


router = APIRouter()


FAILED_AGENT_STATES = {
    "protocol_mismatch",
    "manual_update_required",
    "update_failed",
}


def render_settings_page(
    request: Request,
    *,
    created_token: dict[str, str] | None = None,
    password_error: str | None = None,
    password_success: str | None = None,
) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        api_tokens = list_api_tokens(connection)
        auth_status = fetch_auth_status(connection)
    current_user = getattr(request.state, "auth_user", None)
    can_change_password = bool(
        current_user
        and current_user.get("auth_source") == "password"
        and current_user.get("user_id")
        and current_user.get("is_admin")
    )
    return context.templates.TemplateResponse(
        request,
        name="settings.html",
        context={
            "request": request,
            "settings": context.settings,
            "api_tokens": api_tokens,
            "created_token": created_token,
            "search_query": "",
            "auth_status": auth_status,
            "password_error": password_error,
            "password_success": password_success,
            "can_change_password": can_change_password,
        },
    )


def render_login_page(
    request: Request,
    *,
    error: str | None = None,
    username: str = "",
) -> HTMLResponse:
    context = get_app_context(request)
    next_path = safe_next_path(request.query_params.get("next"))
    return context.templates.TemplateResponse(
        request,
        name="login.html",
        context={
            "request": request,
            "settings": context.settings,
            "search_query": "",
            "auth_error": error,
            "next_path": next_path,
            "password_login_enabled": context.settings.auth_allows_password(),
            "proxy_auth_enabled": context.settings.auth_allows_proxy(),
            "username": username,
        },
    )


def render_setup_page(
    request: Request,
    *,
    error: str | None = None,
    username: str = "admin",
) -> HTMLResponse:
    context = get_app_context(request)
    return context.templates.TemplateResponse(
        request,
        name="setup.html",
        context={
            "request": request,
            "settings": context.settings,
            "search_query": "",
            "setup_error": error,
            "username": username,
        },
    )


def agent_has_failure(remote: dict[str, object]) -> bool:
    return bool(
        int(remote.get("last_fail_count") or 0) > 0
        or remote.get("last_error")
        or remote.get("api_mismatch")
        or remote.get("version_mismatch")
        or str(remote.get("update_state") or "").strip() in FAILED_AGENT_STATES
    )


def build_active_hosts_panel(
    rows: list[sqlite3.Row],
    turn_metadata: dict[str, dict[str, str | int]],
    remotes: list[dict[str, object]],
    *,
    limit: int = 5,
) -> tuple[int, list[dict[str, object]], bool]:
    active_remotes = [remote for remote in remotes if not remote.get("stale")]
    if active_remotes:
        items = [
            {
                "source_host": str(remote["source_host"]),
                "timestamp": str(remote.get("last_seen_at") or ""),
                "detail": (
                    f"{int(remote.get('last_upload_count') or 0)} up / "
                    f"{int(remote.get('last_skip_count') or 0)} skip / "
                    f"{int(remote.get('last_fail_count') or 0)} fail"
                ),
                "status": str(remote.get("sync_mode") or "remote"),
                "status_tone": "sky",
            }
            for remote in active_remotes[:limit]
        ]
        return len(active_remotes), items, True

    host_activity: dict[str, dict[str, object]] = {}
    for row in rows:
        source_host = str(row["source_host"] or "").strip()
        if not source_host:
            continue
        timestamp = (
            str((turn_metadata.get(row["id"]) or {}).get("timestamp") or "").strip()
            or str(row["session_timestamp"] or row["started_at"] or row["imported_at"] or "").strip()
        )
        host_entry = host_activity.setdefault(
            source_host,
            {
                "source_host": source_host,
                "timestamp": timestamp,
                "sessions": 0,
                "turns": 0,
            },
        )
        host_entry["sessions"] = int(host_entry["sessions"]) + 1
        host_entry["turns"] = int(host_entry["turns"]) + int((turn_metadata.get(row["id"]) or {}).get("turn_count", 0) or 0)
        if timestamp and (not host_entry["timestamp"] or timestamp > str(host_entry["timestamp"])):
            host_entry["timestamp"] = timestamp

    ordered = sorted(
        host_activity.values(),
        key=lambda item: str(item["timestamp"] or ""),
        reverse=True,
    )
    items = [
        {
            "source_host": str(item["source_host"]),
            "timestamp": str(item["timestamp"] or ""),
            "detail": f"{int(item['sessions'])} sessions / {int(item['turns'])} turns",
            "status": "recent activity",
            "status_tone": "stone",
        }
        for item in ordered[:limit]
    ]
    return len(ordered), items, False


def build_hottest_projects_panel(
    rows: list[sqlite3.Row],
    repo_groups: list[object],
    recent_turn_activity: dict[str, dict[str, str | int]],
    *,
    limit: int = 5,
) -> list[dict[str, object]]:
    group_index = {group.key: group for group in repo_groups}
    aggregated: dict[str, dict[str, object]] = {}
    for row in rows:
        recent = recent_turn_activity.get(row["id"])
        if not recent:
            continue
        project = effective_project_fields(row)
        key = str(project["effective_group_key"])
        group = group_index.get(key)
        if group is None:
            continue
        item = aggregated.setdefault(
            key,
            {
                "display_label": group.display_label,
                "detail_href": group.detail_href,
                "recent_turn_count": 0,
                "latest_timestamp": "",
                "session_count": group.session_count,
            },
        )
        item["recent_turn_count"] = int(item["recent_turn_count"]) + int(recent.get("turn_count", 0) or 0)
        latest_timestamp = str(recent.get("latest_timestamp") or "")
        if latest_timestamp and latest_timestamp > str(item["latest_timestamp"] or ""):
            item["latest_timestamp"] = latest_timestamp

    return sorted(
        aggregated.values(),
        key=lambda item: (
            int(item["recent_turn_count"]),
            str(item["latest_timestamp"] or ""),
        ),
        reverse=True,
    )[:limit]


def build_error_sessions_panel(
    rows: list[sqlite3.Row],
    turn_metadata: dict[str, dict[str, str | int]],
    issue_counts: dict[str, dict[str, int]],
    repo_groups: list[object],
    *,
    limit: int = 6,
) -> list[dict[str, object]]:
    group_index = {group.key: group for group in repo_groups}
    items: list[dict[str, object]] = []
    for row in rows:
        issues = issue_counts.get(row["id"], {})
        command_failures = int(issues.get("command_failures", 0) or 0)
        aborted_turns = int(issues.get("aborted_turns", 0) or 0)
        import_warning = str(row["import_warning"] or "").strip()
        issue_count = command_failures + aborted_turns + (1 if import_warning else 0)
        if issue_count <= 0:
            continue

        project = effective_project_fields(row)
        group = group_index.get(str(project["effective_group_key"]))
        title = str((turn_metadata.get(row["id"]) or {}).get("message") or "").strip() or str(row["summary"] or "").strip() or "Session with errors"
        latest_timestamp = (
            str((turn_metadata.get(row["id"]) or {}).get("timestamp") or "").strip()
            or str(row["session_timestamp"] or row["started_at"] or row["imported_at"] or "").strip()
        )
        items.append(
            {
                "session_id": row["id"],
                "session_href": f"/sessions/{row['id']}",
                "title": title,
                "project_label": group.display_label if group is not None else str(project["display_label"]),
                "host": str(project["source_host"] or ""),
                "timestamp": latest_timestamp,
                "issue_count": issue_count,
                "command_failures": command_failures,
                "aborted_turns": aborted_turns,
                "has_import_warning": bool(import_warning),
            }
        )

    return sorted(
        items,
        key=lambda item: (int(item["issue_count"]), str(item["timestamp"] or "")),
        reverse=True,
    )[:limit]


@router.get("/setup", response_class=HTMLResponse)
def setup_page(request: Request) -> Response:
    context = get_app_context(request)
    if not context.settings.auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    if not getattr(request.state, "bootstrap_required", False):
        if getattr(request.state, "auth_user", None):
            return RedirectResponse(url="/", status_code=303)
        return RedirectResponse(url="/login", status_code=303)
    return render_setup_page(request)


@router.post("/setup")
async def setup_submit(request: Request) -> Response:
    context = get_app_context(request)
    if not context.settings.auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    if not getattr(request.state, "bootstrap_required", False):
        return RedirectResponse(url="/login", status_code=303)

    fields = await parse_form_fields(request)
    username = fields.get("username", "").strip() or "admin"
    password = fields.get("password", "")
    confirm_password = fields.get("confirm_password", "")

    try:
        validate_username(username)
        validate_new_password(password)
        if password != confirm_password:
            raise ValueError("Passwords do not match.")
        with connect(context.settings.database_path) as connection:
            with write_transaction(connection):
                user = create_initial_admin(
                    connection,
                    username=username,
                    password=password,
                )
    except ValueError as exc:
        return render_setup_page(request, error=str(exc), username=username)
    except sqlite3.IntegrityError:
        return render_setup_page(request, error="That username is already taken.", username=username)

    auth_user = build_auth_user(
        user_id=str(user["id"]),
        username=str(user["username"]),
        display_name=str(user["username"]),
        auth_source="password",
        is_admin=bool(user["is_admin"]),
    )
    set_password_session(request, auth_user)
    return RedirectResponse(url="/", status_code=303)


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request) -> Response:
    context = get_app_context(request)
    if not context.settings.auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    if getattr(request.state, "bootstrap_required", False):
        return RedirectResponse(url="/setup", status_code=303)
    if getattr(request.state, "auth_user", None):
        return RedirectResponse(url=safe_next_path(request.query_params.get("next")), status_code=303)
    if (
        context.settings.auth_mode == "proxy"
        and context.settings.auth_proxy_login_url
        and not context.settings.auth_allows_password()
    ):
        return RedirectResponse(url=context.settings.auth_proxy_login_url, status_code=303)
    return render_login_page(request)


@router.post("/login")
async def login_submit(request: Request) -> Response:
    context = get_app_context(request)
    if not context.settings.auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    if getattr(request.state, "bootstrap_required", False):
        return RedirectResponse(url="/setup", status_code=303)
    if not context.settings.auth_allows_password():
        return render_login_page(request, error="Password login is disabled. Use your configured SSO entrypoint.")

    fields = await parse_form_fields(request)
    username = fields.get("username", "").strip()
    password = fields.get("password", "")
    next_path = safe_next_path(fields.get("next"))

    with connect(context.settings.database_path) as connection:
        user = verify_local_password_login(connection, username, password)
        if user is None:
            return render_login_page(request, error="Invalid username or password.", username=username)
        with write_transaction(connection):
            touch_user_login(connection, str(user["id"]))

    auth_user = build_auth_user(
        user_id=str(user["id"]),
        username=str(user["username"]),
        display_name=str(user["username"]),
        auth_source="password",
        is_admin=bool(user["is_admin"]),
    )
    set_password_session(request, auth_user)
    return RedirectResponse(url=next_path, status_code=303)


@router.post("/logout")
async def logout(request: Request) -> RedirectResponse:
    context = get_app_context(request)
    if context.settings.auth_enabled():
        clear_auth_session(request)
    if getattr(request.state, "bootstrap_required", False):
        return RedirectResponse(url="/setup", status_code=303)
    if context.settings.auth_proxy_logout_url:
        return RedirectResponse(url=context.settings.auth_proxy_logout_url, status_code=303)
    return RedirectResponse(url="/login", status_code=303)


@router.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    q: str | None = Query(default=None),
    host: str | None = Query(default=None),
) -> HTMLResponse:
    context = get_app_context(request)
    now = datetime.now().astimezone()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    hot_window_start = (now - timedelta(days=7)).isoformat()
    with connect(context.settings.database_path) as connection:
        all_rows = query_group_rows(connection)
        has_filters = bool((q or "").strip() or (host or "").strip())
        rows = query_group_rows(connection, q=q, host=host) if has_filters else all_rows
        summary_overrides = fetch_session_stream_summaries(connection, [row["id"] for row in rows])
        turn_metadata = fetch_session_user_turn_metadata(connection, [row["id"] for row in rows])
        today_turn_activity = fetch_recent_session_turn_activity(
            connection,
            [row["id"] for row in rows],
            today_start,
        )
        hot_turn_activity = fetch_recent_session_turn_activity(
            connection,
            [row["id"] for row in rows],
            hot_window_start,
        )
        issue_counts = fetch_session_issue_counts(connection, [row["id"] for row in rows])
        repo_groups = build_grouped_projects(
            rows,
            route_rows=all_rows if rows is not all_rows else rows,
            summary_overrides=summary_overrides,
            turn_metadata=turn_metadata,
        )
        groups = repo_groups[: context.settings.page_size]
        stats = dashboard_stats(rows, turn_metadata=turn_metadata)
        remotes = fetch_remote_agent_health(connection, context.settings)

    active_host_count, active_hosts, active_hosts_from_agents = build_active_hosts_panel(
        rows,
        turn_metadata,
        remotes,
    )
    failed_agents = [remote for remote in remotes if agent_has_failure(remote)][:5]
    hottest_projects = build_hottest_projects_panel(
        rows,
        repo_groups,
        hot_turn_activity,
    )
    error_sessions = build_error_sessions_panel(
        rows,
        turn_metadata,
        issue_counts,
        repo_groups,
    )
    stats["active_hosts"] = active_host_count
    stats["failed_agents"] = len([remote for remote in remotes if agent_has_failure(remote)])
    stats["turns_today"] = sum(int(item.get("turn_count", 0) or 0) for item in today_turn_activity.values())
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
            "active_hosts": active_hosts,
            "active_hosts_from_agents": active_hosts_from_agents,
            "failed_agents": failed_agents,
            "hottest_projects": hottest_projects,
            "error_sessions": error_sessions,
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
        turn_metadata = fetch_session_user_turn_metadata(connection, [row["id"] for row in rows])
        groups = build_grouped_projects(
            rows,
            route_rows=all_rows,
            summary_overrides=summary_overrides,
            turn_metadata=turn_metadata,
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


@router.post("/settings/password")
async def settings_change_password(request: Request) -> HTMLResponse:
    context = get_app_context(request)
    current_user = getattr(request.state, "auth_user", None)
    if (
        not current_user
        or current_user.get("auth_source") != "password"
        or not current_user.get("user_id")
        or not current_user.get("is_admin")
    ):
        raise HTTPException(status_code=403, detail="Password changes require a local password-authenticated session.")

    fields = await parse_form_fields(request)
    current_password = fields.get("current_password", "")
    new_password = fields.get("new_password", "")
    confirm_password = fields.get("confirm_password", "")

    try:
        validate_new_password(new_password)
        if new_password != confirm_password:
            raise ValueError("New passwords do not match.")
        with connect(context.settings.database_path) as connection:
            if not verify_local_password_for_user(connection, str(current_user["user_id"]), current_password):
                raise ValueError("Current password is incorrect.")
            with write_transaction(connection):
                update_user_password(connection, str(current_user["user_id"]), new_password)
    except ValueError as exc:
        return render_settings_page(request, password_error=str(exc))

    return render_settings_page(request, password_success="Password updated.")


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
