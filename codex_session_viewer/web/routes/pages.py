from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

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
    build_signal_badges,
    dashboard_stats,
    effective_project_fields,
    fetch_group_detail,
    fetch_recent_session_turn_activity_windows,
    query_group_rows,
    summarize_attention_status,
)
from ...saved_turns import (
    count_saved_turns_by_status,
    fetch_turn_snapshot,
    list_saved_turns,
    normalize_saved_turn_sort,
    owner_scope_from_request,
    set_saved_turn_status,
    upsert_saved_turn,
)
from ...server_settings import (
    apply_server_settings,
    normalize_expected_agent_version,
    normalize_page_size,
    parse_bool_value,
    update_server_settings,
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
    server_settings_error: str | None = None,
    server_settings_success: str | None = None,
) -> HTMLResponse:
    context = get_app_context(request)
    with connect(context.settings.database_path) as connection:
        server_settings = apply_server_settings(connection, context.settings)
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
            "server_settings": server_settings,
            "server_settings_error": server_settings_error,
            "server_settings_success": server_settings_success,
            "can_change_password": can_change_password,
        },
    )


def render_queue_page(
    request: Request,
    *,
    status: str = "open",
    sort: str = "newest",
    project_key: str | None = None,
) -> HTMLResponse:
    context = get_app_context(request)
    owner_scope = owner_scope_from_request(request)
    queue_sort = normalize_saved_turn_sort(sort)
    queue_project = None
    with connect(context.settings.database_path) as connection:
        if project_key:
            detail = fetch_group_detail(connection, project_key)
            if detail is None:
                raise HTTPException(status_code=404, detail="Project group not found")
            queue_project = detail["group"]
        counts = count_saved_turns_by_status(connection, owner_scope, project_key=project_key)
        items = list_saved_turns(
            connection,
            owner_scope,
            status=status,
            sort=queue_sort,
            project_key=project_key,
        )
    queue_scope_suffix = f"&project={quote(project_key, safe='')}" if project_key else ""
    return context.templates.TemplateResponse(
        request,
        name="queue.html",
        context={
            "request": request,
            "queue_status": status,
            "queue_sort": queue_sort,
            "queue_counts": counts,
            "queue_items": items,
            "queue_project": queue_project,
            "queue_scope_suffix": queue_scope_suffix,
            "queue_return_to": f"/queue?status={status}&sort={queue_sort}{queue_scope_suffix}",
            "queue_global_href": f"/queue?status={status}&sort={queue_sort}",
            "search_query": "",
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


def queue_action_response(
    request: Request,
    *,
    session_id: str,
    turn_number: int,
    status: str,
    return_to: str,
) -> Response:
    if "application/json" in request.headers.get("accept", "") or request.headers.get("x-codex-viewer-fetch") == "1":
        context = get_app_context(request)
        owner_scope = owner_scope_from_request(request)
        with connect(context.settings.database_path) as connection:
            open_count = count_saved_turns_by_status(connection, owner_scope).get("open", 0)
        return JSONResponse(
            {
                "ok": True,
                "status": status,
                "session_id": session_id,
                "turn_number": turn_number,
                "open_count": open_count,
                "return_to": return_to,
            }
        )
    return RedirectResponse(url=return_to, status_code=303)


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
            str(row["last_turn_timestamp"] or "").strip()
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
        host_entry["turns"] = int(host_entry["turns"]) + int(row["turn_count"] or 0)
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


def build_group_signal_map(
    rows: list[sqlite3.Row],
    recent_turn_activity: dict[str, dict[str, str | int]],
) -> dict[str, dict[str, object]]:
    signals: dict[str, dict[str, object]] = {}
    for row in rows:
        project = effective_project_fields(row)
        group_key = str(project["effective_group_key"])
        signal = signals.setdefault(
            group_key,
            {
                "recent_turn_count": 0,
                "latest_recent_timestamp": "",
                "command_failures": 0,
                "aborted_turns": 0,
                "viewer_warnings": 0,
            },
        )

        recent = recent_turn_activity.get(str(row["id"]))
        if recent:
            signal["recent_turn_count"] = int(signal["recent_turn_count"]) + int(recent.get("turn_count", 0) or 0)
            latest_recent_timestamp = str(recent.get("latest_timestamp") or "")
            if latest_recent_timestamp and latest_recent_timestamp > str(signal["latest_recent_timestamp"] or ""):
                signal["latest_recent_timestamp"] = latest_recent_timestamp

        signal["command_failures"] = int(signal["command_failures"]) + int(row["command_failure_count"] or 0)
        signal["aborted_turns"] = int(signal["aborted_turns"]) + int(row["aborted_turn_count"] or 0)
        if str(row["import_warning"] or "").strip():
            signal["viewer_warnings"] = int(signal["viewer_warnings"]) + 1

    for signal in signals.values():
        status = summarize_attention_status(
            command_failures=int(signal["command_failures"]),
            aborted_turns=int(signal["aborted_turns"]),
            viewer_warnings=int(signal["viewer_warnings"]),
            recent_turn_count=int(signal["recent_turn_count"]),
        )
        signal.update(status)
    return signals


def build_repo_nav_items(
    repo_groups: list[object],
    group_signals: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for group in repo_groups:
        signal = group_signals.get(group.key, {})
        status_tone = str(signal.get("status_tone") or "stone")
        status_title = str(signal.get("status_title") or "No recent turn activity")
        items.append(
            {
                "display_label": group.display_label,
                "detail_href": group.detail_href,
                "status_tone": status_tone,
                "status_title": status_title,
            }
        )
    return sorted(items, key=lambda item: str(item["display_label"]).lower())


def build_active_repos_panel(
    repo_groups: list[object],
    group_signals: dict[str, dict[str, object]],
    *,
    limit: int = 12,
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for group in repo_groups:
        signal = group_signals.get(group.key, {})
        recent_turn_count = int(signal.get("recent_turn_count", 0) or 0)
        command_failures = int(signal.get("command_failures", 0) or 0)
        aborted_turns = int(signal.get("aborted_turns", 0) or 0)
        viewer_warnings = int(signal.get("viewer_warnings", 0) or 0)
        latest_recent_timestamp = str(signal.get("latest_recent_timestamp") or "")
        latest_timestamp = latest_recent_timestamp or str(group.latest_timestamp or "")
        status = summarize_attention_status(
            command_failures=command_failures,
            aborted_turns=aborted_turns,
            viewer_warnings=viewer_warnings,
            recent_turn_count=recent_turn_count,
        )
        items.append(
            {
                "display_label": group.display_label,
                "detail_href": group.detail_href,
                "latest_timestamp": latest_timestamp,
                "recent_turn_count": recent_turn_count,
                "host_count": group.host_count,
                "host_label": f"{group.host_count} host" + ("" if group.host_count == 1 else "s"),
                "session_count": group.session_count,
                "summary": group.latest_summary or "",
                "command_failures": command_failures,
                "aborted_turns": aborted_turns,
                "viewer_warnings": viewer_warnings,
                "signal_badges": build_signal_badges(
                    command_failures=command_failures,
                    aborted_turns=aborted_turns,
                    viewer_warnings=viewer_warnings,
                ),
                "status_tone": str(status["status_tone"]),
                "status_label": str(status["status_label"]),
                "status_title": str(status["status_title"]),
                "has_attention": bool(status["has_attention"]),
                "attention_count": int(status["attention_count"]),
            }
        )

    items.sort(
        key=lambda item: (
            1 if item["has_attention"] else 0,
            int(item["recent_turn_count"]),
            str(item["latest_timestamp"] or ""),
            int(item["attention_count"]),
        ),
        reverse=True,
    )
    return items[:limit]


def build_error_sessions_panel(
    rows: list[sqlite3.Row],
    repo_groups: list[object],
    *,
    limit: int = 6,
) -> list[dict[str, object]]:
    group_index = {group.key: group for group in repo_groups}
    items: list[dict[str, object]] = []
    for row in rows:
        command_failures = int(row["command_failure_count"] or 0)
        aborted_turns = int(row["aborted_turn_count"] or 0)
        import_warning = str(row["import_warning"] or "").strip()
        status = summarize_attention_status(
            command_failures=command_failures,
            aborted_turns=aborted_turns,
            viewer_warnings=1 if import_warning else 0,
        )
        if not status["has_attention"]:
            continue

        project = effective_project_fields(row)
        group = group_index.get(str(project["effective_group_key"]))
        title = str(row["last_user_message"] or "").strip() or str(row["summary"] or "").strip() or "Session with errors"
        latest_timestamp = (
            str(row["last_turn_timestamp"] or "").strip()
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
                "command_failures": command_failures,
                "aborted_turns": aborted_turns,
                "viewer_warning": import_warning,
                "signal_badges": build_signal_badges(
                    command_failures=command_failures,
                    aborted_turns=aborted_turns,
                    viewer_warnings=1 if import_warning else 0,
                ),
                "status_tone": str(status["status_tone"]),
                "status_label": str(status["status_label"]),
                "attention_count": int(status["attention_count"]),
            }
        )

    return sorted(
        items,
        key=lambda item: (int(item["attention_count"]), str(item["timestamp"] or "")),
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


@router.get("/queue", response_class=HTMLResponse)
def review_queue(
    request: Request,
    status: str = Query(default="open"),
    sort: str = Query(default="newest"),
    project: str | None = Query(default=None),
) -> HTMLResponse:
    queue_status = status.strip().lower()
    if queue_status not in {"open", "resolved"}:
        queue_status = "open"
    project_key = (project or "").strip() or None
    return render_queue_page(
        request,
        status=queue_status,
        sort=normalize_saved_turn_sort(sort),
        project_key=project_key,
    )


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


@router.post("/queue/actions")
async def queue_action(request: Request) -> Response:
    context = get_app_context(request)
    owner_scope = owner_scope_from_request(request)
    fields = await parse_form_fields(request)
    action = fields.get("action", "").strip().lower()
    session_id = fields.get("session_id", "").strip()
    return_to = fields.get("return_to", "").strip() or "/queue"
    try:
        turn_number = int(fields.get("turn_number", "0") or 0)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid turn number") from exc

    if not session_id or turn_number <= 0:
        raise HTTPException(status_code=400, detail="Missing turn reference")

    if action not in {"save", "resolve", "reopen"}:
        raise HTTPException(status_code=400, detail="Unsupported queue action")

    with connect(context.settings.database_path) as connection:
        with write_transaction(connection):
            if action == "save":
                snapshot = fetch_turn_snapshot(connection, session_id, turn_number)
                if snapshot is None:
                    raise HTTPException(status_code=404, detail="Turn not found")
                upsert_saved_turn(
                    connection,
                    owner_scope=owner_scope,
                    session_id=session_id,
                    turn_number=turn_number,
                    prompt_excerpt=str(snapshot["prompt_excerpt"]),
                    response_excerpt=str(snapshot["response_excerpt"]),
                    prompt_timestamp=snapshot["prompt_timestamp"],
                    response_timestamp=snapshot["response_timestamp"],
                )
                new_status = "open"
            elif action == "resolve":
                if not set_saved_turn_status(
                    connection,
                    owner_scope=owner_scope,
                    session_id=session_id,
                    turn_number=turn_number,
                    status="resolved",
                ):
                    raise HTTPException(status_code=404, detail="Saved turn not found")
                new_status = "resolved"
            else:
                if not set_saved_turn_status(
                    connection,
                    owner_scope=owner_scope,
                    session_id=session_id,
                    turn_number=turn_number,
                    status="open",
                ):
                    raise HTTPException(status_code=404, detail="Saved turn not found")
                new_status = "open"

    return queue_action_response(
        request,
        session_id=session_id,
        turn_number=turn_number,
        status=new_status,
        return_to=return_to,
    )


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
        hot_turn_activity = fetch_recent_session_turn_activity_windows(
            connection,
            [row["id"] for row in rows],
            hot_window_start,
            secondary_since_timestamp=today_start,
        )
        repo_groups = build_grouped_projects(
            rows,
            route_rows=all_rows if rows is not all_rows else rows,
        )
        stats = dashboard_stats(rows)
        remotes = fetch_remote_agent_health(connection, context.settings)

    active_host_count, active_hosts, active_hosts_from_agents = build_active_hosts_panel(
        rows,
        remotes,
    )
    failed_agents = [remote for remote in remotes if agent_has_failure(remote)][:5]
    group_signals = build_group_signal_map(rows, hot_turn_activity)
    active_repos = build_active_repos_panel(
        repo_groups,
        group_signals,
        limit=context.settings.page_size,
    )
    repo_nav_items = build_repo_nav_items(
        repo_groups,
        group_signals,
    )
    error_sessions = build_error_sessions_panel(
        rows,
        repo_groups,
    )
    stats["active_hosts"] = active_host_count
    stats["failed_agents"] = len([remote for remote in remotes if agent_has_failure(remote)])
    stats["turns_today"] = sum(int(item.get("secondary_turn_count", 0) or 0) for item in hot_turn_activity.values())
    return context.templates.TemplateResponse(
        request,
        name="index.html",
        context={
            "request": request,
            "settings": context.settings,
            "repo_nav_items": repo_nav_items,
            "active_repos": active_repos,
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
            "error_sessions": error_sessions,
        },
    )


@router.get("/stream", response_class=HTMLResponse)
def global_stream() -> RedirectResponse:
    return RedirectResponse(url="/", status_code=308)


@router.get("/search", response_class=HTMLResponse)
def search_results(request: Request, q: str | None = Query(default=None)) -> HTMLResponse:
    context = get_app_context(request)
    search_query = (q or "").strip()
    if not search_query:
        return RedirectResponse(url="/", status_code=303)

    with connect(context.settings.database_path) as connection:
        all_rows = query_group_rows(connection)
        rows = query_group_rows(connection, q=search_query)
        groups = build_grouped_projects(
            rows,
            route_rows=all_rows,
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


@router.post("/settings/server")
async def settings_update_server(request: Request) -> HTMLResponse:
    context = get_app_context(request)
    fields = await parse_form_fields(request)

    try:
        page_size = normalize_page_size(fields.get("page_size", ""))
        expected_agent_version = normalize_expected_agent_version(
            fields.get("expected_agent_version", ""),
            context.settings.app_version,
        )
        sync_on_start = parse_bool_value(fields.get("sync_on_start"), False)
        with connect(context.settings.database_path) as connection:
            with write_transaction(connection):
                snapshot = update_server_settings(
                    connection,
                    page_size=page_size,
                    expected_agent_version=expected_agent_version,
                    sync_on_start=sync_on_start,
                )
        context.settings.page_size = snapshot.page_size
        context.settings.expected_agent_version = snapshot.expected_agent_version
        context.settings.sync_on_start = snapshot.sync_on_start
    except ValueError as exc:
        return render_settings_page(request, server_settings_error=str(exc))

    return render_settings_page(
        request,
        server_settings_success="Server settings updated.",
    )


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
