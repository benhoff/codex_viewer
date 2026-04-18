from __future__ import annotations

import argparse
import json
import logging
import signal
import sqlite3
import threading
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, quote


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEPENDENCY_ROOT = PROJECT_ROOT / ".deps"
if DEPENDENCY_ROOT.exists():
    sys.path.insert(0, str(DEPENDENCY_ROOT))

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

from .agents import fetch_remote_agent_health, upsert_remote_agent_status
from .config import Settings
from .db import connect, init_db
from .git_utils import normalize_github_remote
from .importer import (
    fetch_host_sync_manifest,
    parsed_session_from_payload,
    shorten,
    strip_codex_wrappers,
    sync_sessions,
    upsert_parsed_session,
)
from .projects import (
    build_grouped_projects,
    dashboard_stats,
    delete_sessions_for_project_keys,
    effective_project_fields,
    fetch_group_detail,
    fetch_group_source_project_keys,
    fetch_session_with_project,
    group_key_from_project_path,
    ignored_project_keys,
    ignore_project_keys,
    project_detail_href,
    query_group_rows,
    upsert_project_override,
)
from .remote_sync import RestartRequired, sync_sessions_remote


def template_filters() -> Jinja2Templates:
    templates = Jinja2Templates(directory=str(PROJECT_ROOT / "codex_session_viewer" / "templates"))
    env = templates.env
    env.filters["shorten"] = shorten
    env.filters["humanize_timestamp"] = humanize_timestamp
    env.filters["full_timestamp"] = full_timestamp
    return templates


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def humanize_timestamp(value: str | None) -> str:
    parsed = parse_timestamp(value)
    if parsed is None:
        return "Unknown activity"
    now = datetime.now(UTC)
    delta = now - parsed.astimezone(UTC)
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 60:
        return "Just now"
    if seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    if seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = seconds // 86400
    if days == 1:
        return "Yesterday"
    if days < 7:
        return f"{days} days ago"
    local = parsed.astimezone()
    now_local = now.astimezone()
    if local.year == now_local.year:
        return local.strftime("%b %d").replace(" 0", " ")
    return local.strftime("%b %d, %Y").replace(" 0", " ")


def full_timestamp(value: str | None) -> str:
    parsed = parse_timestamp(value)
    if parsed is None:
        return value or "Unknown time"
    return parsed.astimezone().strftime("%b %d, %Y %I:%M %p %Z").replace(" 0", " ")


def kind_style(kind: str, role: str | None = None) -> str:
    if kind == "message" and role == "user":
        return "rose"
    if kind == "message" and role == "assistant":
        return "emerald"
    if kind == "tool_call":
        return "sky"
    if kind == "tool_result":
        return "cyan"
    if kind == "command":
        return "amber"
    if kind == "telemetry":
        return "slate"
    return "stone"


def styled_event(event: sqlite3.Row | dict[str, object]) -> dict[str, object]:
    row = dict(event)
    row["style"] = kind_style(str(row.get("kind") or ""), row.get("role"))
    return row


def is_user_turn_start(event: sqlite3.Row | dict[str, object], prefer_event_msg: bool) -> bool:
    row = event
    if row["kind"] != "message" or row["role"] != "user":
        return False
    if prefer_event_msg:
        return row["record_type"] == "event_msg" and row["payload_type"] == "user_message"
    return row["record_type"] == "response_item" and row["payload_type"] == "message"


def build_turns(events: list[sqlite3.Row]) -> list[dict[str, object]]:
    prefer_event_msg = any(
        row["kind"] == "message"
        and row["role"] == "user"
        and row["record_type"] == "event_msg"
        and row["payload_type"] == "user_message"
        for row in events
    )

    turns: list[dict[str, object]] = []
    current: dict[str, object] | None = None

    def finalize_turn(turn: dict[str, object]) -> dict[str, object]:
        assistant_messages: list[sqlite3.Row] = turn["assistant_messages"]  # type: ignore[assignment]
        assistant_updates: list[sqlite3.Row] = turn["assistant_updates"]  # type: ignore[assignment]
        all_events: list[sqlite3.Row] = turn["events"]  # type: ignore[assignment]

        final_response_event = assistant_messages[-1] if assistant_messages else None
        if final_response_event is None and assistant_updates:
            final_response_event = assistant_updates[-1]

        response_text = ""
        response_timestamp = None
        if final_response_event is not None:
            response_text = str(final_response_event["display_text"] or "")
            response_timestamp = final_response_event["timestamp"]

        prompt_text = str(turn["prompt_text"])
        prompt_excerpt = shorten(prompt_text, 220)
        response_excerpt = shorten(response_text, 280) if response_text else "No assistant response captured."

        detail_events: list[dict[str, object]] = []
        for event in all_events:
            skip = False
            if event["kind"] == "message" and event["role"] == "user":
                skip = True
            if (
                final_response_event is not None
                and event["event_index"] == final_response_event["event_index"]
            ):
                skip = True
            if (
                event["kind"] == "message"
                and event["role"] == "assistant"
                and event["record_type"] == "response_item"
            ):
                skip = True
            if skip:
                continue
            detail_events.append(styled_event(event))

        return {
            "number": turn["number"],
            "prompt_text": prompt_text,
            "prompt_excerpt": prompt_excerpt,
            "prompt_timestamp": turn["prompt_timestamp"],
            "response_text": response_text,
            "response_excerpt": response_excerpt,
            "response_timestamp": response_timestamp,
            "detail_events": detail_events,
            "work_count": len(detail_events),
        }

    for event in events:
        if is_user_turn_start(event, prefer_event_msg):
            cleaned_prompt = strip_codex_wrappers(str(event["display_text"] or "")).strip()
            if not cleaned_prompt:
                continue
            if current is not None:
                turns.append(finalize_turn(current))
            current = {
                "number": len(turns) + 1,
                "prompt_text": cleaned_prompt,
                "prompt_timestamp": event["timestamp"],
                "events": [],
                "assistant_messages": [],
                "assistant_updates": [],
            }
            continue

        if current is None:
            continue

        current["events"].append(event)
        if event["kind"] == "message" and event["role"] == "assistant":
            if event["record_type"] == "response_item" and event["payload_type"] == "message":
                current["assistant_messages"].append(event)
            elif event["record_type"] == "event_msg" and event["payload_type"] == "agent_message":
                current["assistant_updates"].append(event)

    if current is not None:
        turns.append(finalize_turn(current))

    return turns


def get_events(connection: sqlite3.Connection, session_id: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM events
        WHERE session_id = ?
        ORDER BY event_index ASC
        """,
        (session_id,),
    ).fetchall()


def export_markdown(session: sqlite3.Row, events: list[sqlite3.Row]) -> str:
    lines = [
        f"# {session['summary']}",
        "",
        f"- Session ID: `{session['id']}`",
        f"- Timestamp: `{session['session_timestamp'] or session['started_at'] or 'unknown'}`",
        f"- CWD: `{session['cwd'] or 'unknown'}`",
        f"- Host: `{session['source_host'] or 'unknown'}`",
        f"- Model Provider: `{session['model_provider'] or 'unknown'}`",
        "",
        "## Timeline",
        "",
    ]

    for event in events:
        lines.append(f"### {event['title']}")
        lines.append("")
        lines.append(f"- Kind: `{event['kind']}`")
        if event["role"]:
            lines.append(f"- Role: `{event['role']}`")
        if event["timestamp"]:
            lines.append(f"- Timestamp: `{event['timestamp']}`")
        if event["tool_name"]:
            lines.append(f"- Tool: `{event['tool_name']}`")
        if event["command_text"]:
            lines.append(f"- Command: `{event['command_text']}`")
        if event["call_id"]:
            lines.append(f"- Call ID: `{event['call_id']}`")
        lines.append("")
        lines.append("~~~text")
        lines.append(event["detail_text"] or event["display_text"] or "")
        lines.append("~~~")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


async def parse_form_fields(request: Request) -> dict[str, str]:
    body = await request.body()
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def require_sync_api_auth(request: Request, settings: Settings) -> None:
    expected = settings.sync_api_token
    if not expected:
        return
    authorization = request.headers.get("authorization", "")
    if authorization != f"Bearer {expected}":
        raise HTTPException(status_code=401, detail="Unauthorized")


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or Settings.from_env(PROJECT_ROOT)
    app_settings.ensure_directories()
    init_db(app_settings.database_path)

    app = FastAPI(title="Codex Session Viewer", version="0.1.0")
    templates = template_filters()

    app.mount(
        "/static",
        StaticFiles(directory=str(PROJECT_ROOT / "codex_session_viewer" / "static")),
        name="static",
    )

    @app.on_event("startup")
    def startup_sync() -> None:
        if app_settings.sync_on_start and app_settings.sync_mode == "local":
            sync_sessions(app_settings)

    @app.get("/", response_class=HTMLResponse)
    def index(
        request: Request,
        q: str | None = Query(default=None),
        host: str | None = Query(default=None),
    ) -> HTMLResponse:
        with connect(app_settings.database_path) as connection:
            rows = query_group_rows(connection, q=q, host=host)
            repo_groups = build_grouped_projects(rows)
            groups = repo_groups[: app_settings.page_size]
            stats = dashboard_stats(rows)
        return templates.TemplateResponse(
            request,
            name="index.html",
            context={
                "request": request,
                "settings": app_settings,
                "groups": groups,
                "repo_groups": sorted(
                    repo_groups,
                    key=lambda item: (
                        (item.organization or "").lower(),
                        (item.repository or "").lower(),
                        (item.display_label or "").lower(),
                    ),
                ),
                "group_total": len(repo_groups),
                "stats": stats,
                "q": q or "",
                "host": host or "",
                "has_filters": bool((q or "").strip() or (host or "").strip()),
                "return_to": str(request.url.path) + (f"?{request.url.query}" if request.url.query else ""),
            },
        )

    @app.get("/remotes", response_class=HTMLResponse)
    def remotes_health(request: Request) -> HTMLResponse:
        with connect(app_settings.database_path) as connection:
            remotes = fetch_remote_agent_health(connection, app_settings)
        return templates.TemplateResponse(
            request,
            name="remotes.html",
            context={
                "request": request,
                "settings": app_settings,
                "remotes": remotes,
            },
        )

    @app.post("/refresh")
    def refresh() -> RedirectResponse:
        sync_sessions(app_settings)
        return RedirectResponse(url="/", status_code=303)

    @app.get("/groups", response_class=HTMLResponse)
    def group_detail_legacy(key: str = Query(...)) -> RedirectResponse:
        return RedirectResponse(url=project_detail_href(key), status_code=308)

    def render_group_detail(request: Request, key: str) -> HTMLResponse:
        with connect(app_settings.database_path) as connection:
            detail = fetch_group_detail(connection, key)
            if detail is None:
                raise HTTPException(status_code=404, detail="Project group not found")
        return templates.TemplateResponse(
            request,
            name="group.html",
            context={
                "request": request,
                "group": detail["group"],
                "source_groups": detail["source_groups"],
            },
        )

    @app.get("/projects/key/{key:path}", response_class=HTMLResponse)
    def group_detail_by_key(request: Request, key: str) -> HTMLResponse:
        return render_group_detail(request, key)

    @app.get("/projects/{root}/{project}/{key:path}", response_class=HTMLResponse)
    def group_detail(request: Request, root: str, project: str, key: str) -> HTMLResponse:
        try:
            group_key = group_key_from_project_path(root, project, key)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return render_group_detail(request, group_key)

    @app.post("/overrides")
    async def save_override(request: Request) -> RedirectResponse:
        fields = await parse_form_fields(request)
        match_project_key = fields.get("match_project_key", "").strip()
        if not match_project_key:
            raise HTTPException(status_code=400, detail="Missing project key")

        with connect(app_settings.database_path) as connection:
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

    @app.post("/projects/github-url")
    async def save_project_github_url(request: Request) -> RedirectResponse:
        fields = await parse_form_fields(request)
        group_key = fields.get("group_key", "").strip()
        github_url = fields.get("github_url", "").strip()
        if not group_key:
            raise HTTPException(status_code=400, detail="Missing project group key")
        normalized = normalize_github_remote(github_url)
        if normalized is None:
            raise HTTPException(status_code=400, detail="GitHub URL must be an HTTPS or SSH github.com remote")

        with connect(app_settings.database_path) as connection:
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

        return RedirectResponse(
            url=project_detail_href(normalized["group_key"]),
            status_code=303,
        )

    @app.post("/projects/actions")
    async def project_action(request: Request) -> RedirectResponse:
        fields = await parse_form_fields(request)
        group_key = fields.get("group_key", "").strip()
        action = fields.get("action", "").strip()
        if not group_key:
            raise HTTPException(status_code=400, detail="Missing project group key")
        if action not in {"delete", "ignore"}:
            raise HTTPException(status_code=400, detail="Unsupported project action")

        with connect(app_settings.database_path) as connection:
            project_keys = fetch_group_source_project_keys(connection, group_key)
            if not project_keys:
                raise HTTPException(status_code=404, detail="Project group not found")
            delete_sessions_for_project_keys(connection, project_keys)
            if action == "ignore":
                ignore_project_keys(connection, project_keys)

        return RedirectResponse(url=fields.get("return_to") or "/", status_code=303)

    @app.get("/sessions/{session_id}", response_class=HTMLResponse)
    def session_detail(request: Request, session_id: str) -> HTMLResponse:
        with connect(app_settings.database_path) as connection:
            session = fetch_session_with_project(connection, session_id)
            if session is None:
                raise HTTPException(status_code=404, detail="Session not found")
            events = get_events(connection, session_id)
        project = effective_project_fields(session)
        turns = build_turns(events)

        secondary_events = [
            styled_event(event)
            for event in events
            if event["kind"] in {"system", "context", "telemetry", "reasoning"}
        ]

        return templates.TemplateResponse(
            request,
            name="session.html",
            context={
                "request": request,
                "session": session,
                "project": project,
                "group_href": project_detail_href(project["effective_group_key"]),
                "turns": turns,
                "secondary_events": secondary_events,
                "source_roots": [str(path) for path in app_settings.session_roots],
            },
        )

    @app.get("/sessions/{session_id}/export/raw")
    def export_raw(session_id: str) -> PlainTextResponse:
        with connect(app_settings.database_path) as connection:
            session = fetch_session_with_project(connection, session_id)
            if session is None:
                raise HTTPException(status_code=404, detail="Session not found")
        source_path = Path(session["source_path"])
        if not source_path.exists():
            raise HTTPException(status_code=404, detail="Source rollout file is no longer available")
        content = source_path.read_text(encoding="utf-8")
        headers = {"Content-Disposition": f'attachment; filename="{session_id}.jsonl"'}
        return PlainTextResponse(content=content, media_type="application/x-ndjson", headers=headers)

    @app.get("/sessions/{session_id}/export/json")
    def export_json(session_id: str) -> JSONResponse:
        with connect(app_settings.database_path) as connection:
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

    @app.get("/sessions/{session_id}/export/markdown")
    def export_session_markdown(session_id: str) -> PlainTextResponse:
        with connect(app_settings.database_path) as connection:
            session = fetch_session_with_project(connection, session_id)
            if session is None:
                raise HTTPException(status_code=404, detail="Session not found")
            events = get_events(connection, session_id)
        headers = {"Content-Disposition": f'attachment; filename="{session_id}.md"'}
        return PlainTextResponse(export_markdown(session, events), media_type="text/markdown", headers=headers)

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {
            "status": "ok",
            "app_version": app_settings.app_version,
            "sync_api_version": app_settings.sync_api_version,
            "expected_agent_version": app_settings.expected_agent_version,
        }

    @app.get("/api/sync/manifest")
    def sync_manifest(request: Request, host: str = Query(...)) -> JSONResponse:
        require_sync_api_auth(request, app_settings)
        with connect(app_settings.database_path) as connection:
            sessions = fetch_host_sync_manifest(connection, host)
            ignored_keys = sorted(ignored_project_keys(connection))
        return JSONResponse(
            {
                "host": host,
                "sessions": sessions,
                "ignored_project_keys": ignored_keys,
                "server": {
                    "app_version": app_settings.app_version,
                    "sync_api_version": app_settings.sync_api_version,
                    "expected_agent_version": app_settings.expected_agent_version,
                    "minimum_agent_version": app_settings.minimum_agent_version,
                },
            }
        )

    @app.post("/api/sync/heartbeat")
    async def sync_heartbeat(request: Request) -> JSONResponse:
        require_sync_api_auth(request, app_settings)
        try:
            payload = await request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Heartbeat payload must be an object")

        source_host = str(payload.get("source_host") or "").strip()
        if not source_host:
            raise HTTPException(status_code=400, detail="Heartbeat payload is missing source_host")

        with connect(app_settings.database_path) as connection:
            upsert_remote_agent_status(
                connection,
                source_host=source_host,
                agent_version=str(payload.get("agent_version") or ""),
                sync_api_version=str(payload.get("sync_api_version") or ""),
                sync_mode=str(payload.get("sync_mode") or ""),
                update_state=str(payload.get("update_state") or ""),
                update_message=str(payload.get("update_message") or "") or None,
                server_version_seen=str(payload.get("server_version_seen") or "") or None,
                server_api_version_seen=str(payload.get("server_api_version_seen") or "") or None,
                last_sync_at=str(payload.get("last_sync_at") or "") or None,
                last_upload_count=int(payload.get("last_upload_count") or 0),
                last_skip_count=int(payload.get("last_skip_count") or 0),
                last_fail_count=int(payload.get("last_fail_count") or 0),
                last_error=str(payload.get("last_error") or "") or None,
            )
        return JSONResponse({"status": "ok", "source_host": source_host})

    @app.post("/api/sync/session")
    async def sync_session(request: Request) -> JSONResponse:
        require_sync_api_auth(request, app_settings)
        try:
            payload = await request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Sync payload must be an object")
        try:
            parsed = parsed_session_from_payload(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        with connect(app_settings.database_path) as connection:
            if parsed.inferred_project_key in ignored_project_keys(connection):
                return JSONResponse(
                    {
                        "status": "ignored",
                        "session_id": parsed.session_id,
                        "source_host": parsed.source_host,
                        "inferred_project_key": parsed.inferred_project_key,
                    }
                )
            upsert_parsed_session(connection, parsed)

        return JSONResponse(
            {
                "status": "ok",
                "session_id": parsed.session_id,
                "source_host": parsed.source_host,
                "event_count": parsed.event_count,
                "content_sha256": parsed.content_sha256,
            }
        )

    return app


app = create_app()


def run_sync_daemon(settings: Settings, interval_seconds: int, rebuild_on_start: bool = False) -> int:
    logger = logging.getLogger("codex_session_viewer.daemon")
    stop_event = threading.Event()
    interval_seconds = max(1, interval_seconds)

    def _request_shutdown(signum: int, _frame: object) -> None:
        logger.info("Received signal %s, shutting down agent daemon", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    if settings.sync_mode == "remote":
        logger.info(
            "Starting agent daemon with interval=%ss roots=%s target=%s mode=remote",
            interval_seconds,
            ",".join(str(path) for path in settings.session_roots),
            settings.server_base_url or "unconfigured",
        )
    else:
        init_db(settings.database_path)
        logger.info(
            "Starting agent daemon with interval=%ss roots=%s db=%s mode=local",
            interval_seconds,
            ",".join(str(path) for path in settings.session_roots),
            settings.database_path,
        )

    first_run = True
    while not stop_event.is_set():
        force = rebuild_on_start and first_run
        try:
            if settings.sync_mode == "remote":
                stats = sync_sessions_remote(settings, force=force)
            else:
                stats = sync_sessions(settings, force=force)
        except RestartRequired as exc:
            logger.info("Agent update completed, restarting daemon: %s", exc)
            return 75
        logger.info("Sync pass finished: %s", json.dumps(stats, sort_keys=True))
        first_run = False
        if stop_event.wait(interval_seconds):
            break

    logger.info("Agent daemon stopped")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="View and export Codex sessions")
    subparsers = parser.add_subparsers(dest="command", required=False)

    serve = subparsers.add_parser("serve", help="Run the FastAPI server")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    serve.add_argument("--no-sync", action="store_true")

    subparsers.add_parser("sync", help="Import rollout files into SQLite")
    sync = subparsers.choices["sync"]
    sync.add_argument("--rebuild", action="store_true")

    daemon = subparsers.add_parser("daemon", help="Run the background sync daemon")
    daemon.add_argument("--interval", type=int)
    daemon.add_argument("--rebuild-on-start", action="store_true")

    export = subparsers.add_parser("export", help="Export one imported session")
    export.add_argument("session_id")
    export.add_argument("--format", choices=["json", "markdown"], default="json")

    return parser.parse_args()


def cli() -> int:
    args = parse_args()
    settings = Settings.from_env(PROJECT_ROOT)
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.command in {None, "serve"}:
        if getattr(args, "no_sync", False):
            settings.sync_on_start = False
        uvicorn.run(
            create_app(settings),
            host=getattr(args, "host", None) or settings.server_host,
            port=getattr(args, "port", None) or settings.server_port,
            reload=False,
            log_level=settings.log_level.lower(),
        )
        return 0

    if args.command == "sync":
        settings.ensure_directories()
        init_db(settings.database_path)
        stats = sync_sessions(settings, force=getattr(args, "rebuild", False))
        print(json.dumps(stats, indent=2))
        return 0

    if args.command == "daemon":
        interval_seconds = getattr(args, "interval", None) or settings.sync_interval_seconds
        return run_sync_daemon(
            settings,
            interval_seconds=interval_seconds,
            rebuild_on_start=getattr(args, "rebuild_on_start", False),
        )

    if args.command == "export":
        settings.ensure_directories()
        init_db(settings.database_path)
        with connect(settings.database_path) as connection:
            session = fetch_session_with_project(connection, args.session_id)
            if session is None:
                raise SystemExit(f"Unknown session: {args.session_id}")
            events = get_events(connection, args.session_id)

        if args.format == "json":
            print(json.dumps({"session": dict(session), "events": [dict(row) for row in events]}, indent=2))
        else:
            print(export_markdown(session, events))
        return 0

    raise SystemExit(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(cli())
