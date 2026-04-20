from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from .alerts import DEFAULT_ALERT_WORKER_INTERVAL, run_alert_worker
from .config import Settings
from .db import connect, init_db
from .importer import sync_sessions
from .projects import fetch_session_with_project
from .runtime import export_markdown, get_events, run_sync_daemon
from .session_artifacts import resolve_session_raw_text
from .session_exports import build_session_bundle, export_json_payload


PROJECT_ROOT = Path(__file__).resolve().parent.parent


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

    alerts = subparsers.add_parser("alerts", help="Run the alert reconciliation and delivery worker")
    alerts.add_argument("--interval", type=int)
    alerts.add_argument("--once", action="store_true")

    export = subparsers.add_parser("export", help="Export one imported session")
    export.add_argument("session_id")
    export.add_argument("--format", choices=["json", "markdown", "raw", "bundle"], default="json")
    export.add_argument("--output")

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

        import uvicorn

        from .main import create_app

        uvicorn.run(
            create_app(
                settings,
                preserve_sync_on_start=getattr(args, "no_sync", False),
            ),
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
            rebuild_on_start=getattr(args, "rebuild_on_start", False) or settings.daemon_rebuild_on_start,
        )

    if args.command == "alerts":
        settings.ensure_directories()
        init_db(settings.database_path)
        interval_seconds = getattr(args, "interval", None) or DEFAULT_ALERT_WORKER_INTERVAL
        return run_alert_worker(
            settings,
            interval_seconds=interval_seconds,
            once=bool(getattr(args, "once", False)),
        )

    if args.command == "export":
        settings.ensure_directories()
        init_db(settings.database_path)
        with connect(settings.database_path) as connection:
            session = fetch_session_with_project(connection, args.session_id)
            if session is None:
                raise SystemExit(f"Unknown session: {args.session_id}")
            events = get_events(connection, args.session_id)
            raw_jsonl, raw_export_info = resolve_session_raw_text(connection, settings, session)

        if args.format == "json":
            output = json.dumps(export_json_payload(session, events), indent=2, ensure_ascii=False)
            if args.output:
                Path(args.output).write_text(output + "\n", encoding="utf-8")
            else:
                print(output)
        elif args.format == "markdown":
            output = export_markdown(session, events)
            if args.output:
                Path(args.output).write_text(output, encoding="utf-8")
            else:
                print(output)
        elif args.format == "raw":
            if raw_jsonl is None:
                raise SystemExit(f"Raw rollout is unavailable for session: {args.session_id}")
            if args.output:
                with Path(args.output).open("w", encoding="utf-8", newline="") as handle:
                    handle.write(raw_jsonl)
            else:
                print(raw_jsonl, end="")
        else:
            if raw_jsonl is None:
                raise SystemExit(f"Portable bundle requires a raw rollout artifact for session: {args.session_id}")
            bundle_bytes = build_session_bundle(
                session,
                events,
                raw_jsonl=raw_jsonl,
                raw_export_info=raw_export_info,
            )
            output_path = Path(args.output or f"{args.session_id}.zip")
            output_path.write_bytes(bundle_bytes)
            print(str(output_path))
        if args.format != "bundle" and args.output:
            print(str(Path(args.output)))
        return 0

    raise SystemExit(f"Unsupported command: {args.command}")
