from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import logging
from pathlib import Path

from .alerts import DEFAULT_ALERT_WORKER_INTERVAL, run_alert_worker
from .backup_restore import create_instance_backup, restore_instance_backup, verify_backup_archive
from .config import Settings
from .db import connect, init_db
from .importer import sync_sessions
from .machine_setup import (
    machine_repair,
    machine_setup,
    machine_status,
    machine_unpair,
    pair_machine,
)
from .projects import fetch_session_with_project
from .runtime import export_markdown, get_events, run_sync_daemon
from .session_artifacts import resolve_session_raw_text
from .session_exports import build_session_bundle, export_json_payload
from .service_manager import (
    install_service,
    service_status,
    start_service,
    stop_service,
    uninstall_service,
)


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

    pair = subparsers.add_parser("pair", help="Pair this machine through the browser and store a machine credential")
    pair.add_argument("--label")
    pair.add_argument("--no-browser", action="store_true")
    pair.add_argument("--force", action="store_true")
    pair.add_argument("--timeout", type=int, default=900)

    machine = subparsers.add_parser("machine", help="Inspect or repair local machine pairing")
    machine_subparsers = machine.add_subparsers(dest="machine_command", required=True)

    machine_subparsers.add_parser("status", help="Show local machine pairing and service status")

    machine_repair_cmd = machine_subparsers.add_parser("repair", help="Repair pairing and service state")
    machine_repair_cmd.add_argument("--re-pair", action="store_true")
    machine_repair_cmd.add_argument("--reinstall-service", action="store_true")
    machine_repair_cmd.add_argument("--no-browser", action="store_true")

    machine_unpair_cmd = machine_subparsers.add_parser("unpair", help="Delete the local machine credential")
    machine_unpair_cmd.add_argument("--uninstall-service", action="store_true")

    machine_setup_cmd = machine_subparsers.add_parser("setup", help="Pair this machine and install the daemon service")
    machine_setup_cmd.add_argument("--label")
    machine_setup_cmd.add_argument("--no-browser", action="store_true")

    service = subparsers.add_parser("service", help="Manage the background daemon service")
    service_subparsers = service.add_subparsers(dest="service_command", required=True)
    service_subparsers.add_parser("install", help="Install the background daemon service")
    service_subparsers.add_parser("start", help="Start the background daemon service")
    service_subparsers.add_parser("stop", help="Stop the background daemon service")
    service_subparsers.add_parser("status", help="Show the background daemon service status")
    service_subparsers.add_parser("uninstall", help="Remove the background daemon service")

    alerts = subparsers.add_parser("alerts", help="Run the alert reconciliation and delivery worker")
    alerts.add_argument("--interval", type=int)
    alerts.add_argument("--once", action="store_true")

    export = subparsers.add_parser("export", help="Export one imported session")
    export.add_argument("session_id")
    export.add_argument("--format", choices=["json", "markdown", "raw", "bundle"], default="json")
    export.add_argument("--output")

    backup = subparsers.add_parser("backup", help="Create, verify, or restore whole-instance backups")
    backup_subparsers = backup.add_subparsers(dest="backup_command", required=True)

    backup_create = backup_subparsers.add_parser("create", help="Create a zip archive from the current instance")
    backup_create.add_argument("--output")

    backup_verify = backup_subparsers.add_parser("verify", help="Verify a backup archive")
    backup_verify.add_argument("archive")

    backup_restore = backup_subparsers.add_parser(
        "restore",
        help="Restore a backup archive into a fresh target directory",
    )
    backup_restore.add_argument("archive")
    backup_restore.add_argument("--data-dir", required=True)
    backup_restore.add_argument("--database-path")

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

    if args.command == "pair":
        result = pair_machine(
            settings,
            label=getattr(args, "label", None),
            open_browser=not bool(getattr(args, "no_browser", False)),
            timeout_seconds=int(getattr(args, "timeout", 900) or 900),
            force=bool(getattr(args, "force", False)),
        )
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "machine":
        if args.machine_command == "status":
            print(json.dumps(machine_status(settings), indent=2))
            return 0
        if args.machine_command == "repair":
            result = machine_repair(
                settings,
                re_pair=bool(getattr(args, "re_pair", False)),
                reinstall_service=bool(getattr(args, "reinstall_service", False)),
                open_browser=not bool(getattr(args, "no_browser", False)),
            )
            print(json.dumps(result, indent=2))
            return 0
        if args.machine_command == "unpair":
            result = machine_unpair(
                settings,
                uninstall=bool(getattr(args, "uninstall_service", False)),
            )
            print(json.dumps(result, indent=2))
            return 0
        if args.machine_command == "setup":
            result = machine_setup(
                settings,
                label=getattr(args, "label", None),
                open_browser=not bool(getattr(args, "no_browser", False)),
            )
            print(json.dumps(result, indent=2))
            return 0
        raise SystemExit(f"Unsupported machine command: {args.machine_command}")

    if args.command == "service":
        if args.service_command == "install":
            print(json.dumps(install_service(settings), indent=2))
            return 0
        if args.service_command == "start":
            print(json.dumps(start_service(settings), indent=2))
            return 0
        if args.service_command == "stop":
            print(json.dumps(stop_service(), indent=2))
            return 0
        if args.service_command == "status":
            print(json.dumps(service_status(), indent=2))
            return 0
        if args.service_command == "uninstall":
            print(json.dumps(uninstall_service(), indent=2))
            return 0
        raise SystemExit(f"Unsupported service command: {args.service_command}")

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

    if args.command == "backup":
        if args.backup_command == "create":
            timestamp = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
            default_output = settings.project_root / f"codex-viewer-backup-{timestamp}.zip"
            output_path = Path(args.output).expanduser() if args.output else default_output
            result = create_instance_backup(settings, output_path=output_path)
            print(json.dumps(result, indent=2))
            return 0

        if args.backup_command == "verify":
            result = verify_backup_archive(Path(args.archive).expanduser())
            print(json.dumps(result, indent=2))
            return 0

        if args.backup_command == "restore":
            result = restore_instance_backup(
                Path(args.archive).expanduser(),
                target_data_dir=Path(args.data_dir).expanduser(),
                target_database_path=Path(args.database_path).expanduser() if args.database_path else None,
            )
            print(json.dumps(result, indent=2))
            return 0

    raise SystemExit(f"Unsupported command: {args.command}")
