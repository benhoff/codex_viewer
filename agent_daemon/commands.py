from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from agent_operations_viewer.config import Settings

from .diagnostics import (
    collect_agent_status,
    doctor_agent,
    format_agent_doctor,
    format_agent_status,
    restart_agent_service,
    run_agent_logs,
    run_sync_once,
)
from .machine_setup import machine_repair, machine_setup, machine_unpair, pair_machine
from .remote_sync import RemoteSyncError
from .runtime import run_sync_daemon
from .service_manager import (
    install_service,
    service_status,
    start_service,
    stop_service,
    uninstall_service,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _service_install_summary(result: dict[str, object]) -> list[str]:
    target = str(result.get("target") or "service manager")
    lines = [f"Installed the background daemon service for your user via {target}."]
    unit_path = str(result.get("unit_path") or result.get("plist_path") or "").strip()
    if unit_path:
        lines.append(f"Definition file: {unit_path}")
    if target == "systemd-user":
        lines.append("The service is enabled for future user-session startup.")
    elif target == "launchd":
        lines.append("The launch agent definition is installed for your user account.")
    elif target == "schtasks":
        lines.append("The scheduled task is registered for your user account.")
    lines.append("It is not started yet. Run `python3 -m agent_daemon service start` to start it now.")
    lines.append("Run `python3 -m agent_daemon service status` to confirm install and runtime state.")
    return lines


def _service_start_summary(result: dict[str, object]) -> list[str]:
    target = str(result.get("target") or "service manager")
    return [
        f"Started the background daemon via {target}.",
        "Run `python3 -m agent_daemon service status` to confirm it is running.",
    ]


def _service_stop_summary(result: dict[str, object]) -> list[str]:
    target = str(result.get("target") or "service manager")
    return [
        f"Stopped the background daemon via {target}.",
        "Run `python3 -m agent_daemon service status` to confirm it is no longer running.",
    ]


def _service_status_summary(result: dict[str, object]) -> list[str]:
    target = str(result.get("target") or "service manager")
    installed = "yes" if bool(result.get("installed")) else "no"
    running = "yes" if bool(result.get("running")) else "no"
    lines = [
        f"Background daemon status via {target}: installed={installed}, running={running}.",
    ]
    unit_path = str(result.get("unit_path") or result.get("plist_path") or result.get("task_name") or "").strip()
    if unit_path:
        if target == "schtasks":
            lines.append(f"Task name: {unit_path}")
        else:
            lines.append(f"Definition: {unit_path}")
    return lines


def _service_uninstall_summary(result: dict[str, object]) -> list[str]:
    target = str(result.get("target") or "service manager")
    deleted_label = ""
    if "deleted_unit" in result:
        deleted_label = "unit file"
    elif "deleted_plist" in result:
        deleted_label = "launch agent plist"
    lines = [f"Removed the background daemon service definition for {target}."]
    if deleted_label:
        deleted_state = "deleted" if bool(result.get("deleted_unit") or result.get("deleted_plist")) else "not found"
        lines.append(f"Local {deleted_label}: {deleted_state}.")
    lines.append("Automatic startup is no longer configured for this user.")
    return lines


def _print_service_feedback(summary_lines: list[str]) -> None:
    print("\n".join(summary_lines))


def _clean_url(raw: str | None) -> str | None:
    if raw is None:
        return None
    stripped = raw.strip().rstrip("/")
    return stripped or None


def _apply_common_overrides(settings: Settings, args: argparse.Namespace) -> None:
    server_url = _clean_url(getattr(args, "server", None))
    if server_url:
        settings.server_base_url = server_url


def _print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2))


def _print_setup_feedback(result: dict[str, object]) -> None:
    actions = [str(action) for action in result.get("actions", [])]
    if actions:
        print("Agent setup completed: " + ", ".join(actions) + ".")
    else:
        print("Agent setup checked existing pairing and service state.")
    status = result.get("status")
    if isinstance(status, dict):
        print()
        print(format_agent_status(status))


def _print_pair_feedback(result: dict[str, object]) -> None:
    print("Machine pairing completed.")
    identity_path = str(result.get("identity_path") or "").strip()
    if identity_path:
        print(f"Identity file: {identity_path}")


def _print_repair_feedback(result: dict[str, object]) -> None:
    actions = [str(action) for action in result.get("actions", [])]
    if actions:
        print("Agent repair completed: " + ", ".join(actions) + ".")
    else:
        print("Agent repair found no service changes to apply.")
    status = result.get("status")
    if isinstance(status, dict):
        print()
        print(format_agent_status(status))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run and manage the Agent Operations Viewer background daemon")
    subparsers = parser.add_subparsers(dest="command", required=False)

    daemon = subparsers.add_parser("daemon", help="Run the background sync daemon")
    daemon.add_argument("--interval", type=int)
    daemon.add_argument("--rebuild-on-start", action="store_true")

    setup = subparsers.add_parser("setup", help="Pair this machine, install the service, and start syncing")
    setup.add_argument("--server")
    setup.add_argument("--label")
    setup.add_argument("--no-browser", action="store_true")
    setup.add_argument("--timeout", type=int, default=900)
    setup.add_argument("--force", action="store_true")
    setup.add_argument("--reinstall-service", action="store_true")
    setup.add_argument("--json", action="store_true")

    pair = subparsers.add_parser("pair", help="Pair this machine through the browser")
    pair.add_argument("--server")
    pair.add_argument("--label")
    pair.add_argument("--no-browser", action="store_true")
    pair.add_argument("--force", action="store_true")
    pair.add_argument("--timeout", type=int, default=900)
    pair.add_argument("--json", action="store_true")

    status = subparsers.add_parser("status", help="Show agent pairing, service, server, and source status")
    status.add_argument("--json", action="store_true")

    doctor = subparsers.add_parser("doctor", help="Run agent diagnostics and print exact next actions")
    doctor.add_argument("--json", action="store_true")

    sync = subparsers.add_parser("sync", help="Run one remote sync pass in the foreground")
    sync.add_argument("--once", action="store_true")
    sync.add_argument("--force", action="store_true")
    sync.add_argument("--json", action="store_true")

    logs = subparsers.add_parser("logs", help="Show daemon logs for the current platform")
    logs.add_argument("--follow", action="store_true")
    logs.add_argument("--lines", type=int, default=100)

    repair = subparsers.add_parser("repair", help="Repair pairing and background service state")
    repair.add_argument("--server")
    repair.add_argument("--re-pair", action="store_true")
    repair.add_argument("--reinstall-service", action="store_true")
    repair.add_argument("--no-browser", action="store_true")
    repair.add_argument("--json", action="store_true")

    subparsers.add_parser("install", help="Install the background daemon service")
    subparsers.add_parser("start", help="Start the background daemon service")
    subparsers.add_parser("stop", help="Stop the background daemon service")
    subparsers.add_parser("restart", help="Restart the background daemon service")

    uninstall = subparsers.add_parser("uninstall", help="Remove the background daemon service")
    uninstall.add_argument("--unpair", action="store_true")
    uninstall.add_argument("--json", action="store_true")

    unpair = subparsers.add_parser("unpair", help="Delete the local machine credential")
    unpair.add_argument("--uninstall-service", action="store_true")
    unpair.add_argument("--json", action="store_true")

    service = subparsers.add_parser("service", help="Manage the background daemon service")
    service_subparsers = service.add_subparsers(dest="service_command", required=True)
    service_subparsers.add_parser("install", help="Install the background daemon service")
    service_subparsers.add_parser("start", help="Start the background daemon service")
    service_subparsers.add_parser("stop", help="Stop the background daemon service")
    service_subparsers.add_parser("status", help="Show the background daemon service status")
    service_subparsers.add_parser("uninstall", help="Remove the background daemon service")

    return parser.parse_args()


def cli() -> int:
    args = parse_args()
    settings = Settings.from_env(PROJECT_ROOT)
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.command in {None, "daemon"}:
        interval_seconds = getattr(args, "interval", None) or settings.sync_interval_seconds
        return run_sync_daemon(
            settings,
            interval_seconds=interval_seconds,
            rebuild_on_start=getattr(args, "rebuild_on_start", False) or settings.daemon_rebuild_on_start,
        )

    _apply_common_overrides(settings, args)

    if args.command == "setup":
        result = machine_setup(
            settings,
            label=getattr(args, "label", None),
            open_browser=not bool(getattr(args, "no_browser", False)),
            timeout_seconds=int(getattr(args, "timeout", 900) or 900),
            force=bool(getattr(args, "force", False)),
            reinstall_service=bool(getattr(args, "reinstall_service", False)),
        )
        if getattr(args, "json", False):
            _print_json(result)
        else:
            _print_setup_feedback(result)
        return 0

    if args.command == "pair":
        result = pair_machine(
            settings,
            label=getattr(args, "label", None),
            open_browser=not bool(getattr(args, "no_browser", False)),
            timeout_seconds=int(getattr(args, "timeout", 900) or 900),
            force=bool(getattr(args, "force", False)),
        )
        if getattr(args, "json", False):
            _print_json(result)
        else:
            _print_pair_feedback(result)
        return 0

    if args.command == "status":
        result = collect_agent_status(settings)
        if getattr(args, "json", False):
            _print_json(result)
        else:
            print(format_agent_status(result))
        return 0

    if args.command == "doctor":
        result = doctor_agent(settings)
        if getattr(args, "json", False):
            _print_json(result)
        else:
            print(format_agent_doctor(result))
        return 0 if bool(result.get("ok")) else 1

    if args.command == "sync":
        try:
            stats = run_sync_once(settings, force=bool(getattr(args, "force", False)))
        except RemoteSyncError as exc:
            raise SystemExit(f"Remote sync failed: {exc}") from exc
        if getattr(args, "json", False):
            _print_json(stats)
        else:
            print(
                "Sync pass finished: "
                f"uploaded={stats.get('uploaded', 0)}, "
                f"skipped={stats.get('skipped', 0)}, "
                f"failed={stats.get('failed', 0)}."
            )
        return 1 if int(stats.get("failed", 0)) > 0 else 0

    if args.command == "logs":
        return run_agent_logs(
            settings,
            lines=int(getattr(args, "lines", 100) or 100),
            follow=bool(getattr(args, "follow", False)),
        )

    if args.command == "repair":
        result = machine_repair(
            settings,
            re_pair=bool(getattr(args, "re_pair", False)),
            reinstall_service=bool(getattr(args, "reinstall_service", False)),
            open_browser=not bool(getattr(args, "no_browser", False)),
        )
        if getattr(args, "json", False):
            _print_json(result)
        else:
            _print_repair_feedback(result)
        return 0

    if args.command == "install":
        result = install_service(settings)
        _print_service_feedback(_service_install_summary(result))
        return 0

    if args.command == "start":
        result = start_service(settings)
        _print_service_feedback(_service_start_summary(result))
        return 0

    if args.command == "stop":
        result = stop_service()
        _print_service_feedback(_service_stop_summary(result))
        return 0

    if args.command == "restart":
        result = restart_agent_service(settings)
        _print_service_feedback(_service_stop_summary(result["stop"]))
        _print_service_feedback(_service_start_summary(result["start"]))
        return 0

    if args.command == "uninstall":
        result = uninstall_service()
        payload: dict[str, object] = {"service": result}
        if bool(getattr(args, "unpair", False)):
            payload["unpair"] = machine_unpair(settings, uninstall=False)
        if getattr(args, "json", False):
            _print_json(payload)
        else:
            _print_service_feedback(_service_uninstall_summary(result))
            if "unpair" in payload:
                print("Local machine credential was removed.")
        return 0

    if args.command == "unpair":
        result = machine_unpair(
            settings,
            uninstall=bool(getattr(args, "uninstall_service", False)),
        )
        if getattr(args, "json", False):
            _print_json(result)
        else:
            deleted = "removed" if result.get("identity_deleted") else "not found"
            print(f"Local machine credential: {deleted}.")
        return 0

    if args.command == "service":
        if args.service_command == "install":
            result = install_service(settings)
            _print_service_feedback(_service_install_summary(result))
            return 0
        if args.service_command == "start":
            result = start_service(settings)
            _print_service_feedback(_service_start_summary(result))
            return 0
        if args.service_command == "stop":
            result = stop_service()
            _print_service_feedback(_service_stop_summary(result))
            return 0
        if args.service_command == "status":
            result = service_status()
            _print_service_feedback(_service_status_summary(result))
            return 0
        if args.service_command == "uninstall":
            result = uninstall_service()
            _print_service_feedback(_service_uninstall_summary(result))
            return 0
        raise SystemExit(f"Unsupported service command: {args.service_command}")

    raise SystemExit(f"Unsupported daemon command: {args.command}")
