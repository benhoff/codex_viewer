from __future__ import annotations

import argparse
import logging
from pathlib import Path

from agent_operations_viewer.config import Settings

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run and manage the Agent Operations Viewer background daemon")
    subparsers = parser.add_subparsers(dest="command", required=False)

    daemon = subparsers.add_parser("daemon", help="Run the background sync daemon")
    daemon.add_argument("--interval", type=int)
    daemon.add_argument("--rebuild-on-start", action="store_true")

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
