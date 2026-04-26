from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from agent_operations_viewer.config import Settings


SYSTEMD_USER_UNIT_NAME = "agent-operations-viewer-agent.service"
LAUNCHD_LABEL = "com.agentoperationsviewer.agent"
WINDOWS_TASK_NAME = "AgentOperationsViewerAgent"


@dataclass(slots=True)
class ServiceCommandResult:
    ok: bool
    command: list[str]
    returncode: int
    stdout: str
    stderr: str


def _command_payload(result: ServiceCommandResult) -> dict[str, object]:
    return asdict(result)


def default_service_target() -> str:
    if sys.platform == "darwin":
        return "launchd"
    if sys.platform.startswith("win"):
        return "schtasks"
    return "systemd-user"


def _run_command(command: list[str], *, check: bool = False) -> ServiceCommandResult:
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        result = ServiceCommandResult(
            ok=completed.returncode == 0,
            command=command,
            returncode=completed.returncode,
            stdout=completed.stdout.strip(),
            stderr=completed.stderr.strip(),
        )
    except FileNotFoundError as exc:
        result = ServiceCommandResult(
            ok=False,
            command=command,
            returncode=127,
            stdout="",
            stderr=str(exc),
        )
    if check and not result.ok:
        detail = result.stderr or result.stdout or "command failed"
        raise RuntimeError(f"{' '.join(command)}: {detail}")
    return result


def _linux_unit_path() -> Path:
    return Path.home() / ".config" / "systemd" / "user" / SYSTEMD_USER_UNIT_NAME


def _linux_unit_contents(settings: Settings) -> str:
    script_path = settings.project_root / "scripts" / "start-agent-daemon.sh"
    return "\n".join(
        [
            "[Unit]",
            "Description=Agent Operations Viewer agent daemon",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=simple",
            f"WorkingDirectory={settings.project_root}",
            f"ExecStart={script_path}",
            "Restart=on-failure",
            "RestartSec=5",
            "TimeoutStopSec=20",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )


def _mac_plist_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"


def _mac_plist_bytes(settings: Settings) -> bytes:
    script_path = settings.project_root / "scripts" / "start-agent-daemon.sh"
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "Label": LAUNCHD_LABEL,
        "ProgramArguments": ["/bin/bash", str(script_path)],
        "WorkingDirectory": str(settings.project_root),
        "RunAtLoad": False,
        "KeepAlive": True,
        "StandardOutPath": str(settings.data_dir / "agent-daemon.stdout.log"),
        "StandardErrorPath": str(settings.data_dir / "agent-daemon.stderr.log"),
    }
    return plistlib.dumps(payload, sort_keys=True)


def _windows_task_command(settings: Settings) -> str:
    script_path = settings.project_root / "scripts" / "start-agent-daemon.ps1"
    return (
        'powershell.exe -NoProfile -ExecutionPolicy Bypass '
        f'-File "{script_path}"'
    )


def install_service(settings: Settings, *, target: str | None = None) -> dict[str, object]:
    effective_target = target or default_service_target()
    if effective_target == "systemd-user":
        unit_path = _linux_unit_path()
        unit_path.parent.mkdir(parents=True, exist_ok=True)
        unit_path.write_text(_linux_unit_contents(settings), encoding="utf-8")
        daemon_reload = _run_command(["systemctl", "--user", "daemon-reload"])
        enable = _run_command(["systemctl", "--user", "enable", SYSTEMD_USER_UNIT_NAME])
        return {
            "target": effective_target,
            "unit_path": str(unit_path),
            "daemon_reload": _command_payload(daemon_reload),
            "enable": _command_payload(enable),
        }
    if effective_target == "launchd":
        plist_path = _mac_plist_path()
        plist_path.parent.mkdir(parents=True, exist_ok=True)
        plist_path.write_bytes(_mac_plist_bytes(settings))
        bootout = _run_command(
            ["launchctl", "bootout", f"gui/{os.getuid()}", str(plist_path)]
        )
        bootstrap = _run_command(
            ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(plist_path)]
        )
        return {
            "target": effective_target,
            "plist_path": str(plist_path),
            "bootout": _command_payload(bootout),
            "bootstrap": _command_payload(bootstrap),
        }
    if effective_target == "schtasks":
        create = _run_command(
            [
                "schtasks",
                "/create",
                "/f",
                "/sc",
                "ONLOGON",
                "/tn",
                WINDOWS_TASK_NAME,
                "/tr",
                _windows_task_command(settings),
            ]
        )
        return {
            "target": effective_target,
            "task_name": WINDOWS_TASK_NAME,
            "create": _command_payload(create),
        }
    raise RuntimeError(f"Unsupported service target: {effective_target}")


def start_service(settings: Settings, *, target: str | None = None) -> dict[str, object]:
    effective_target = target or default_service_target()
    if effective_target == "systemd-user":
        result = _run_command(["systemctl", "--user", "start", SYSTEMD_USER_UNIT_NAME])
        return {"target": effective_target, "start": _command_payload(result)}
    if effective_target == "launchd":
        plist_path = _mac_plist_path()
        bootstrap = _run_command(
            ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(plist_path)]
        )
        kickstart = _run_command(
            ["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/{LAUNCHD_LABEL}"]
        )
        return {
            "target": effective_target,
            "bootstrap": _command_payload(bootstrap),
            "kickstart": _command_payload(kickstart),
        }
    if effective_target == "schtasks":
        result = _run_command(["schtasks", "/run", "/tn", WINDOWS_TASK_NAME])
        return {"target": effective_target, "start": _command_payload(result)}
    raise RuntimeError(f"Unsupported service target: {effective_target}")


def stop_service(*, target: str | None = None) -> dict[str, object]:
    effective_target = target or default_service_target()
    if effective_target == "systemd-user":
        result = _run_command(["systemctl", "--user", "stop", SYSTEMD_USER_UNIT_NAME])
        return {"target": effective_target, "stop": _command_payload(result)}
    if effective_target == "launchd":
        plist_path = _mac_plist_path()
        result = _run_command(
            ["launchctl", "bootout", f"gui/{os.getuid()}", str(plist_path)]
        )
        return {"target": effective_target, "stop": _command_payload(result)}
    if effective_target == "schtasks":
        result = _run_command(["schtasks", "/end", "/tn", WINDOWS_TASK_NAME])
        return {"target": effective_target, "stop": _command_payload(result)}
    raise RuntimeError(f"Unsupported service target: {effective_target}")


def service_status(*, target: str | None = None) -> dict[str, object]:
    effective_target = target or default_service_target()
    if effective_target == "systemd-user":
        active = _run_command(["systemctl", "--user", "is-active", SYSTEMD_USER_UNIT_NAME])
        enabled = _run_command(["systemctl", "--user", "is-enabled", SYSTEMD_USER_UNIT_NAME])
        return {
            "target": effective_target,
            "installed": enabled.ok,
            "running": active.ok and active.stdout == "active",
            "active": _command_payload(active),
            "enabled": _command_payload(enabled),
            "unit_path": str(_linux_unit_path()),
        }
    if effective_target == "launchd":
        result = _run_command(
            ["launchctl", "print", f"gui/{os.getuid()}/{LAUNCHD_LABEL}"]
        )
        output = "\n".join(part for part in [result.stdout, result.stderr] if part)
        return {
            "target": effective_target,
            "installed": result.ok,
            "running": "state = running" in output,
            "detail": _command_payload(result),
            "plist_path": str(_mac_plist_path()),
        }
    if effective_target == "schtasks":
        result = _run_command(
            ["schtasks", "/query", "/tn", WINDOWS_TASK_NAME, "/fo", "LIST", "/v"]
        )
        output = "\n".join(part for part in [result.stdout, result.stderr] if part)
        return {
            "target": effective_target,
            "installed": result.ok,
            "running": "Status: Running" in output,
            "detail": _command_payload(result),
            "task_name": WINDOWS_TASK_NAME,
        }
    raise RuntimeError(f"Unsupported service target: {effective_target}")


def uninstall_service(*, target: str | None = None) -> dict[str, object]:
    effective_target = target or default_service_target()
    if effective_target == "systemd-user":
        disable = _run_command(["systemctl", "--user", "disable", "--now", SYSTEMD_USER_UNIT_NAME])
        unit_path = _linux_unit_path()
        deleted = False
        if unit_path.exists():
            unit_path.unlink()
            deleted = True
        daemon_reload = _run_command(["systemctl", "--user", "daemon-reload"])
        return {
            "target": effective_target,
            "disable": _command_payload(disable),
            "daemon_reload": _command_payload(daemon_reload),
            "deleted_unit": deleted,
        }
    if effective_target == "launchd":
        plist_path = _mac_plist_path()
        bootout = _run_command(
            ["launchctl", "bootout", f"gui/{os.getuid()}", str(plist_path)]
        )
        deleted = False
        if plist_path.exists():
            plist_path.unlink()
            deleted = True
        return {
            "target": effective_target,
            "bootout": _command_payload(bootout),
            "deleted_plist": deleted,
        }
    if effective_target == "schtasks":
        delete = _run_command(["schtasks", "/delete", "/f", "/tn", WINDOWS_TASK_NAME])
        return {
            "target": effective_target,
            "delete": _command_payload(delete),
        }
    raise RuntimeError(f"Unsupported service target: {effective_target}")
