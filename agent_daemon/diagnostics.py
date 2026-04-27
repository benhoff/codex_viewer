from __future__ import annotations

import json
import subprocess
from collections import Counter
from dataclasses import replace

from agent_operations_viewer.config import Settings
from agent_operations_viewer.session_parsing import iter_session_files

from .agent_state import connect_agent_state
from .local_machine import load_machine_identity
from .machine_setup import machine_status
from .remote_sync import RemoteSyncError, sync_sessions_remote
from .service_manager import LAUNCHD_LABEL, SYSTEMD_USER_UNIT_NAME, default_service_target, start_service, stop_service


def _is_server_health_ok(health: object) -> bool:
    if not isinstance(health, dict):
        return False
    return health.get("status") == "ok" or health.get("ok") is True


def _source_root_summaries(settings: Settings) -> list[dict[str, object]]:
    counts = Counter(str(root) for root, _path in iter_session_files(settings.session_roots))
    summaries: list[dict[str, object]] = []
    for root in settings.session_roots:
        expanded = root.expanduser()
        summaries.append(
            {
                "path": str(expanded),
                "exists": expanded.exists(),
                "session_files": counts.get(str(expanded), 0),
            }
        )
    return summaries


def _agent_state_summary(settings: Settings) -> dict[str, object]:
    database_path = settings.agent_state_db_path()
    if not database_path.exists():
        return {
            "path": str(database_path),
            "exists": False,
            "tracked": 0,
            "states": {},
            "last_seen_at": None,
            "last_uploaded_at": None,
        }

    with connect_agent_state(database_path) as connection:
        state_rows = connection.execute(
            """
            SELECT state, COUNT(*) AS count
            FROM agent_session_files
            WHERE deleted_at IS NULL
            GROUP BY state
            ORDER BY state
            """
        ).fetchall()
        rollup = connection.execute(
            """
            SELECT
                COUNT(*) AS tracked,
                MAX(last_seen_at) AS last_seen_at,
                MAX(last_uploaded_at) AS last_uploaded_at
            FROM agent_session_files
            WHERE deleted_at IS NULL
            """
        ).fetchone()
    return {
        "path": str(database_path),
        "exists": True,
        "tracked": int(rollup["tracked"] or 0) if rollup is not None else 0,
        "states": {str(row["state"]): int(row["count"] or 0) for row in state_rows},
        "last_seen_at": str(rollup["last_seen_at"]) if rollup is not None and rollup["last_seen_at"] else None,
        "last_uploaded_at": str(rollup["last_uploaded_at"])
        if rollup is not None and rollup["last_uploaded_at"]
        else None,
    }


def collect_agent_status(settings: Settings) -> dict[str, object]:
    status = machine_status(settings)
    identity = load_machine_identity(settings)
    return {
        **status,
        "auth_method": "machine" if identity is not None else "token" if settings.sync_api_token else None,
        "source_host": settings.source_host,
        "session_roots": _source_root_summaries(settings),
        "agent_state": _agent_state_summary(settings),
    }


def _format_service(service: object) -> str:
    if not isinstance(service, dict):
        return "unknown"
    target = str(service.get("target") or "service manager")
    installed = "installed" if bool(service.get("installed")) else "not installed"
    running = "running" if bool(service.get("running")) else "stopped"
    return f"{running}, {installed} via {target}"


def _format_auth(status: dict[str, object]) -> str:
    if status.get("paired"):
        identity = status.get("identity")
        label = ""
        if isinstance(identity, dict):
            label = str(identity.get("label") or identity.get("source_host") or "").strip()
        probe = status.get("auth_probe")
        if isinstance(probe, dict) and probe.get("ok") is False:
            return f"paired{f' as {label}' if label else ''}, auth probe failed"
        return f"paired{f' as {label}' if label else ''}"
    if status.get("auth_method") == "token":
        return "token configured"
    return "not configured"


def format_agent_status(status: dict[str, object]) -> str:
    server_url = str(status.get("server_url") or "not configured")
    health = status.get("server_health")
    server_state = "reachable" if _is_server_health_ok(health) else "not reachable"
    roots = [item for item in status.get("session_roots", []) if isinstance(item, dict)]
    total_files = sum(int(item.get("session_files") or 0) for item in roots)
    existing_roots = sum(1 for item in roots if bool(item.get("exists")))
    agent_state = status.get("agent_state") if isinstance(status.get("agent_state"), dict) else {}
    last_uploaded_at = agent_state.get("last_uploaded_at") or "never"

    lines = [
        "Agent daemon status",
        f"Service: {_format_service(status.get('service'))}",
        f"Server: {server_url} ({server_state})",
        f"Auth: {_format_auth(status)}",
        f"Sources: {total_files} session file(s) across {existing_roots}/{len(roots)} existing root(s)",
        f"Last upload: {last_uploaded_at}",
    ]
    states = agent_state.get("states")
    if isinstance(states, dict) and states:
        state_text = ", ".join(f"{key}={value}" for key, value in sorted(states.items()))
        lines.append(f"Tracked state: {state_text}")
    return "\n".join(lines)


def doctor_agent(settings: Settings) -> dict[str, object]:
    status = collect_agent_status(settings)
    checks: list[dict[str, object]] = []
    next_actions: list[str] = []

    def add_check(name: str, ok: bool, message: str, action: str | None = None) -> None:
        checks.append({"name": name, "ok": ok, "message": message})
        if not ok and action and action not in next_actions:
            next_actions.append(action)

    server_url = str(status.get("server_url") or "").strip()
    add_check(
        "server_url",
        bool(server_url),
        f"Server URL is {server_url}." if server_url else "No server URL is configured.",
        "Set CODEX_VIEWER_SERVER_URL or run `python -m agent_daemon setup --server URL`.",
    )

    health = status.get("server_health")
    if server_url:
        add_check(
            "server_health",
            _is_server_health_ok(health),
            "Server health endpoint is reachable."
            if _is_server_health_ok(health)
            else f"Server health check failed: {json.dumps(health, sort_keys=True)}",
            "Check the viewer URL and network path from this machine.",
        )
    else:
        add_check(
            "server_health",
            False,
            "Server health check skipped because no server URL is configured.",
            "Set CODEX_VIEWER_SERVER_URL or run `python -m agent_daemon setup --server URL`.",
        )

    has_auth = bool(status.get("paired")) or status.get("auth_method") == "token"
    add_check(
        "auth_config",
        has_auth,
        f"Auth method: {status.get('auth_method')}." if has_auth else "No machine credential or sync token is configured.",
        "Run `python -m agent_daemon setup` to pair this machine.",
    )

    probe = status.get("auth_probe")
    if isinstance(probe, dict):
        add_check(
            "auth_probe",
            probe.get("ok") is True,
            "Machine auth can fetch the remote manifest."
            if probe.get("ok") is True
            else f"Machine auth probe failed: {probe.get('error') or 'unknown error'}",
            "Run `python -m agent_daemon repair --re-pair` if the credential was revoked or points at the wrong server.",
        )

    service = status.get("service") if isinstance(status.get("service"), dict) else {}
    add_check(
        "service_installed",
        bool(service.get("installed")),
        "Background service is installed." if service.get("installed") else "Background service is not installed.",
        "Run `python -m agent_daemon setup` or `python -m agent_daemon repair --reinstall-service`.",
    )
    add_check(
        "service_running",
        bool(service.get("running")),
        "Background service is running." if service.get("running") else "Background service is stopped.",
        "Run `python -m agent_daemon start`.",
    )

    roots = [item for item in status.get("session_roots", []) if isinstance(item, dict)]
    existing_roots = [item for item in roots if bool(item.get("exists"))]
    session_file_count = sum(int(item.get("session_files") or 0) for item in roots)
    add_check(
        "session_roots",
        bool(existing_roots),
        f"{len(existing_roots)} configured session root(s) exist."
        if existing_roots
        else "No configured session roots exist.",
        "Set CODEX_SESSION_ROOTS to the local Codex or Claude session directory.",
    )
    add_check(
        "session_files",
        session_file_count > 0,
        f"Found {session_file_count} session file(s)." if session_file_count else "No session files were found.",
        "Run Codex or set CODEX_SESSION_ROOTS to the directory containing session JSONL files.",
    )

    ok = all(bool(check["ok"]) for check in checks)
    return {
        "ok": ok,
        "checks": checks,
        "next_actions": next_actions,
        "status": status,
    }


def format_agent_doctor(result: dict[str, object]) -> str:
    lines = ["Agent daemon doctor"]
    for check in result.get("checks", []):
        if not isinstance(check, dict):
            continue
        state = "ok" if check.get("ok") else "fail"
        lines.append(f"{state}: {check.get('name')}: {check.get('message')}")
    actions = [str(item) for item in result.get("next_actions", []) if str(item).strip()]
    if actions:
        lines.append("")
        lines.append("Next actions:")
        lines.extend(f"- {action}" for action in actions)
    else:
        lines.append("")
        lines.append("No action needed.")
    return "\n".join(lines)


def run_sync_once(settings: Settings, *, force: bool = False) -> dict[str, int]:
    try:
        return sync_sessions_remote(replace(settings, sync_mode="remote"), force=force)
    except RemoteSyncError:
        raise


def restart_agent_service(settings: Settings) -> dict[str, object]:
    stop_result = stop_service()
    start_result = start_service(settings)
    return {
        "stop": stop_result,
        "start": start_result,
    }


def run_agent_logs(settings: Settings, *, lines: int = 100, follow: bool = False) -> int:
    target = default_service_target()
    safe_lines = str(max(1, lines))
    if target == "systemd-user":
        command = ["journalctl", "--user", "-u", SYSTEMD_USER_UNIT_NAME, "-n", safe_lines, "--no-pager"]
        if follow:
            command.append("--follow")
        return subprocess.call(command)

    if target == "launchd":
        log_paths = [
            settings.data_dir / "agent-daemon.stdout.log",
            settings.data_dir / "agent-daemon.stderr.log",
        ]
        existing = [str(path) for path in log_paths if path.exists()]
        if not existing:
            print(f"No daemon log files found for {LAUNCHD_LABEL} in {settings.data_dir}.")
            return 1
        command = ["tail", "-n", safe_lines]
        if follow:
            command.append("-f")
        command.extend(existing)
        return subprocess.call(command)

    print("Windows Task Scheduler does not expose a unified daemon log stream here.")
    print(f"Task status is available through `python -m agent_daemon status` for {target}.")
    return 1
