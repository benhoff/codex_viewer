from __future__ import annotations

import copy
import hashlib
import re
import shlex
import sqlite3
from collections import Counter, OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from .agents import fetch_remote_agent_health
from .projects import (
    ProjectAccessContext,
    build_grouped_projects,
    effective_project_fields,
    filter_rows_for_project_access,
    project_detail_href_for_route,
    project_route_segments,
    query_group_rows,
    query_group_rows_for_key,
    trimmed,
)


SHELL_WRAPPERS = {"bash", "sh", "zsh", "dash", "fish"}
PASS_THROUGH_WRAPPERS = {"sudo", "command", "builtin", "stdbuf", "nice", "nohup", "time", "timeout"}
BASELINE_BINARIES = {
    "bash",
    "sh",
    "zsh",
    "dash",
    "fish",
    "sed",
    "rg",
    "git",
    "grep",
    "awk",
    "nl",
    "ls",
    "find",
    "cat",
    "head",
    "tail",
    "pwd",
    "echo",
    "mkdir",
    "mv",
    "cp",
    "rm",
    "touch",
    "xargs",
}
PROFILE_DEFINITIONS = [
    {
        "key": "baseline_dev",
        "label": "Baseline dev",
        "required": ["git", "rg", "sed", "find", "bash", "python3", "curl"],
        "optional": ["sqlite3", "jq"],
    },
    {
        "key": "frontend",
        "label": "Frontend",
        "required": ["node", "npm"],
        "optional": ["pnpm", "yarn", "playwright", "pdftotext"],
    },
    {
        "key": "backend",
        "label": "Backend",
        "required": ["python3", "git"],
        "optional": ["pytest", "sqlite3", "make", "docker"],
    },
    {
        "key": "middleware",
        "label": "Middleware",
        "required": ["curl"],
        "optional": ["jq", "yq", "grpcurl", "kcat", "redis-cli", "psql", "mysql", "sqlite3"],
    },
    {
        "key": "machine_learning",
        "label": "Machine learning",
        "required": ["python3"],
        "optional": ["pip", "uv", "git-lfs", "nvidia-smi", "jupyter"],
    },
    {
        "key": "data_analysis",
        "label": "Data analysis",
        "required": ["python3", "sqlite3"],
        "optional": ["duckdb", "jq", "csvkit"],
    },
    {
        "key": "sysadmin",
        "label": "Sysadmin",
        "required": ["bash"],
        "optional": ["systemctl", "journalctl", "ss", "lsof", "dig", "tcpdump", "ethtool", "docker", "podman"],
    },
    {
        "key": "embedded",
        "label": "Embedded / hardware",
        "required": ["cmake", "make"],
        "optional": ["gdb", "arm-none-eabi-gcc", "kicad-cli"],
    },
]

FAILURE_CLASS_META = {
    "command_not_found": {"label": "Command not found", "tone": "rose", "guidance": "Install"},
    "permission_denied": {"label": "Permission denied", "tone": "amber", "guidance": "Access"},
    "missing_device": {"label": "Missing device", "tone": "amber", "guidance": "Access"},
    "auth_failure": {"label": "Auth / credentials", "tone": "amber", "guidance": "Auth"},
    "timeout": {"label": "Timeout", "tone": "amber", "guidance": "Runtime"},
    "network_failure": {"label": "Network failure", "tone": "amber", "guidance": "Access"},
    "repo_state_missing": {"label": "Project setup", "tone": "stone", "guidance": "Project setup"},
    "unknown": {"label": "Other failure", "tone": "stone", "guidance": "Inspect"},
}

PROJECT_ENVIRONMENT_AUDIT_CACHE_MAXSIZE = 32
PROJECT_ENVIRONMENT_AUDIT_CACHE: OrderedDict[
    tuple[str, str],
    tuple[str, dict[str, Any]],
] = OrderedDict()
ENVIRONMENT_ROLLUP_VERSION = 1


@dataclass(slots=True)
class ParsedCommand:
    binary: str
    command_family: str
    display_command: str
    wrapper_label: str | None = None


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _row_value(row: sqlite3.Row | dict[str, Any] | object, key: str) -> Any:
    if isinstance(row, sqlite3.Row):
        try:
            return row[key]
        except (IndexError, KeyError):
            return None
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


def _normalized_string(value: object) -> str:
    return str(value or "").strip()


def _normalize_environment_events(
    events: Sequence[sqlite3.Row | dict[str, Any] | object],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for event in events:
        normalized.append(
            {
                "session_id": _row_value(event, "session_id"),
                "event_index": _row_value(event, "event_index"),
                "timestamp": _row_value(event, "timestamp"),
                "command_text": _row_value(event, "command_text"),
                "exit_code": _row_value(event, "exit_code"),
                "detail_text": _row_value(event, "detail_text"),
            }
        )
    return normalized


def _is_env_assignment(token: str) -> bool:
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*=.*$", token))


def _shell_split(text: str) -> list[str]:
    try:
        return shlex.split(text, posix=True)
    except ValueError:
        return text.split()


def _normalize_binary_name(token: str) -> str:
    name = token.rsplit("/", 1)[-1].strip().strip("'\"`")
    if not name:
        return "unknown"
    if re.fullmatch(r"python\d+(?:\.\d+)?", name):
        return "python3"
    return name.lower()


def _parse_tokens(tokens: list[str], *, wrapper_label: str | None = None, depth: int = 0) -> ParsedCommand:
    if depth > 4 or not tokens:
        return ParsedCommand(binary="unknown", command_family="unknown", display_command="", wrapper_label=wrapper_label)

    index = 0
    while index < len(tokens) and _is_env_assignment(tokens[index]):
        index += 1
    if index >= len(tokens):
        return ParsedCommand(binary="unknown", command_family="unknown", display_command="", wrapper_label=wrapper_label)

    token = tokens[index]
    token_name = _normalize_binary_name(token)
    if token_name == "env":
        index += 1
        while index < len(tokens):
            current = tokens[index]
            if current in {"-u", "--unset"}:
                index += 2
                continue
            if current.startswith("-") or _is_env_assignment(current):
                index += 1
                continue
            break
        return _parse_tokens(tokens[index:], wrapper_label="env" if wrapper_label is None else wrapper_label, depth=depth + 1)

    if token_name in PASS_THROUGH_WRAPPERS:
        index += 1
        while index < len(tokens):
            current = tokens[index]
            if token_name == "timeout" and re.fullmatch(r"[\d.]+[smhd]?", current):
                index += 1
                continue
            if current == "--":
                index += 1
                break
            if current.startswith("-"):
                index += 1
                continue
            break
        return _parse_tokens(
            tokens[index:],
            wrapper_label=token_name if wrapper_label is None else wrapper_label,
            depth=depth + 1,
        )

    if token_name in SHELL_WRAPPERS:
        for flag in ("-lc", "-c"):
            if flag in tokens[index + 1 :]:
                flag_index = tokens.index(flag, index + 1)
                if flag_index + 1 < len(tokens):
                    inner_text = tokens[flag_index + 1]
                    parsed_inner = parse_command(inner_text)
                    parsed_inner.wrapper_label = f"{token_name} {flag}"
                    return parsed_inner
        display = " ".join(tokens[index : index + 2]).strip()
        binary = token_name
        return ParsedCommand(binary=binary, command_family=binary, display_command=display, wrapper_label=wrapper_label)

    binary = token_name
    display = " ".join(tokens[index:]).strip()
    return ParsedCommand(binary=binary, command_family=binary, display_command=display, wrapper_label=wrapper_label)


def parse_command(command_text: str | None) -> ParsedCommand:
    text = str(command_text or "").strip()
    if not text:
        return ParsedCommand(binary="unknown", command_family="unknown", display_command="")
    invocation_line = text.splitlines()[0].strip()
    tokens = _shell_split(invocation_line)
    return _parse_tokens(tokens)


def classify_failure(command_text: str | None, exit_code: int | None, detail_text: str | None) -> dict[str, Any] | None:
    code = int(exit_code or 0)
    if code == 0:
        return None

    parsed = parse_command(command_text)
    detail = str(detail_text or "")
    detail_lower = detail.lower()
    command_lower = str(command_text or "").lower()
    haystack = f"{command_lower}\n{detail_lower}"

    failure_class = "unknown"
    capability_key = f"binary:{parsed.binary}"
    subject_label = parsed.binary
    status = "warning"

    if "command not found" in haystack or code == 127:
        failure_class = "command_not_found"
        status = "missing"
    elif "/dev/" in haystack and ("no such file or directory" in haystack or "cannot access" in haystack):
        failure_class = "missing_device"
        match = re.search(r"(/dev/[A-Za-z0-9._/-]+)", detail)
        device_path = match.group(1) if match else "/dev/device"
        capability_key = f"device:{device_path.split('/', 3)[2] if device_path.count('/') >= 2 else device_path}"
        subject_label = device_path
        status = "blocked"
    elif "permission denied" in haystack or code == 126:
        failure_class = "permission_denied"
        if "/var/run/docker.sock" in haystack or "docker.sock" in haystack:
            capability_key = "access:docker_socket"
            subject_label = "Docker socket"
        elif "/dev/" in haystack:
            match = re.search(r"(/dev/[A-Za-z0-9._/-]+)", detail)
            device_path = match.group(1) if match else "/dev/device"
            capability_key = f"device:{device_path.split('/', 3)[2] if device_path.count('/') >= 2 else device_path}"
            subject_label = device_path
        status = "blocked"
    elif any(marker in haystack for marker in ("permission denied (publickey)", "authentication failed", "not logged in", "401 unauthorized", "403 forbidden", "could not read username")):
        failure_class = "auth_failure"
        if parsed.binary in {"git", "gh"}:
            capability_key = "auth:github"
            subject_label = "GitHub auth"
        else:
            capability_key = f"auth:{parsed.binary}"
            subject_label = f"{parsed.binary} auth"
        status = "blocked"
    elif any(marker in haystack for marker in ("timed out", "timeout", "deadline exceeded")) or code == 124:
        failure_class = "timeout"
    elif any(marker in haystack for marker in ("could not resolve host", "temporary failure in name resolution", "network is unreachable", "connection refused", "failed to connect")):
        failure_class = "network_failure"
        capability_key = "network:outbound"
        subject_label = "Outbound network"
        status = "blocked"
    elif "no such file or directory" in haystack or "not a git repository" in haystack:
        failure_class = "repo_state_missing"
        path_match = re.search(r"([~/.A-Za-z0-9_-]+/(?:\.venv|node_modules|dist|build|target|out)[^:\s]*)", detail)
        if path_match:
            subject_label = path_match.group(1)
            capability_key = f"path:{subject_label}"
        status = "setup"

    meta = FAILURE_CLASS_META[failure_class]
    return {
        "class_key": failure_class,
        "class_label": meta["label"],
        "tone": meta["tone"],
        "guidance_kind": meta["guidance"],
        "status": status,
        "capability_key": capability_key,
        "subject_label": subject_label,
    }


def _visible_session_rows_for_host(
    connection: sqlite3.Connection,
    source_host: str,
    *,
    project_access: ProjectAccessContext | None = None,
) -> list[sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT
            s.*,
            p.id AS project_id,
            p.visibility AS project_visibility,
            o.override_group_key,
            o.override_organization,
            o.override_repository,
            o.override_remote_url,
            o.override_display_label
        FROM sessions AS s
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        LEFT JOIN project_sources AS ps
            ON ps.match_project_key = s.inferred_project_key
        LEFT JOIN projects AS p
            ON p.id = ps.project_id
        WHERE s.source_host = ?
          AND NOT EXISTS (
            SELECT 1
            FROM ignored_project_sources AS i
            WHERE i.match_project_key = s.inferred_project_key
          )
        ORDER BY COALESCE(s.last_turn_timestamp, s.session_timestamp, s.started_at, s.imported_at) DESC
        """,
        (source_host,),
    ).fetchall()
    return filter_rows_for_project_access(rows, project_access)


def _visible_session_rows_for_project(
    connection: sqlite3.Connection,
    group_key: str,
    *,
    project_access: ProjectAccessContext | None = None,
) -> list[sqlite3.Row]:
    return query_group_rows_for_key(connection, group_key, project_access=project_access)


def _database_cache_key(connection: sqlite3.Connection) -> str:
    rows = connection.execute("PRAGMA database_list").fetchall()
    for row in rows:
        name = str(row["name"] if "name" in row.keys() else row[1])
        if name == "main":
            return str(row["file"] if "file" in row.keys() else row[2] or ":memory:")
    if rows:
        row = rows[0]
        return str(row["file"] if "file" in row.keys() else row[2] or ":memory:")
    return ":memory:"


def _project_audit_fingerprint(
    group_key: str,
    session_rows: list[sqlite3.Row],
) -> str:
    hasher = hashlib.sha1()
    hasher.update(group_key.encode("utf-8"))
    for row in session_rows:
        hasher.update(
            "|".join(
                (
                    str(row["id"] or ""),
                    str(row["updated_at"] or ""),
                    str(_row_value(row, "environment_rollup_version") or 0),
                    str(row["source_host"] or ""),
                    str(row["inferred_project_key"] or ""),
                    str(row["override_updated_at"] or ""),
                    str(row["override_group_key"] or ""),
                    str(row["override_organization"] or ""),
                    str(row["override_repository"] or ""),
                    str(row["override_remote_url"] or ""),
                    str(row["override_display_label"] or ""),
                )
            ).encode("utf-8")
        )
    return hasher.hexdigest()


def _get_cached_project_environment_audit(
    connection: sqlite3.Connection,
    group_key: str,
    fingerprint: str,
) -> dict[str, Any] | None:
    cache_key = (_database_cache_key(connection), group_key)
    cached = PROJECT_ENVIRONMENT_AUDIT_CACHE.get(cache_key)
    if cached is None or cached[0] != fingerprint:
        return None
    PROJECT_ENVIRONMENT_AUDIT_CACHE.move_to_end(cache_key)
    return copy.deepcopy(cached[1])


def _store_cached_project_environment_audit(
    connection: sqlite3.Connection,
    group_key: str,
    fingerprint: str,
    audit: dict[str, Any],
) -> dict[str, Any]:
    cache_key = (_database_cache_key(connection), group_key)
    cached_audit = copy.deepcopy(audit)
    PROJECT_ENVIRONMENT_AUDIT_CACHE[cache_key] = (fingerprint, cached_audit)
    PROJECT_ENVIRONMENT_AUDIT_CACHE.move_to_end(cache_key)
    while len(PROJECT_ENVIRONMENT_AUDIT_CACHE) > PROJECT_ENVIRONMENT_AUDIT_CACHE_MAXSIZE:
        PROJECT_ENVIRONMENT_AUDIT_CACHE.popitem(last=False)
    return copy.deepcopy(cached_audit)


def _fetch_environment_session_rows(
    connection: sqlite3.Connection,
    session_ids: list[str],
) -> dict[str, sqlite3.Row]:
    normalized_ids = [session_id for session_id in session_ids if _normalized_string(session_id)]
    if not normalized_ids:
        return {}
    placeholders = ", ".join("?" for _ in normalized_ids)
    rows = connection.execute(
        f"""
        SELECT
            id,
            source_host,
            inferred_project_key,
            inferred_project_label,
            cwd_name,
            last_user_message,
            latest_turn_summary,
            last_turn_timestamp,
            session_timestamp,
            started_at,
            imported_at
        FROM sessions
        WHERE id IN ({placeholders})
        """,
        normalized_ids,
    ).fetchall()
    return {str(row["id"] or ""): row for row in rows if _normalized_string(row["id"])}


def _fetch_environment_events(
    connection: sqlite3.Connection,
    session_ids: list[str],
) -> dict[str, list[sqlite3.Row]]:
    normalized_ids = [session_id for session_id in session_ids if _normalized_string(session_id)]
    if not normalized_ids:
        return {}
    placeholders = ", ".join("?" for _ in normalized_ids)
    rows = connection.execute(
        f"""
        SELECT
            session_id,
            event_index,
            timestamp,
            command_text,
            exit_code,
            detail_text
        FROM events
        WHERE session_id IN ({placeholders})
        ORDER BY session_id ASC, event_index ASC
        """,
        normalized_ids,
    ).fetchall()
    events_by_session: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        session_id = _normalized_string(row["session_id"])
        if not session_id:
            continue
        events_by_session.setdefault(session_id, []).append(row)
    return events_by_session


def _delete_environment_rollups(connection: sqlite3.Connection, session_ids: list[str]) -> None:
    normalized_ids = [session_id for session_id in session_ids if _normalized_string(session_id)]
    if not normalized_ids:
        return
    placeholders = ", ".join("?" for _ in normalized_ids)
    connection.execute(
        f"DELETE FROM environment_command_observations WHERE session_id IN ({placeholders})",
        normalized_ids,
    )


def _fetch_materialized_observation_rows(
    connection: sqlite3.Connection,
    *,
    source_host: str | None = None,
    source_project_keys: list[str] | None = None,
) -> list[sqlite3.Row]:
    conditions = [
        """
        NOT EXISTS (
            SELECT 1
            FROM ignored_project_sources AS i
            WHERE i.match_project_key = o.inferred_project_key
        )
        """,
    ]
    params: list[Any] = []
    if source_host is not None:
        conditions.append("o.source_host = ?")
        params.append(source_host)
    if source_project_keys:
        placeholders = ", ".join("?" for _ in source_project_keys)
        conditions.append(f"o.inferred_project_key IN ({placeholders})")
        params.extend(source_project_keys)
    return connection.execute(
        f"""
        SELECT
            o.session_id,
            o.event_index,
            o.timestamp,
            o.source_host,
            o.inferred_project_key,
            o.project_label,
            o.title,
            o.command_text,
            o.exit_code,
            o.binary,
            o.command_family,
            o.display_command,
            o.wrapper_label,
            o.failure_capability_key,
            o.failure_subject_label,
            o.failure_status,
            o.failure_guidance_kind,
            o.failure_class_key,
            o.failure_class_label,
            o.failure_tone,
            o.is_success
        FROM environment_command_observations AS o
        WHERE {' AND '.join(conditions)}
        ORDER BY COALESCE(o.timestamp, '') DESC,
                 o.session_id DESC,
                 o.event_index DESC
        """,
        params,
    ).fetchall()


def _fetch_host_capability_rows(
    connection: sqlite3.Connection,
    hosts: list[str],
) -> dict[str, dict[str, sqlite3.Row]]:
    normalized_hosts = sorted({trimmed(host) for host in hosts if trimmed(host)})
    if not normalized_hosts:
        return {}
    placeholders = ", ".join("?" for _ in normalized_hosts)
    rows = connection.execute(
        f"""
        SELECT
            source_host,
            capability_key,
            subject_label,
            status,
            attempt_count,
            success_count,
            failure_count,
            missing_count,
            blocked_count,
            unknown_count,
            latest_timestamp,
            updated_at
        FROM environment_host_capabilities
        WHERE source_host IN ({placeholders})
        ORDER BY source_host ASC, capability_key ASC
        """,
        normalized_hosts,
    ).fetchall()
    by_host: dict[str, dict[str, sqlite3.Row]] = {host: {} for host in normalized_hosts}
    for row in rows:
        host = _normalized_string(row["source_host"])
        capability_key = _normalized_string(row["capability_key"])
        if not host or not capability_key:
            continue
        by_host.setdefault(host, {})[capability_key] = row
    return by_host


def _materialized_project_label(session_row: sqlite3.Row) -> str:
    return (
        trimmed(session_row["inferred_project_label"])
        or trimmed(session_row["cwd_name"])
        or trimmed(session_row["inferred_project_key"])
        or "Unknown project"
    )


def _materialized_session_title(session_row: sqlite3.Row) -> str:
    return trimmed(session_row["last_user_message"]) or trimmed(session_row["latest_turn_summary"]) or "Session activity"


def _materialized_observation_inserts(
    session_row: sqlite3.Row,
    events: Sequence[dict[str, Any]],
) -> list[tuple[Any, ...]]:
    session_id = _normalized_string(session_row["id"])
    if not session_id:
        return []
    source_host = trimmed(session_row["source_host"]) or "unknown-host"
    project_key = trimmed(session_row["inferred_project_key"]) or ""
    project_label = _materialized_project_label(session_row)
    title = _materialized_session_title(session_row)

    inserts: list[tuple[Any, ...]] = []
    for event in events:
        command_text = _normalized_string(event.get("command_text"))
        if not command_text:
            continue
        event_index = int(event.get("event_index") or 0)
        timestamp = _normalized_string(event.get("timestamp"))
        exit_code = int(event.get("exit_code") or 0)
        detail_text = _normalized_string(event.get("detail_text"))
        parsed = parse_command(command_text)
        failure = classify_failure(command_text, exit_code, detail_text)
        inserts.append(
            (
                session_id,
                event_index,
                timestamp or None,
                source_host,
                project_key,
                project_label,
                title,
                command_text,
                exit_code,
                parsed.binary,
                parsed.command_family,
                parsed.display_command or command_text,
                parsed.wrapper_label,
                str(failure["capability_key"]) if failure else "",
                str(failure["subject_label"]) if failure else "",
                str(failure["status"]) if failure else "",
                str(failure["guidance_kind"]) if failure else "",
                str(failure["class_key"]) if failure else "",
                str(failure["class_label"]) if failure else "",
                str(failure["tone"]) if failure else "stone",
                1 if exit_code == 0 else 0,
            )
        )
    return inserts


def _materialized_observation_columns() -> str:
    return """
        session_id,
        event_index,
        timestamp,
        source_host,
        inferred_project_key,
        project_label,
        title,
        command_text,
        exit_code,
        binary,
        command_family,
        display_command,
        wrapper_label,
        failure_capability_key,
        failure_subject_label,
        failure_status,
        failure_guidance_kind,
        failure_class_key,
        failure_class_label,
        failure_tone,
        is_success
    """


def _rebuild_host_capability_rollups(
    connection: sqlite3.Connection,
    hosts: Sequence[str],
) -> None:
    normalized_hosts = sorted({trimmed(host) for host in hosts if trimmed(host)})
    if not normalized_hosts:
        return

    placeholders = ", ".join("?" for _ in normalized_hosts)
    connection.execute(
        f"DELETE FROM environment_host_capabilities WHERE source_host IN ({placeholders})",
        normalized_hosts,
    )

    updated_at = _utc_now_iso()
    inserts: list[tuple[Any, ...]] = []
    for host in normalized_hosts:
        observation_rows = _fetch_materialized_observation_rows(connection, source_host=host)
        binary_map, signals, _recent_evidence, _failure_rows = _build_observations_from_materialized(
            observation_rows,
            {},
        )
        for binary, item in binary_map.items():
            if binary == "unknown":
                continue
            status = str(item["status"])
            inserts.append(
                (
                    host,
                    f"binary:{binary}",
                    binary,
                    status,
                    int(item["attempt_count"]),
                    int(item["success_count"]),
                    int(item["failure_count"]),
                    int(item["failure_count"]) if status == "missing" else 0,
                    int(item["failure_count"]) if status == "blocked" else 0,
                    int(item["attempt_count"]) if status == "unknown" else 0,
                    item["last_timestamp"] or None,
                    updated_at,
                )
            )
        for signal in signals:
            signal_key = str(signal["key"])
            if signal_key.startswith("binary:"):
                continue
            signal_status = str(signal["status"])
            normalized_status = "missing" if signal_status == "missing" else ("blocked" if signal_status else "unknown")
            inserts.append(
                (
                    host,
                    signal_key,
                    str(signal["subject_label"]),
                    normalized_status,
                    int(signal["count"]),
                    0,
                    int(signal["count"]),
                    int(signal["count"]) if normalized_status == "missing" else 0,
                    int(signal["count"]) if normalized_status == "blocked" else 0,
                    int(signal["count"]) if normalized_status == "unknown" else 0,
                    (signal.get("examples") or [{}])[0].get("timestamp") or None,
                    updated_at,
                )
            )

    if inserts:
        connection.executemany(
            f"""
            INSERT INTO environment_host_capabilities (
                source_host,
                capability_key,
                subject_label,
                status,
                attempt_count,
                success_count,
                failure_count,
                missing_count,
                blocked_count,
                unknown_count,
                latest_timestamp,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )


def replace_session_environment_rollups(
    connection: sqlite3.Connection,
    session_id: str,
    events: Sequence[sqlite3.Row | dict[str, Any] | object] | None = None,
) -> None:
    normalized_session_id = _normalized_string(session_id)
    if not normalized_session_id:
        return

    session_row = _fetch_environment_session_rows(connection, [normalized_session_id]).get(normalized_session_id)
    _delete_environment_rollups(connection, [normalized_session_id])
    if session_row is None:
        return

    normalized_events = _normalize_environment_events(
        events if events is not None else _fetch_environment_events(connection, [normalized_session_id]).get(normalized_session_id, [])
    )
    observation_inserts = _materialized_observation_inserts(session_row, normalized_events)
    if observation_inserts:
        connection.executemany(
            f"""
            INSERT INTO environment_command_observations (
                {_materialized_observation_columns()}
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            observation_inserts,
        )

    _rebuild_host_capability_rollups(
        connection,
        [trimmed(session_row["source_host"]) or "unknown-host"],
    )
    connection.execute(
        "UPDATE sessions SET environment_rollup_version = ? WHERE id = ?",
        (ENVIRONMENT_ROLLUP_VERSION, normalized_session_id),
    )


def backfill_environment_rollups(connection: sqlite3.Connection) -> int:
    stale_rows = connection.execute(
        """
        SELECT id, source_host
        FROM sessions
        WHERE COALESCE(environment_rollup_version, 0) < ?
        ORDER BY id ASC
        """,
        (ENVIRONMENT_ROLLUP_VERSION,),
    ).fetchall()
    session_ids = [_normalized_string(row["id"]) for row in stale_rows if _normalized_string(row["id"])]
    if not session_ids:
        return 0

    session_rows = _fetch_environment_session_rows(connection, session_ids)
    events_by_session = _fetch_environment_events(connection, session_ids)
    _delete_environment_rollups(connection, session_ids)

    observation_inserts: list[tuple[Any, ...]] = []
    affected_hosts: set[str] = set()
    for session_id in session_ids:
        session_row = session_rows.get(session_id)
        if session_row is None:
            continue
        affected_hosts.add(trimmed(session_row["source_host"]) or "unknown-host")
        observation_inserts.extend(
            _materialized_observation_inserts(
                session_row,
                _normalize_environment_events(events_by_session.get(session_id, [])),
            )
        )

    if observation_inserts:
        connection.executemany(
            f"""
            INSERT INTO environment_command_observations (
                {_materialized_observation_columns()}
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            observation_inserts,
        )

    _rebuild_host_capability_rollups(connection, sorted(affected_hosts))
    connection.executemany(
        "UPDATE sessions SET environment_rollup_version = ? WHERE id = ?",
        [(ENVIRONMENT_ROLLUP_VERSION, session_id) for session_id in session_ids],
    )
    return len(session_ids)


def _command_rows_for_sessions(
    connection: sqlite3.Connection,
    *,
    source_host: str | None = None,
    source_project_keys: list[str] | None = None,
) -> list[sqlite3.Row]:
    conditions = [
        "e.command_text IS NOT NULL",
        "TRIM(e.command_text) != ''",
        """
        NOT EXISTS (
            SELECT 1
            FROM ignored_project_sources AS i
            WHERE i.match_project_key = s.inferred_project_key
        )
        """,
    ]
    params: list[Any] = []
    if source_host is not None:
        conditions.append("s.source_host = ?")
        params.append(source_host)
    if source_project_keys:
        placeholders = ", ".join("?" for _ in source_project_keys)
        conditions.append(f"s.inferred_project_key IN ({placeholders})")
        params.extend(source_project_keys)
    return connection.execute(
        f"""
        SELECT
            e.session_id,
            e.event_index,
            e.tool_name,
            e.command_text,
            e.exit_code,
            e.detail_text,
            s.source_host,
            s.inferred_project_key,
            s.last_user_message,
            s.latest_turn_summary,
            s.last_turn_timestamp,
            s.session_timestamp
        FROM events AS e
        JOIN sessions AS s
            ON s.id = e.session_id
        WHERE {' AND '.join(conditions)}
        ORDER BY COALESCE(s.last_turn_timestamp, s.session_timestamp, s.started_at, s.imported_at) DESC,
                 e.session_id DESC,
                 e.event_index DESC
        """,
        params,
    ).fetchall()


def _tool_rows_for_sessions(
    connection: sqlite3.Connection,
    *,
    source_host: str | None = None,
    source_project_keys: list[str] | None = None,
) -> list[sqlite3.Row]:
    conditions = [
        "e.tool_name IS NOT NULL",
        "TRIM(e.tool_name) != ''",
        """
        NOT EXISTS (
            SELECT 1
            FROM ignored_project_sources AS i
            WHERE i.match_project_key = s.inferred_project_key
        )
        """,
    ]
    params: list[Any] = []
    if source_host is not None:
        conditions.append("s.source_host = ?")
        params.append(source_host)
    if source_project_keys:
        placeholders = ", ".join("?" for _ in source_project_keys)
        conditions.append(f"s.inferred_project_key IN ({placeholders})")
        params.extend(source_project_keys)
    return connection.execute(
        f"""
        SELECT e.tool_name, COUNT(*) AS count
        FROM events AS e
        JOIN sessions AS s
            ON s.id = e.session_id
        WHERE {' AND '.join(conditions)}
        GROUP BY e.tool_name
        ORDER BY count DESC, e.tool_name ASC
        """,
        params,
    ).fetchall()


def _session_evidence_item(row: sqlite3.Row, project_label: str | None = None) -> dict[str, Any]:
    timestamp = trimmed(row["last_turn_timestamp"]) or trimmed(row["session_timestamp"]) or ""
    title = trimmed(row["last_user_message"]) or trimmed(row["latest_turn_summary"]) or "Session activity"
    item = {
        "session_id": str(row["session_id"] if "session_id" in row.keys() else row["id"]),
        "session_href": f"/sessions/{quote(str(row['session_id'] if 'session_id' in row.keys() else row['id']), safe='')}",
        "title": title,
        "timestamp": timestamp,
    }
    if project_label:
        item["project_label"] = project_label
    return item


def _build_observations(
    command_rows: list[sqlite3.Row],
    project_labels_by_key: dict[str, str],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    binaries: dict[str, dict[str, Any]] = {}
    signals: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, Any]] = []
    recent_evidence: list[dict[str, Any]] = []

    for row in command_rows:
        parsed = parse_command(row["command_text"])
        project_key = trimmed(row["inferred_project_key"]) or ""
        project_label = project_labels_by_key.get(project_key, project_key or "Unknown project")
        observation = binaries.setdefault(
            parsed.binary,
            {
                "binary": parsed.binary,
                "command_family": parsed.command_family,
                "display_command": parsed.display_command or (row["command_text"] or "").strip(),
                "wrapper_label": parsed.wrapper_label,
                "attempt_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "project_labels": set(),
                "example_session_id": str(row["session_id"]),
                "example_command": str(row["command_text"] or "").strip(),
                "last_timestamp": trimmed(row["last_turn_timestamp"]) or trimmed(row["session_timestamp"]) or "",
                "last_failure_class": None,
            },
        )
        observation["attempt_count"] += 1
        observation["project_labels"].add(project_label)
        if int(row["exit_code"] or 0) == 0:
            observation["success_count"] += 1
        else:
            observation["failure_count"] += 1
            failure = classify_failure(row["command_text"], row["exit_code"], row["detail_text"])
            if failure is not None:
                observation["last_failure_class"] = failure["class_key"]
                signal = signals.setdefault(
                    str(failure["capability_key"]),
                    {
                        "key": str(failure["capability_key"]),
                        "status": str(failure["status"]),
                        "subject_label": str(failure["subject_label"]),
                        "guidance_kind": str(failure["guidance_kind"]),
                        "tone": str(failure["tone"]),
                        "count": 0,
                        "binaries": set(),
                        "project_labels": set(),
                        "examples": [],
                    },
                )
                signal["count"] += 1
                signal["binaries"].add(parsed.binary)
                signal["project_labels"].add(project_label)
                if len(signal["examples"]) < 3:
                    signal["examples"].append(
                        {
                            "command": str(row["command_text"] or "").strip(),
                            "session_href": f"/sessions/{quote(str(row['session_id']), safe='')}",
                            "session_id": str(row["session_id"]),
                            "timestamp": trimmed(row["last_turn_timestamp"]) or trimmed(row["session_timestamp"]) or "",
                            "project_label": project_label,
                        }
                    )
                failures.append(
                    {
                        "binary": parsed.binary,
                        "command": str(row["command_text"] or "").strip(),
                        "class_key": failure["class_key"],
                        "class_label": failure["class_label"],
                        "guidance_kind": failure["guidance_kind"],
                        "tone": failure["tone"],
                        "project_label": project_label,
                        "session_href": f"/sessions/{quote(str(row['session_id']), safe='')}",
                        "session_id": str(row["session_id"]),
                        "timestamp": trimmed(row["last_turn_timestamp"]) or trimmed(row["session_timestamp"]) or "",
                    }
                )
        if len(recent_evidence) < 12:
            evidence = _session_evidence_item(row, project_label)
            evidence["command"] = str(row["command_text"] or "").strip()
            evidence["binary"] = parsed.binary
            evidence["exit_code"] = int(row["exit_code"] or 0)
            recent_evidence.append(evidence)

    binary_list: list[dict[str, Any]] = []
    for binary, item in binaries.items():
        status = "available" if item["success_count"] > 0 else "unknown"
        guidance_summary = None
        signal = signals.get(f"binary:{binary}")
        if signal and item["success_count"] == 0:
            status = "missing" if signal["status"] == "missing" else "blocked"
            guidance_summary = signal["guidance_kind"]
        binary_list.append(
            {
                **item,
                "status": status,
                "status_label": "Available" if status == "available" else ("Missing" if status == "missing" else ("Blocked" if status == "blocked" else "Unknown")),
                "project_labels": sorted(item["project_labels"]),
                "guidance_summary": guidance_summary,
            }
        )
    binary_list.sort(
        key=lambda item: (
            0 if item["status"] == "missing" else 1 if item["status"] == "blocked" else 2 if item["status"] == "unknown" else 3,
            1 if item["binary"] in BASELINE_BINARIES else 0,
            -int(item["failure_count"]),
            -int(item["attempt_count"]),
            item["binary"],
        )
    )
    failures.sort(key=lambda item: (item["class_label"], item["binary"], item["timestamp"]), reverse=True)
    signal_list = [
        {
            **signal,
            "binaries": sorted(signal["binaries"]),
            "project_labels": sorted(signal["project_labels"]),
        }
        for signal in signals.values()
    ]
    signal_list.sort(key=lambda item: (-int(item["count"]), item["subject_label"]))
    return {item["binary"]: item for item in binary_list}, signal_list, recent_evidence, failures


def _build_observations_from_materialized(
    observation_rows: list[sqlite3.Row],
    project_labels_by_key: dict[str, str],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    binaries: dict[str, dict[str, Any]] = {}
    signals: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, Any]] = []
    recent_evidence: list[dict[str, Any]] = []

    for row in observation_rows:
        binary = _normalized_string(row["binary"]) or "unknown"
        project_key = trimmed(row["inferred_project_key"]) or ""
        project_label = (
            project_labels_by_key.get(project_key)
            or trimmed(row["project_label"])
            or project_key
            or "Unknown project"
        )
        observation = binaries.setdefault(
            binary,
            {
                "binary": binary,
                "command_family": _normalized_string(row["command_family"]) or binary,
                "display_command": _normalized_string(row["display_command"]) or _normalized_string(row["command_text"]),
                "wrapper_label": trimmed(row["wrapper_label"]) or None,
                "attempt_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "project_labels": set(),
                "example_session_id": _normalized_string(row["session_id"]),
                "example_command": _normalized_string(row["command_text"]),
                "last_timestamp": trimmed(row["timestamp"]) or "",
                "last_failure_class": None,
            },
        )
        observation["attempt_count"] += 1
        observation["project_labels"].add(project_label)
        if int(row["is_success"] or 0) == 1:
            observation["success_count"] += 1
        else:
            observation["failure_count"] += 1
            failure_key = _normalized_string(row["failure_class_key"])
            if failure_key:
                observation["last_failure_class"] = failure_key
                signal_key = _normalized_string(row["failure_capability_key"])
                signal = signals.setdefault(
                    signal_key,
                    {
                        "key": signal_key,
                        "status": _normalized_string(row["failure_status"]),
                        "subject_label": _normalized_string(row["failure_subject_label"]),
                        "guidance_kind": _normalized_string(row["failure_guidance_kind"]),
                        "tone": _normalized_string(row["failure_tone"]) or "stone",
                        "count": 0,
                        "binaries": set(),
                        "project_labels": set(),
                        "examples": [],
                    },
                )
                signal["count"] += 1
                signal["binaries"].add(binary)
                signal["project_labels"].add(project_label)
                if len(signal["examples"]) < 3:
                    signal["examples"].append(
                        {
                            "command": _normalized_string(row["command_text"]),
                            "session_href": f"/sessions/{quote(_normalized_string(row['session_id']), safe='')}",
                            "session_id": _normalized_string(row["session_id"]),
                            "timestamp": trimmed(row["timestamp"]) or "",
                            "project_label": project_label,
                        }
                    )
                failures.append(
                    {
                        "binary": binary,
                        "command": _normalized_string(row["command_text"]),
                        "class_key": failure_key,
                        "class_label": _normalized_string(row["failure_class_label"]),
                        "guidance_kind": _normalized_string(row["failure_guidance_kind"]),
                        "tone": _normalized_string(row["failure_tone"]) or "stone",
                        "project_label": project_label,
                        "session_href": f"/sessions/{quote(_normalized_string(row['session_id']), safe='')}",
                        "session_id": _normalized_string(row["session_id"]),
                        "timestamp": trimmed(row["timestamp"]) or "",
                    }
                )

        if len(recent_evidence) < 12:
            recent_evidence.append(
                {
                    "session_id": _normalized_string(row["session_id"]),
                    "session_href": f"/sessions/{quote(_normalized_string(row['session_id']), safe='')}",
                    "title": _normalized_string(row["title"]) or "Session activity",
                    "timestamp": trimmed(row["timestamp"]) or "",
                    "project_label": project_label,
                    "command": _normalized_string(row["command_text"]),
                    "binary": binary,
                    "exit_code": int(row["exit_code"] or 0),
                }
            )

    binary_list: list[dict[str, Any]] = []
    for binary, item in binaries.items():
        status = "available" if item["success_count"] > 0 else "unknown"
        guidance_summary = None
        signal = signals.get(f"binary:{binary}")
        if signal and item["success_count"] == 0:
            status = "missing" if signal["status"] == "missing" else "blocked"
            guidance_summary = signal["guidance_kind"]
        binary_list.append(
            {
                **item,
                "status": status,
                "status_label": "Available" if status == "available" else ("Missing" if status == "missing" else ("Blocked" if status == "blocked" else "Unknown")),
                "project_labels": sorted(item["project_labels"]),
                "guidance_summary": guidance_summary,
            }
        )
    binary_list.sort(
        key=lambda item: (
            0 if item["status"] == "missing" else 1 if item["status"] == "blocked" else 2 if item["status"] == "unknown" else 3,
            1 if item["binary"] in BASELINE_BINARIES else 0,
            -int(item["failure_count"]),
            -int(item["attempt_count"]),
            item["binary"],
        )
    )
    failures.sort(key=lambda item: (item["class_label"], item["binary"], item["timestamp"]), reverse=True)
    signal_list = [
        {
            **signal,
            "binaries": sorted(signal["binaries"]),
            "project_labels": sorted(signal["project_labels"]),
        }
        for signal in signals.values()
    ]
    signal_list.sort(key=lambda item: (-int(item["count"]), item["subject_label"]))
    return {item["binary"]: item for item in binary_list}, signal_list, recent_evidence, failures


def _tool_surface_summary(tool_rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [
        {"tool_name": str(row["tool_name"]), "count": int(row["count"] or 0)}
        for row in tool_rows
    ]


def _status_for_requirement(binary_map: dict[str, dict[str, Any]], signals: list[dict[str, Any]], requirement_key: str) -> str:
    if requirement_key.startswith("binary:"):
        binary = requirement_key.split(":", 1)[1]
        item = binary_map.get(binary)
        if item is None:
            return "unknown"
        return str(item["status"])
    for signal in signals:
        if signal["key"] == requirement_key:
            return "missing" if signal["status"] == "missing" else "blocked"
    return "unknown"


def _capability_status_for_requirement(
    capability_rows: dict[str, sqlite3.Row],
    requirement_key: str,
) -> str:
    row = capability_rows.get(requirement_key)
    if row is None:
        return "unknown"
    status = _normalized_string(row["status"])
    if status == "available":
        return "available"
    if status == "missing":
        return "missing"
    if status in {"blocked", "setup", "warning"}:
        return "blocked"
    return "unknown"


def _score_profiles(binary_map: dict[str, dict[str, Any]], signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for profile in PROFILE_DEFINITIONS:
        required_statuses = [
            _status_for_requirement(binary_map, signals, f"binary:{binary}")
            for binary in profile["required"]
        ]
        optional_statuses = [
            _status_for_requirement(binary_map, signals, f"binary:{binary}")
            for binary in profile["optional"]
        ]
        missing_required = sum(1 for status in required_statuses if status in {"missing", "blocked"})
        available_required = sum(1 for status in required_statuses if status == "available")
        available_optional = sum(1 for status in optional_statuses if status == "available")
        unknown_required = sum(1 for status in required_statuses if status == "unknown")
        total_possible = max(len(profile["required"]) * 2 + len(profile["optional"]), 1)
        score = int(((available_required * 2) + available_optional) * 100 / total_possible)
        if missing_required > 0:
            status = "fail"
            tone = "rose"
            status_label = "Missing required tools"
        elif available_required == len(profile["required"]) and available_required > 0:
            status = "ready"
            tone = "emerald"
            status_label = "Ready"
        elif available_required > 0 or unknown_required < len(profile["required"]):
            status = "partial"
            tone = "amber"
            status_label = "Partial"
        else:
            status = "unknown"
            tone = "stone"
            status_label = "Unknown"
        profiles.append(
            {
                "key": profile["key"],
                "label": profile["label"],
                "score": score,
                "status": status,
                "tone": tone,
                "status_label": status_label,
                "required_met": available_required,
                "required_total": len(profile["required"]),
                "missing_required": missing_required,
                "unknown_required": unknown_required,
                "available_optional": available_optional,
            }
        )
    profiles.sort(key=lambda item: (-int(item["score"]), item["label"]))
    return profiles


def _group_failures(failure_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for failure in failure_rows:
        group = grouped.setdefault(
            failure["class_key"],
            {
                "class_key": failure["class_key"],
                "class_label": failure["class_label"],
                "tone": failure["tone"],
                "guidance_kind": failure["guidance_kind"],
                "count": 0,
                "binaries": Counter(),
                "projects": set(),
                "examples": [],
            },
        )
        group["count"] += 1
        group["binaries"][failure["binary"]] += 1
        if failure.get("project_label"):
            group["projects"].add(str(failure["project_label"]))
        if len(group["examples"]) < 4:
            group["examples"].append(failure)

    rows: list[dict[str, Any]] = []
    for group in grouped.values():
        binaries = [name for name, _ in group["binaries"].most_common(4)]
        rows.append(
            {
                "class_key": group["class_key"],
                "class_label": group["class_label"],
                "tone": group["tone"],
                "guidance_kind": group["guidance_kind"],
                "count": group["count"],
                "binaries": binaries,
                "project_count": len(group["projects"]),
                "project_labels": sorted(group["projects"])[:4],
                "examples": group["examples"],
            }
        )
    rows.sort(key=lambda item: (-int(item["count"]), item["class_label"]))
    return rows


def _build_install_guidance(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    guidance: list[dict[str, Any]] = []
    for signal in signals:
        status = str(signal["status"])
        label = "Blocked by access" if status == "blocked" else ("Required" if status == "missing" else "Project-specific")
        guidance.append(
            {
                "subject_label": signal["subject_label"],
                "status": status,
                "tone": signal["tone"],
                "label": label,
                "guidance_kind": signal["guidance_kind"],
                "count": int(signal["count"]),
                "project_labels": signal["project_labels"][:4],
                "examples": signal["examples"][:2],
            }
        )
    guidance.sort(key=lambda item: (0 if item["status"] == "missing" else 1 if item["status"] == "blocked" else 2, -int(item["count"])))
    return guidance


def _host_summary(
    session_rows: list[sqlite3.Row],
    project_labels_by_key: dict[str, str],
) -> dict[str, Any]:
    project_counter: Counter[str] = Counter()
    turn_total = 0
    command_failure_total = 0
    aborted_turn_total = 0
    recent_by_key: dict[str, dict[str, Any]] = {}
    for row in session_rows:
        project_key = trimmed(row["inferred_project_key"]) or ""
        project_label = project_labels_by_key.get(project_key, project_key or "Unknown project")
        project_counter[project_label] += 1
        turn_total += int(row["turn_count"] or 0)
        command_failure_total += int(row["command_failure_count"] or 0)
        aborted_turn_total += int(row["aborted_turn_count"] or 0)
        timestamp = trimmed(row["last_turn_timestamp"]) or trimmed(row["session_timestamp"]) or ""
        current = recent_by_key.get(project_label)
        if current is None or timestamp > current["timestamp"]:
            recent_by_key[project_label] = {
                "project_label": project_label,
                "timestamp": timestamp,
                "session_href": f"/sessions/{quote(str(row['id']), safe='')}",
            }
    recent_projects = sorted(recent_by_key.values(), key=lambda item: item["timestamp"], reverse=True)[:6]
    top_projects = [
        {"project_label": label, "session_count": count}
        for label, count in project_counter.most_common(6)
    ]
    return {
        "session_count": len(session_rows),
        "turn_count": turn_total,
        "project_count": len(project_counter),
        "command_failure_count": command_failure_total,
        "aborted_turn_count": aborted_turn_total,
        "top_projects": top_projects,
        "recent_projects": recent_projects,
    }


def fetch_host_environment_audit(
    connection: sqlite3.Connection,
    source_host: str,
    settings: Any,
    *,
    project_access: ProjectAccessContext | None = None,
) -> dict[str, Any]:
    session_rows = _visible_session_rows_for_host(connection, source_host, project_access=project_access)
    project_rows = query_group_rows(connection, project_access=project_access)
    grouped_projects = build_grouped_projects(project_rows)
    route_map = {project.key: project.detail_href for project in grouped_projects}
    visible_source_project_keys = sorted(
        {
            str(effective_project_fields(row)["inferred_project_key"])
            for row in session_rows
            if trimmed(effective_project_fields(row)["inferred_project_key"])
        }
    )
    project_labels_by_key = {
        effective_project_fields(row)["inferred_project_key"]: effective_project_fields(row)["display_label"]
        for row in session_rows
    }
    observation_rows = _fetch_materialized_observation_rows(
        connection,
        source_host=source_host,
        source_project_keys=visible_source_project_keys,
    )
    tool_rows = _tool_rows_for_sessions(
        connection,
        source_host=source_host,
        source_project_keys=visible_source_project_keys,
    )
    binary_map, signals, recent_evidence, failure_rows = _build_observations_from_materialized(
        observation_rows,
        project_labels_by_key,
    )
    remote_snapshot = next(
        (item for item in fetch_remote_agent_health(connection, settings) if str(item["source_host"]) == source_host),
        None,
    )
    failure_groups = _group_failures(failure_rows)
    project_links: dict[str, str] = {}
    for row in session_rows:
        fields = effective_project_fields(row)
        detail_href = route_map.get(fields["effective_group_key"])
        if detail_href:
            project_links[fields["display_label"]] = detail_href
    return {
        "source_host": source_host,
        "remote": remote_snapshot,
        "summary": _host_summary(session_rows, project_labels_by_key),
        "tool_surface": _tool_surface_summary(tool_rows),
        "binaries": list(binary_map.values())[:18],
        "signals": signals,
        "profiles": _score_profiles(binary_map, signals),
        "failure_groups": failure_groups,
        "install_guidance": _build_install_guidance(signals)[:10],
        "recent_evidence": recent_evidence[:10],
        "project_links": project_links,
    }


def _infer_project_requirements(
    binary_map: dict[str, dict[str, Any]],
    signals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    requirements: list[dict[str, Any]] = []
    for binary, item in binary_map.items():
        attempts = int(item["attempt_count"])
        if binary in BASELINE_BINARIES:
            continue
        if binary == "unknown":
            continue
        strength = "required" if int(item["success_count"]) > 0 and attempts >= 2 else ("required" if int(item["failure_count"]) >= 2 else "likely")
        requirements.append(
            {
                "key": f"binary:{binary}",
                "label": binary,
                "kind": "binary",
                "strength": strength,
                "attempt_count": attempts,
                "success_count": int(item["success_count"]),
                "failure_count": int(item["failure_count"]),
                "status_label": "Observed" if int(item["success_count"]) > 0 else "Attempted",
            }
        )
    for signal in signals:
        if signal["key"].startswith("binary:"):
            continue
        requirements.append(
            {
                "key": signal["key"],
                "label": signal["subject_label"],
                "kind": "constraint",
                "strength": "required" if int(signal["count"]) > 1 else "likely",
                "attempt_count": int(signal["count"]),
                "success_count": 0,
                "failure_count": int(signal["count"]),
                "status_label": "Constraint",
            }
        )
    requirements.sort(
        key=lambda item: (
            0 if item["strength"] == "required" else 1,
            0 if item["kind"] == "constraint" else 1,
            -int(item["attempt_count"]),
            item["label"],
        )
    )
    return requirements[:14]


def _project_summary(
    session_rows: list[sqlite3.Row],
    failure_groups: list[dict[str, Any]],
    profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    hosts = {trimmed(row["source_host"]) or "unknown-host" for row in session_rows}
    top_failure = failure_groups[0]["class_label"] if failure_groups else "None"
    dominant_profile = profiles[0]["label"] if profiles else "Unknown"
    return {
        "session_count": len(session_rows),
        "turn_count": sum(int(row["turn_count"] or 0) for row in session_rows),
        "host_count": len(hosts),
        "hosts": sorted(hosts),
        "top_failure_class": top_failure,
        "dominant_profile": dominant_profile,
    }


def _host_fit_for_project(
    connection: sqlite3.Connection,
    host: str,
    requirements: list[dict[str, Any]],
    *,
    project_access: ProjectAccessContext | None = None,
) -> dict[str, Any]:
    capability_rows = _fetch_host_capability_rows(connection, [host]).get(host, {})
    required = [item for item in requirements if item["strength"] == "required"]
    if not required:
        return {
            "source_host": host,
            "score": 0,
            "status": "unknown",
            "tone": "stone",
            "status_label": "No hard requirements yet",
            "met_count": 0,
            "required_total": 0,
            "missing": [],
            "blocked": [],
            "unknown": [],
            "audit_href": f"/machines/{quote(host, safe='')}/audit",
        }
    met: list[str] = []
    missing: list[str] = []
    blocked: list[str] = []
    unknown: list[str] = []
    for requirement in required:
        status = _capability_status_for_requirement(capability_rows, requirement["key"])
        if status == "available":
            met.append(requirement["label"])
        elif status == "missing":
            missing.append(requirement["label"])
        elif status == "blocked":
            blocked.append(requirement["label"])
        else:
            unknown.append(requirement["label"])
    total = max(len(required), 1)
    score = int(len(met) * 100 / total)
    if missing or blocked:
        status = "fail"
        tone = "rose"
        status_label = "Missing blockers"
    elif unknown:
        status = "warn"
        tone = "amber"
        status_label = "Unknown fit"
    else:
        status = "good"
        tone = "emerald"
        status_label = "Good fit"
    return {
        "source_host": host,
        "score": score,
        "status": status,
        "tone": tone,
        "status_label": status_label,
        "met_count": len(met),
        "required_total": len(required),
        "missing": missing[:4],
        "blocked": blocked[:4],
        "unknown": unknown[:4],
        "audit_href": f"/machines/{quote(host, safe='')}/audit",
    }


def fetch_project_environment_audit(
    connection: sqlite3.Connection,
    group_key: str,
    *,
    project_access: ProjectAccessContext | None = None,
) -> dict[str, Any] | None:
    session_rows = _visible_session_rows_for_project(connection, group_key, project_access=project_access)
    if not session_rows:
        return None
    fingerprint = _project_audit_fingerprint(group_key, session_rows)
    source_project_keys = sorted(
        {
            str(effective_project_fields(row)["inferred_project_key"])
            for row in session_rows
            if trimmed(effective_project_fields(row)["inferred_project_key"])
        }
    )
    project_labels_by_key = {
        effective_project_fields(row)["inferred_project_key"]: effective_project_fields(row)["display_label"]
        for row in session_rows
    }
    core = _get_cached_project_environment_audit(connection, group_key, fingerprint)
    if core is None:
        grouped = build_grouped_projects(session_rows)
        group = next((item for item in grouped if item.key == group_key), None)
        if group is None:
            return None
        group.detail_href = project_detail_href_for_route(*project_route_segments(group))
        observation_rows = _fetch_materialized_observation_rows(
            connection,
            source_project_keys=source_project_keys,
        )
        tool_rows = _tool_rows_for_sessions(connection, source_project_keys=source_project_keys)
        binary_map, signals, recent_evidence, failure_rows = _build_observations_from_materialized(
            observation_rows,
            project_labels_by_key,
        )
        failure_groups = _group_failures(failure_rows)
        requirements = _infer_project_requirements(binary_map, signals)
        profiles = _score_profiles(binary_map, signals)
        core = _store_cached_project_environment_audit(
            connection,
            group_key,
            fingerprint,
            {
                "group": group,
                "summary": _project_summary(session_rows, failure_groups, profiles),
                "tool_surface": _tool_surface_summary(tool_rows),
                "requirements": requirements,
                "profiles": profiles,
                "failure_groups": failure_groups,
                "recent_evidence": recent_evidence[:10],
            },
        )

    requirements = list(core["requirements"])
    hosts = sorted({trimmed(row["source_host"]) or "unknown-host" for row in session_rows})
    host_fit = [
        _host_fit_for_project(connection, host, requirements, project_access=project_access)
        for host in hosts
    ]
    host_fit.sort(key=lambda item: (0 if item["status"] == "fail" else 1 if item["status"] == "warn" else 2, item["score"], item["source_host"]))
    guidance = []
    for requirement in requirements:
        impacted_hosts = []
        for fit in host_fit:
            if requirement["label"] in fit["missing"] or requirement["label"] in fit["blocked"]:
                impacted_hosts.append(fit["source_host"])
        if impacted_hosts:
            guidance.append(
                {
                    "label": requirement["label"],
                    "strength": requirement["strength"],
                    "kind": requirement["kind"],
                    "impacted_hosts": impacted_hosts[:4],
                }
            )
    audit = {
        **core,
        "host_fit": host_fit,
        "setup_guidance": guidance[:10],
    }
    return audit
