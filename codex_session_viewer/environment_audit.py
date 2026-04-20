from __future__ import annotations

import copy
import hashlib
import re
import shlex
import sqlite3
from collections import Counter, OrderedDict
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from .agents import fetch_remote_agent_health
from .projects import (
    build_grouped_projects,
    effective_project_fields,
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
    "repo_state_missing": {"label": "Repo setup", "tone": "stone", "guidance": "Repo setup"},
    "unknown": {"label": "Other failure", "tone": "stone", "guidance": "Inspect"},
}

PROJECT_ENVIRONMENT_AUDIT_CACHE_MAXSIZE = 32
PROJECT_ENVIRONMENT_AUDIT_CACHE: OrderedDict[
    tuple[str, str],
    tuple[str, dict[str, Any]],
] = OrderedDict()


@dataclass(slots=True)
class ParsedCommand:
    binary: str
    command_family: str
    display_command: str
    wrapper_label: str | None = None


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


def _visible_session_rows_for_host(connection: sqlite3.Connection, source_host: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            s.*,
            o.override_group_key,
            o.override_organization,
            o.override_repository,
            o.override_remote_url,
            o.override_display_label
        FROM sessions AS s
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
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


def _visible_session_rows_for_project(connection: sqlite3.Connection, group_key: str) -> list[sqlite3.Row]:
    return query_group_rows_for_key(connection, group_key)


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


def _host_cache_fingerprints(connection: sqlite3.Connection, hosts: list[str]) -> list[str]:
    normalized_hosts = sorted({trimmed(host) for host in hosts if trimmed(host)})
    if not normalized_hosts:
        return []
    placeholders = ", ".join("?" for _ in normalized_hosts)
    rows = connection.execute(
        f"""
        SELECT
            s.source_host,
            COUNT(*) AS session_count,
            MAX(s.updated_at) AS latest_updated_at
        FROM sessions AS s
        WHERE s.source_host IN ({placeholders})
          AND NOT EXISTS (
            SELECT 1
            FROM ignored_project_sources AS i
            WHERE i.match_project_key = s.inferred_project_key
          )
        GROUP BY s.source_host
        ORDER BY s.source_host ASC
        """,
        normalized_hosts,
    ).fetchall()
    return [
        "|".join(
            (
                str(row["source_host"] or ""),
                str(row["session_count"] or 0),
                str(row["latest_updated_at"] or ""),
            )
        )
        for row in rows
    ]


def _project_audit_fingerprint(
    connection: sqlite3.Connection,
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
    hosts = [str(row["source_host"] or "") for row in session_rows]
    for host_fingerprint in _host_cache_fingerprints(connection, hosts):
        hasher.update(host_fingerprint.encode("utf-8"))
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
        label = "Blocked by access" if status == "blocked" else ("Required" if status == "missing" else "Repo-specific")
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
) -> dict[str, Any]:
    session_rows = _visible_session_rows_for_host(connection, source_host)
    project_rows = query_group_rows(connection)
    grouped_projects = build_grouped_projects(project_rows)
    route_map = {project.key: project.detail_href for project in grouped_projects}
    project_labels_by_key = {
        effective_project_fields(row)["inferred_project_key"]: effective_project_fields(row)["display_label"]
        for row in session_rows
    }
    command_rows = _command_rows_for_sessions(connection, source_host=source_host)
    tool_rows = _tool_rows_for_sessions(connection, source_host=source_host)
    binary_map, signals, recent_evidence, failure_rows = _build_observations(command_rows, project_labels_by_key)
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
) -> dict[str, Any]:
    host_commands = _command_rows_for_sessions(connection, source_host=host)
    host_project_rows = _visible_session_rows_for_host(connection, host)
    project_labels_by_key = {
        effective_project_fields(row)["inferred_project_key"]: effective_project_fields(row)["display_label"]
        for row in host_project_rows
    }
    binary_map, signals, _recent_evidence, _failure_rows = _build_observations(host_commands, project_labels_by_key)
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
            "audit_href": f"/remotes/{quote(host, safe='')}/audit",
        }
    met: list[str] = []
    missing: list[str] = []
    blocked: list[str] = []
    unknown: list[str] = []
    for requirement in required:
        status = _status_for_requirement(binary_map, signals, requirement["key"])
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
        "audit_href": f"/remotes/{quote(host, safe='')}/audit",
    }


def fetch_project_environment_audit(
    connection: sqlite3.Connection,
    group_key: str,
) -> dict[str, Any] | None:
    session_rows = _visible_session_rows_for_project(connection, group_key)
    if not session_rows:
        return None
    fingerprint = _project_audit_fingerprint(connection, group_key, session_rows)
    cached = _get_cached_project_environment_audit(connection, group_key, fingerprint)
    if cached is not None:
        return cached
    grouped = build_grouped_projects(session_rows)
    group = next((item for item in grouped if item.key == group_key), None)
    if group is None:
        return None
    group.detail_href = project_detail_href_for_route(*project_route_segments(group))
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
    command_rows = _command_rows_for_sessions(connection, source_project_keys=source_project_keys)
    tool_rows = _tool_rows_for_sessions(connection, source_project_keys=source_project_keys)
    binary_map, signals, recent_evidence, failure_rows = _build_observations(command_rows, project_labels_by_key)
    failure_groups = _group_failures(failure_rows)
    requirements = _infer_project_requirements(binary_map, signals)
    profiles = _score_profiles(binary_map, signals)
    hosts = sorted({trimmed(row["source_host"]) or "unknown-host" for row in session_rows})
    host_fit = [_host_fit_for_project(connection, host, requirements) for host in hosts]
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
        "group": group,
        "summary": _project_summary(session_rows, failure_groups, profiles),
        "tool_surface": _tool_surface_summary(tool_rows),
        "requirements": requirements,
        "profiles": profiles,
        "failure_groups": failure_groups,
        "host_fit": host_fit,
        "setup_guidance": guidance[:10],
        "recent_evidence": recent_evidence[:10],
    }
    return _store_cached_project_environment_audit(connection, group_key, fingerprint, audit)
