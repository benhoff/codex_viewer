from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

from .config import Settings
from .projects import (
    build_grouped_projects,
    effective_project_fields,
    fetch_recent_session_turn_activity_windows,
    query_group_rows,
)


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def upsert_remote_agent_status(
    connection: sqlite3.Connection,
    *,
    source_host: str,
    agent_version: str,
    sync_api_version: str,
    sync_mode: str,
    update_state: str,
    update_message: str | None,
    server_version_seen: str | None,
    server_api_version_seen: str | None,
    last_seen_at: str | None = None,
    last_sync_at: str | None = None,
    last_upload_count: int = 0,
    last_skip_count: int = 0,
    last_fail_count: int = 0,
    last_error: str | None = None,
    acknowledged_raw_resend_token: str | None = None,
    last_raw_resend_at: str | None = None,
) -> None:
    seen_at = last_seen_at or utc_now_iso()
    row = connection.execute(
        """
        SELECT
            source_host,
            requested_raw_resend_token,
            acknowledged_raw_resend_token
        FROM remote_agents
        WHERE source_host = ?
        """,
        (source_host,),
    ).fetchone()
    existing_ack_token = trimmed(row["acknowledged_raw_resend_token"]) if row is not None else None
    ack_token = trimmed(acknowledged_raw_resend_token) or existing_ack_token
    values = (
        agent_version,
        sync_api_version,
        sync_mode,
        update_state,
        trimmed(update_message),
        trimmed(server_version_seen),
        trimmed(server_api_version_seen),
        seen_at,
        last_sync_at,
        last_upload_count,
        last_skip_count,
        last_fail_count,
        trimmed(last_error),
        ack_token,
        trimmed(last_raw_resend_at),
    )
    if row is None:
        connection.execute(
            """
            INSERT INTO remote_agents (
                source_host,
                agent_version,
                sync_api_version,
                sync_mode,
                update_state,
                update_message,
                server_version_seen,
                server_api_version_seen,
                last_seen_at,
                last_sync_at,
                last_upload_count,
                last_skip_count,
                last_fail_count,
                last_error,
                acknowledged_raw_resend_token,
                last_raw_resend_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (source_host, *values),
        )
        return

    connection.execute(
        """
        UPDATE remote_agents
        SET
            agent_version = ?,
            sync_api_version = ?,
            sync_mode = ?,
            update_state = ?,
            update_message = ?,
            server_version_seen = ?,
            server_api_version_seen = ?,
            last_seen_at = ?,
            last_sync_at = ?,
            last_upload_count = ?,
            last_skip_count = ?,
            last_fail_count = ?,
            last_error = ?,
            acknowledged_raw_resend_token = ?,
            last_raw_resend_at = ?
        WHERE source_host = ?
        """,
        (*values, source_host),
    )


def request_remote_raw_resend(
    connection: sqlite3.Connection,
    source_host: str,
    note: str | None = None,
) -> str:
    token = utc_now_iso()
    requested_at = token
    row = connection.execute(
        "SELECT source_host FROM remote_agents WHERE source_host = ?",
        (source_host,),
    ).fetchone()
    if row is None:
        connection.execute(
            """
            INSERT INTO remote_agents (
                source_host,
                agent_version,
                sync_api_version,
                sync_mode,
                update_state,
                last_seen_at,
                last_upload_count,
                last_skip_count,
                last_fail_count,
                requested_raw_resend_token,
                requested_raw_resend_at,
                requested_raw_resend_note
            ) VALUES (?, '', '', '', 'awaiting_contact', ?, 0, 0, 0, ?, ?, ?)
            """,
            (
                source_host,
                requested_at,
                token,
                requested_at,
                trimmed(note),
            ),
        )
        return token

    connection.execute(
        """
        UPDATE remote_agents
        SET
            requested_raw_resend_token = ?,
            requested_raw_resend_at = ?,
            requested_raw_resend_note = ?
        WHERE source_host = ?
        """,
        (
            token,
            requested_at,
            trimmed(note),
            source_host,
        ),
    )
    return token


def fetch_pending_remote_actions(
    connection: sqlite3.Connection,
    source_host: str,
) -> dict[str, dict[str, str]]:
    row = connection.execute(
        """
        SELECT
            requested_raw_resend_token,
            requested_raw_resend_at,
            requested_raw_resend_note,
            acknowledged_raw_resend_token
        FROM remote_agents
        WHERE source_host = ?
        """,
        (source_host,),
    ).fetchone()
    if row is None:
        return {}

    requested_token = trimmed(row["requested_raw_resend_token"])
    acknowledged_token = trimmed(row["acknowledged_raw_resend_token"])
    if not requested_token or requested_token == acknowledged_token:
        return {}

    action: dict[str, str] = {
        "token": requested_token,
        "requested_at": trimmed(row["requested_raw_resend_at"]) or "",
    }
    note = trimmed(row["requested_raw_resend_note"])
    if note:
        action["note"] = note
    return {"resend_raw": action}


def build_remote_agent_health(rows: list[sqlite3.Row], settings: Settings) -> list[dict[str, Any]]:
    health_rows: list[dict[str, Any]] = []
    for row in rows:
        agent_version = trimmed(row["agent_version"]) or "unknown"
        sync_api_version = trimmed(row["sync_api_version"]) or "unknown"
        server_version_seen = trimmed(row["server_version_seen"]) or settings.expected_agent_version
        server_api_version_seen = trimmed(row["server_api_version_seen"]) or settings.sync_api_version

        api_mismatch = sync_api_version != settings.sync_api_version
        version_mismatch = agent_version != settings.expected_agent_version
        requested_raw_resend_token = trimmed(row["requested_raw_resend_token"])
        acknowledged_raw_resend_token = trimmed(row["acknowledged_raw_resend_token"])
        pending_raw_resend = bool(
            requested_raw_resend_token
            and requested_raw_resend_token != acknowledged_raw_resend_token
        )
        stale = False
        last_seen_at = trimmed(row["last_seen_at"])
        if last_seen_at:
            seen = datetime.fromisoformat(last_seen_at.replace("Z", "+00:00"))
            stale = (datetime.now(UTC) - seen.astimezone(UTC)).total_seconds() > max(120, settings.sync_interval_seconds * 4)

        health_rows.append(
            {
                "source_host": row["source_host"],
                "agent_version": agent_version,
                "sync_api_version": sync_api_version,
                "sync_mode": trimmed(row["sync_mode"]) or "unknown",
                "update_state": trimmed(row["update_state"]) or "unknown",
                "update_message": trimmed(row["update_message"]),
                "server_version_seen": server_version_seen,
                "server_api_version_seen": server_api_version_seen,
                "last_seen_at": last_seen_at,
                "last_sync_at": trimmed(row["last_sync_at"]),
                "last_upload_count": int(row["last_upload_count"] or 0),
                "last_skip_count": int(row["last_skip_count"] or 0),
                "last_fail_count": int(row["last_fail_count"] or 0),
                "last_error": trimmed(row["last_error"]),
                "pending_raw_resend": pending_raw_resend,
                "requested_raw_resend_at": trimmed(row["requested_raw_resend_at"]),
                "requested_raw_resend_note": trimmed(row["requested_raw_resend_note"]),
                "last_raw_resend_at": trimmed(row["last_raw_resend_at"]),
                "api_mismatch": api_mismatch,
                "version_mismatch": version_mismatch,
                "stale": stale,
            }
        )
    health_rows.sort(key=lambda item: item["last_seen_at"] or "", reverse=True)
    return health_rows


def fetch_remote_agent_health(
    connection: sqlite3.Connection,
    settings: Settings,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM remote_agents
        ORDER BY last_seen_at DESC, source_host ASC
        """
    ).fetchall()
    return build_remote_agent_health(rows, settings)


def _session_timestamp(row: sqlite3.Row) -> str:
    return (
        trimmed(row["last_turn_timestamp"])
        or trimmed(row["session_timestamp"])
        or trimmed(row["started_at"])
        or trimmed(row["imported_at"])
        or ""
    )


def _session_title(row: sqlite3.Row) -> str:
    return (
        trimmed(row["last_user_message"])
        or trimmed(row["latest_turn_summary"])
        or trimmed(row["summary"])
        or "Session"
    )


def _new_agent_entry(source_host: str, remote: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "source_host": source_host,
        "remote": remote,
        "session_count_total": 0,
        "aborted_turn_count_total": 0,
        "recent_turn_count": 0,
        "recent_session_count": 0,
        "recent_aborted_turns": 0,
        "projects_24h": {},
        "recent_projects": {},
        "recent_sessions": [],
        "latest_session": None,
        "latest_failed_session": None,
    }


def _session_link(session_id: str) -> str:
    return f"/sessions/{quote(session_id, safe='')}"


def _row_session_item(
    row: sqlite3.Row,
    *,
    project_label: str,
    project_href: str,
) -> dict[str, Any]:
    session_id = str(row["id"])
    return {
        "session_id": session_id,
        "href": _session_link(session_id),
        "timestamp": _session_timestamp(row),
        "title": _session_title(row),
        "project_label": project_label,
        "project_href": project_href,
        "command_failures": int(row["command_failure_count"] or 0),
        "aborted_turns": int(row["aborted_turn_count"] or 0),
        "viewer_warning": bool(trimmed(row["import_warning"])),
    }


def _latest_session(current: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    if current is None:
        return candidate
    if str(candidate.get("timestamp") or "") > str(current.get("timestamp") or ""):
        return candidate
    return current


def _attention_badges(entry: dict[str, Any]) -> list[dict[str, str]]:
    badges: list[dict[str, str]] = []
    remote = entry.get("remote") or {}
    if int(remote.get("last_fail_count") or 0) > 0 or remote.get("last_error"):
        badges.append({"tone": "rose", "label": "Sync failure"})
    return badges


def _secondary_badges(entry: dict[str, Any]) -> list[dict[str, str]]:
    badges: list[dict[str, str]] = []
    remote = entry.get("remote") or {}
    if remote.get("stale"):
        badges.append({"tone": "stone", "label": "Offline"})
    if remote.get("version_mismatch"):
        badges.append({"tone": "amber", "label": "Version drift"})
    if remote.get("api_mismatch"):
        badges.append({"tone": "amber", "label": "API drift"})
    update_state = str(remote.get("update_state") or "").strip()
    if update_state and update_state not in {"", "current"}:
        badges.append({"tone": "stone", "label": update_state.replace("_", " ")})
    return badges


def _recent_projects_list(entry: dict[str, Any]) -> list[dict[str, str]]:
    values = list(entry["recent_projects"].values())
    values.sort(key=lambda item: item["timestamp"], reverse=True)
    return values[:4]


def fetch_agents_dashboard(
    connection: sqlite3.Connection,
    settings: Settings,
) -> dict[str, Any]:
    remotes = fetch_remote_agent_health(connection, settings)
    rows = query_group_rows(connection)
    route_map = {
        project.key: project.detail_href
        for project in build_grouped_projects(rows)
    }
    recent_window_start = (datetime.now().astimezone() - timedelta(hours=24)).isoformat()
    recent_turn_activity = fetch_recent_session_turn_activity_windows(
        connection,
        [row["id"] for row in rows],
        recent_window_start,
    )

    entries: dict[str, dict[str, Any]] = {
        str(remote["source_host"]): _new_agent_entry(str(remote["source_host"]), remote)
        for remote in remotes
    }

    for row in rows:
        project = effective_project_fields(row)
        source_host = str(project["source_host"] or "").strip()
        if not source_host:
            continue
        entry = entries.setdefault(source_host, _new_agent_entry(source_host, None))
        entry["session_count_total"] += 1
        entry["aborted_turn_count_total"] += int(row["aborted_turn_count"] or 0)

        project_href = route_map.get(project["effective_group_key"], "")
        session_item = _row_session_item(
            row,
            project_label=str(project["display_label"]),
            project_href=project_href,
        )
        if len(entry["recent_sessions"]) < 4:
            entry["recent_sessions"].append(session_item)
        entry["latest_session"] = _latest_session(entry["latest_session"], session_item)

        session_problematic = False
        if session_problematic:
            entry["latest_failed_session"] = _latest_session(entry["latest_failed_session"], session_item)

        recent = recent_turn_activity.get(str(row["id"]))
        if recent:
            entry["recent_turn_count"] += int(recent.get("turn_count", 0) or 0)
            entry["recent_session_count"] += 1
            entry["recent_aborted_turns"] += int(row["aborted_turn_count"] or 0)
            entry["projects_24h"][project["display_label"]] = {
                "label": str(project["display_label"]),
                "href": project_href,
            }

        existing_project = entry["recent_projects"].get(project["display_label"])
        if existing_project is None or session_item["timestamp"] > existing_project["timestamp"]:
            entry["recent_projects"][project["display_label"]] = {
                "label": str(project["display_label"]),
                "href": project_href,
                "timestamp": session_item["timestamp"],
            }

    agent_rows: list[dict[str, Any]] = []
    for source_host, entry in entries.items():
        remote = entry.get("remote") or {}
        latest_session = entry.get("latest_session")
        latest_failed_session = entry.get("latest_failed_session")
        recent_projects = _recent_projects_list(entry)
        issue_badges = _attention_badges(entry)
        secondary_badges = _secondary_badges(entry)
        is_attention = bool(issue_badges)
        is_dormant = bool(remote.get("stale")) or (not is_attention and int(entry["recent_turn_count"] or 0) == 0)
        if is_attention:
            section = "attention"
            summary = (
                latest_failed_session["title"]
                if latest_failed_session
                else str(remote.get("last_error") or "Needs attention")
            )
            primary = latest_failed_session or latest_session
            primary_label = "Open latest attention session" if latest_failed_session else "Open latest session"
        elif is_dormant:
            section = "dormant"
            summary = (
                f"Last repo {latest_session['project_label']}"
                if latest_session
                else "No recent session activity"
            )
            primary = latest_session
            primary_label = "Open latest session"
        else:
            section = "active"
            project_count_24h = len(entry["projects_24h"])
            summary = (
                f"{int(entry['recent_turn_count'])} turns · {int(entry['recent_session_count'])} sessions · "
                f"{project_count_24h} project{'s' if project_count_24h != 1 else ''} in the last 24h"
            )
            primary = latest_session
            primary_label = "Open latest session"

        latest_repo = latest_session["project_label"] if latest_session else "No repo yet"
        latest_repo_href = latest_session["project_href"] if latest_session else ""
        row_item = {
            "source_host": source_host,
            "section": section,
            "summary": summary,
            "last_seen_at": trimmed(remote.get("last_seen_at")) or (latest_session["timestamp"] if latest_session else ""),
            "last_sync_at": trimmed(remote.get("last_sync_at")),
            "latest_repo": latest_repo,
            "latest_repo_href": latest_repo_href,
            "latest_session": latest_session,
            "latest_failed_session": latest_failed_session,
            "recent_turn_count": int(entry["recent_turn_count"] or 0),
            "recent_session_count": int(entry["recent_session_count"] or 0),
            "projects_touched_24h_count": len(entry["projects_24h"]),
            "recent_aborted_turns": int(entry["recent_aborted_turns"] or 0),
            "recent_projects": recent_projects,
            "recent_sessions": entry["recent_sessions"],
            "issue_badges": issue_badges,
            "secondary_badges": secondary_badges,
            "pending_raw_resend": bool(remote.get("pending_raw_resend")),
            "requested_raw_resend_at": trimmed(remote.get("requested_raw_resend_at")),
            "requested_raw_resend_note": trimmed(remote.get("requested_raw_resend_note")),
            "last_raw_resend_at": trimmed(remote.get("last_raw_resend_at")),
            "last_upload_count": int(remote.get("last_upload_count") or 0),
            "last_skip_count": int(remote.get("last_skip_count") or 0),
            "last_fail_count": int(remote.get("last_fail_count") or 0),
            "agent_version": trimmed(remote.get("agent_version")) or "",
            "sync_api_version": trimmed(remote.get("sync_api_version")) or "",
            "sync_mode": trimmed(remote.get("sync_mode")) or "",
            "update_state": trimmed(remote.get("update_state")) or "",
            "update_message": trimmed(remote.get("update_message")) or "",
            "last_error": trimmed(remote.get("last_error")) or "",
            "primary_href": primary["href"] if primary else "",
            "primary_label": primary_label,
            "audit_href": f"/remotes/{quote(source_host, safe='')}/audit",
            "toggle_id": f"agent-details-{quote(source_host, safe='')}",
        }
        agent_rows.append(row_item)

    def sort_key(item: dict[str, Any]) -> tuple[str, str]:
        latest = item["last_seen_at"] or (item["latest_session"]["timestamp"] if item["latest_session"] else "")
        return (str(latest), str(item["source_host"]))

    attention = sorted(
        [item for item in agent_rows if item["section"] == "attention"],
        key=sort_key,
        reverse=True,
    )
    active = sorted(
        [item for item in agent_rows if item["section"] == "active"],
        key=lambda item: (item["recent_turn_count"], item["recent_session_count"], item["last_seen_at"] or ""),
        reverse=True,
    )
    dormant = sorted(
        [item for item in agent_rows if item["section"] == "dormant"],
        key=sort_key,
        reverse=True,
    )

    return {
        "attention": attention,
        "active": active,
        "dormant": dormant,
        "counts": {
            "attention": len(attention),
            "active": len(active),
            "dormant": len(dormant),
            "all": len(agent_rows),
        },
    }
