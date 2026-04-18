from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote


OVERRIDE_SELECT = """
    o.override_group_key,
    o.override_organization,
    o.override_repository,
    o.override_remote_url,
    o.override_display_label,
    o.created_at AS override_created_at,
    o.updated_at AS override_updated_at
"""


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def project_detail_href(group_key: str) -> str:
    key = trimmed(group_key) or ""
    if key.startswith("github:"):
        slug = key.split(":", 1)[1]
        if "/" in slug:
            org, repo = slug.split("/", 1)
            return f"/projects/github/{quote(org, safe='')}/{quote(repo, safe='')}"
    if key.startswith("directory:"):
        parts = key.split(":", 2)
        if len(parts) == 3:
            host = parts[1]
            directory = parts[2].lstrip("/")
            return f"/projects/directory/{quote(host, safe='')}/{quote(directory, safe='/')}"
    return f"/projects/key/{quote(key, safe='')}"


def project_edit_href(group_key: str) -> str:
    key = trimmed(group_key) or ""
    return f"/projects/edit?key={quote(key, safe='')}"


def group_key_from_project_path(
    root: str,
    project: str,
    key: str | None = None,
) -> str:
    if root == "github":
        if not key:
            raise ValueError("Missing repository component for github project route")
        repo = key
        return f"github:{project}/{repo}".lower()
    if root == "directory":
        if not key:
            raise ValueError("Missing directory component for directory project route")
        directory = key
        directory_path = f"/{directory.lstrip('/')}" if directory else "unknown-directory"
        return f"directory:{project}:{directory_path}"
    if root == "key":
        return project
    raise ValueError(f"Unsupported project route root: {root}")


def joined_session_query(where_clause: str = "", order_clause: str = "") -> str:
    return f"""
        SELECT
            s.*,
            {OVERRIDE_SELECT}
        FROM sessions AS s
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        {where_clause}
        {order_clause}
    """


def visible_session_where(extra_conditions: list[str] | None = None) -> str:
    conditions = [
        """
        NOT EXISTS (
            SELECT 1
            FROM ignored_project_sources AS i
            WHERE i.match_project_key = s.inferred_project_key
        )
        """
    ]
    if extra_conditions:
        conditions.extend(extra_conditions)
    return "WHERE " + " AND ".join(conditions)


def effective_project_fields(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    source_host = trimmed(row["source_host"]) or "unknown-host"
    inferred_key = trimmed(row["inferred_project_key"]) or f"directory:{source_host}:{trimmed(row['cwd']) or row['id']}"
    override_key = trimmed(row["override_group_key"])
    override_remote_url = trimmed(row["override_remote_url"])

    organization = (
        trimmed(row["override_organization"])
        or trimmed(row["github_org"])
        or source_host
    )
    repository = (
        trimmed(row["override_repository"])
        or trimmed(row["github_repo"])
        or trimmed(row["cwd_name"])
        or trimmed(row["cwd"])
        or "unassigned"
    )
    remote_url = (
        trimmed(row["override_remote_url"])
        or trimmed(row["github_remote_url"])
        or trimmed(row["git_repository_url"])
    )
    display_label = (
        trimmed(row["override_display_label"])
        or (f"{organization}/{repository}" if organization and repository else repository)
        or inferred_key
    )
    effective_group_key = override_key or inferred_key
    effective_kind = trimmed(row["inferred_project_kind"]) or "directory"
    if effective_group_key.startswith("github:"):
        effective_kind = "github"
    elif effective_group_key.startswith("directory:"):
        effective_kind = "directory"
    elif override_remote_url and "github.com" in override_remote_url.lower():
        effective_kind = "github"

    return {
        "effective_group_key": effective_group_key,
        "effective_project_kind": effective_kind,
        "organization": organization,
        "repository": repository,
        "remote_url": remote_url,
        "display_label": display_label,
        "manual_override": any(
            trimmed(row[column])
            for column in (
                "override_group_key",
                "override_organization",
                "override_repository",
                "override_remote_url",
                "override_display_label",
            )
        ),
        "inferred_project_key": inferred_key,
        "inferred_project_label": trimmed(row["inferred_project_label"]) or display_label,
        "inferred_project_kind": trimmed(row["inferred_project_kind"]) or "directory",
        "source_host": source_host,
        "cwd": trimmed(row["cwd"]),
        "cwd_name": trimmed(row["cwd_name"]) or repository,
        "github_slug": trimmed(row["github_slug"]),
        "github_remote_url": trimmed(row["github_remote_url"]),
    }


def query_group_rows(
    connection: sqlite3.Connection,
    q: str | None = None,
    host: str | None = None,
) -> list[sqlite3.Row]:
    conditions = []
    params: list[Any] = []

    if q:
        like = f"%{q}%"
        conditions.append(
            """
            (
                s.search_text LIKE ?
                OR s.github_slug LIKE ?
                OR s.github_org LIKE ?
                OR s.github_repo LIKE ?
                OR s.source_host LIKE ?
                OR s.cwd LIKE ?
                OR o.override_display_label LIKE ?
                OR o.override_organization LIKE ?
                OR o.override_repository LIKE ?
            )
            """
        )
        params.extend([like] * 9)
    if host:
        conditions.append("s.source_host LIKE ?")
        params.append(f"%{host}%")

    return connection.execute(
        joined_session_query(
            visible_session_where(conditions),
            "ORDER BY COALESCE(s.session_timestamp, s.started_at, s.imported_at) DESC",
        ),
        params,
    ).fetchall()


@dataclass(slots=True)
class GroupedProject:
    key: str
    organization: str
    repository: str
    display_label: str
    remote_url: str | None
    inferred_kind: str
    latest_timestamp: str | None
    latest_session_id: str | None
    latest_summary: str | None
    session_count: int
    event_count: int
    host_count: int
    hosts: list[str]
    directories: list[str]
    source_project_count: int
    manual_override: bool
    detail_href: str


def build_grouped_projects(rows: list[sqlite3.Row], limit: int | None = None) -> list[GroupedProject]:
    grouped: dict[str, dict[str, Any]] = {}

    for row in rows:
        project = effective_project_fields(row)
        group = grouped.setdefault(
            project["effective_group_key"],
            {
                "key": project["effective_group_key"],
                "organization": project["organization"],
                "repository": project["repository"],
                "display_label": project["display_label"],
                "remote_url": project["remote_url"],
                "inferred_kind": project["effective_project_kind"],
                "latest_timestamp": row["session_timestamp"] or row["started_at"] or row["imported_at"],
                "latest_session_id": row["id"],
                "latest_summary": row["summary"],
                "session_count": 0,
                "event_count": 0,
                "hosts": set(),
                "directories": set(),
                "source_project_keys": set(),
                "manual_override": False,
            },
        )

        timestamp = row["session_timestamp"] or row["started_at"] or row["imported_at"]
        if timestamp and (group["latest_timestamp"] is None or timestamp > group["latest_timestamp"]):
            group["latest_timestamp"] = timestamp
            group["latest_session_id"] = row["id"]
            group["latest_summary"] = row["summary"]

        group["session_count"] += 1
        group["event_count"] += int(row["event_count"] or 0)
        if project["source_host"]:
            group["hosts"].add(project["source_host"])
        if project["cwd"]:
            group["directories"].add(project["cwd"])
        group["source_project_keys"].add(project["inferred_project_key"])
        group["manual_override"] = group["manual_override"] or project["manual_override"]

    projects = [
        GroupedProject(
            key=group["key"],
            organization=group["organization"],
            repository=group["repository"],
            display_label=group["display_label"],
            remote_url=group["remote_url"],
            inferred_kind=group["inferred_kind"],
            latest_timestamp=group["latest_timestamp"],
            latest_session_id=group["latest_session_id"],
            latest_summary=group["latest_summary"],
            session_count=group["session_count"],
            event_count=group["event_count"],
            host_count=len(group["hosts"]),
            hosts=sorted(group["hosts"]),
            directories=sorted(group["directories"]),
            source_project_count=len(group["source_project_keys"]),
            manual_override=group["manual_override"],
            detail_href=project_detail_href(group["key"]),
        )
        for group in grouped.values()
    ]

    projects.sort(key=lambda item: item.latest_timestamp or "", reverse=True)
    if limit is not None:
        return projects[:limit]
    return projects


def dashboard_stats(rows: list[sqlite3.Row]) -> dict[str, int]:
    hosts = {
        effective_project_fields(row)["source_host"]
        for row in rows
        if effective_project_fields(row)["source_host"]
    }
    organizations = {
        effective_project_fields(row)["organization"]
        for row in rows
        if effective_project_fields(row)["organization"]
    }
    return {
        "sessions": len(rows),
        "events": sum(int(row["event_count"] or 0) for row in rows),
        "projects": len(build_grouped_projects(rows)),
        "hosts": len(hosts),
        "organizations": len(organizations),
    }


def fetch_session_with_project(
    connection: sqlite3.Connection,
    session_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        joined_session_query("WHERE s.id = ?"),
        (session_id,),
    ).fetchone()


def fetch_group_detail(
    connection: sqlite3.Connection,
    group_key: str,
) -> dict[str, Any] | None:
    rows = query_group_rows(connection)
    matching_rows = [
        row for row in rows if effective_project_fields(row)["effective_group_key"] == group_key
    ]
    if not matching_rows:
        return None

    grouped_projects = build_grouped_projects(matching_rows)
    group = grouped_projects[0]

    source_groups: list[dict[str, Any]] = []
    by_source: dict[str, dict[str, Any]] = {}
    for row in matching_rows:
        project = effective_project_fields(row)
        source_group = by_source.setdefault(
            project["inferred_project_key"],
            {
                "inferred_project_key": project["inferred_project_key"],
                "inferred_project_label": project["inferred_project_label"],
                "inferred_project_kind": project["inferred_project_kind"],
                "source_host": project["source_host"],
                "cwd": project["cwd"],
                "cwd_name": project["cwd_name"],
                "github_slug": project["github_slug"],
                "github_remote_url": project["github_remote_url"],
                "organization": project["organization"],
                "repository": project["repository"],
                "display_label": project["display_label"],
                "effective_group_key": project["effective_group_key"],
                "override_group_key": trimmed(row["override_group_key"]) or "",
                "override_organization": trimmed(row["override_organization"]) or "",
                "override_repository": trimmed(row["override_repository"]) or "",
                "override_remote_url": trimmed(row["override_remote_url"]) or "",
                "override_display_label": trimmed(row["override_display_label"]) or "",
                "sessions": [],
            },
        )
        source_group["sessions"].append(
            {
                "id": row["id"],
                "summary": row["summary"],
                "session_timestamp": row["session_timestamp"] or row["started_at"] or row["imported_at"],
                "event_count": row["event_count"],
                "host": project["source_host"],
                "cwd": project["cwd"],
            }
        )

    for source_group in by_source.values():
        source_group["sessions"].sort(
            key=lambda item: item["session_timestamp"] or "",
            reverse=True,
        )
        source_groups.append(source_group)

    source_groups.sort(
        key=lambda item: item["sessions"][0]["session_timestamp"] if item["sessions"] else "",
        reverse=True,
    )

    return {
        "group": group,
        "source_groups": source_groups,
    }


def fetch_group_source_project_keys(
    connection: sqlite3.Connection,
    group_key: str,
) -> list[str]:
    rows = query_group_rows(connection)
    keys = {
        effective_project_fields(row)["inferred_project_key"]
        for row in rows
        if effective_project_fields(row)["effective_group_key"] == group_key
    }
    return sorted(keys)


def ignored_project_keys(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT match_project_key FROM ignored_project_sources"
    ).fetchall()
    return {
        row["match_project_key"]
        for row in rows
        if trimmed(row["match_project_key"])
    }


def project_is_ignored(connection: sqlite3.Connection, project_key: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM ignored_project_sources
        WHERE match_project_key = ?
        """,
        (project_key,),
    ).fetchone()
    return row is not None


def delete_sessions_for_project_keys(
    connection: sqlite3.Connection,
    project_keys: list[str],
) -> int:
    keys = [key for key in project_keys if trimmed(key)]
    if not keys:
        return 0
    placeholders = ", ".join("?" for _ in keys)
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM sessions
        WHERE inferred_project_key IN ({placeholders})
        """,
        keys,
    ).fetchone()
    count = int(row["count"] or 0) if row is not None else 0
    connection.execute(
        f"DELETE FROM sessions WHERE inferred_project_key IN ({placeholders})",
        keys,
    )
    return count


def ignore_project_keys(
    connection: sqlite3.Connection,
    project_keys: list[str],
) -> int:
    keys = sorted({key for key in project_keys if trimmed(key)})
    if not keys:
        return 0
    now = utc_now_iso()
    connection.executemany(
        """
        INSERT OR IGNORE INTO ignored_project_sources (match_project_key, created_at)
        VALUES (?, ?)
        """,
        [(key, now) for key in keys],
    )
    return len(keys)


def upsert_project_override(
    connection: sqlite3.Connection,
    match_project_key: str,
    override_group_key: str | None,
    override_organization: str | None,
    override_repository: str | None,
    override_remote_url: str | None,
    override_display_label: str | None,
) -> None:
    now = utc_now_iso()
    existing = connection.execute(
        "SELECT match_project_key FROM project_overrides WHERE match_project_key = ?",
        (match_project_key,),
    ).fetchone()
    values = (
        trimmed(override_group_key),
        trimmed(override_organization),
        trimmed(override_repository),
        trimmed(override_remote_url),
        trimmed(override_display_label),
    )

    if not any(values):
        connection.execute(
            "DELETE FROM project_overrides WHERE match_project_key = ?",
            (match_project_key,),
        )
        return

    if existing is None:
        connection.execute(
            """
            INSERT INTO project_overrides (
                match_project_key,
                override_group_key,
                override_organization,
                override_repository,
                override_remote_url,
                override_display_label,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                match_project_key,
                *values,
                now,
                now,
            ),
        )
        return

    connection.execute(
        """
        UPDATE project_overrides
        SET
            override_group_key = ?,
            override_organization = ?,
            override_repository = ?,
            override_remote_url = ?,
            override_display_label = ?,
            updated_at = ?
        WHERE match_project_key = ?
        """,
        (
            *values,
            now,
            match_project_key,
        ),
    )
