from __future__ import annotations

import hashlib
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote
from uuid import uuid4

from .session_rollups import activity_date_key
from .session_status import is_user_turn_start, prefers_event_msg_user_turns, terminal_turn_summary
from .text_utils import shorten, strip_codex_wrappers

PROJECT_VISIBILITIES = {"authenticated", "private"}
PROJECT_ACCESS_ROLES = {"viewer", "editor"}


OVERRIDE_SELECT = """
    o.override_group_key,
    o.override_organization,
    o.override_repository,
    o.override_remote_url,
    o.override_display_label,
    o.created_at AS override_created_at,
    o.updated_at AS override_updated_at
"""

GROUP_ROW_SELECT = f"""
    s.id,
    s.session_timestamp,
    s.started_at,
    s.imported_at,
    s.updated_at,
    s.summary,
    s.import_warning,
    s.event_count,
    s.turn_count,
    s.last_user_message,
    s.last_turn_timestamp,
    s.latest_turn_summary,
    s.command_failure_count,
    s.aborted_turn_count,
    s.source_host,
    s.cwd,
    s.cwd_name,
    s.git_repository_url,
    s.github_remote_url,
    s.github_org,
    s.github_repo,
    s.github_slug,
    s.inferred_project_kind,
    s.inferred_project_key,
    s.inferred_project_label,
    p.id AS project_id,
    p.current_group_key AS current_project_group_key,
    p.display_label AS current_project_display_label,
    p.visibility AS project_visibility,
    {OVERRIDE_SELECT}
"""

GROUP_KEY_MATCH_SQL = """
(
    o.override_group_key = ?
    OR (
        (o.override_group_key IS NULL OR TRIM(o.override_group_key) = '')
        AND s.inferred_project_key = ?
    )
)
"""

TURN_STREAM_SELECT = f"""
    st.session_id,
    st.turn_number,
    st.prompt_excerpt,
    st.prompt_timestamp,
    st.response_excerpt,
    st.response_timestamp,
    st.response_state,
    st.latest_timestamp,
    st.command_count,
    st.patch_count,
    st.failure_count,
    st.files_touched_count,
    s.session_timestamp,
    s.started_at,
    s.imported_at,
    s.source_host,
    s.cwd,
    s.cwd_name,
    s.git_repository_url,
    s.github_remote_url,
    s.github_org,
    s.github_repo,
    s.github_slug,
    s.inferred_project_kind,
    s.inferred_project_key,
    s.inferred_project_label,
    s.import_warning,
    p.id AS project_id,
    p.current_group_key AS current_project_group_key,
    p.display_label AS current_project_display_label,
    p.visibility AS project_visibility,
    {OVERRIDE_SELECT}
"""


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def count_label(count: int, singular: str, plural: str | None = None) -> str:
    suffix = singular if count == 1 else (plural or f"{singular}s")
    return f"{count} {suffix}"


def normalize_project_visibility(value: str | None) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if candidate in PROJECT_VISIBILITIES else "authenticated"


def normalize_project_acl_role(value: str | None) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if candidate in PROJECT_ACCESS_ROLES else "viewer"


def _project_acl_role_rank(value: str | None) -> int:
    role = normalize_project_acl_role(value)
    return 2 if role == "editor" else 1


@dataclass(slots=True)
class ProjectAccessContext:
    auth_enabled: bool
    bypass: bool
    user_id: str | None
    project_roles: dict[str, str]


def build_command_exit_badges(
    *,
    command_exits: int = 0,
    tone: str = "stone",
) -> list[dict[str, str]]:
    if command_exits <= 0:
        return []
    return [
        {
            "tone": tone,
            "label": count_label(command_exits, "command exit"),
        }
    ]


def summarize_attention_status(
    *,
    recent_turn_count: int = 0,
) -> dict[str, str | int | bool]:
    if recent_turn_count > 0:
        return {
            "status_tone": "emerald",
            "status_label": "Active",
            "status_title": count_label(recent_turn_count, "turn") + " in the last 7 days",
            "has_attention": False,
            "attention_count": 0,
        }
    return {
        "status_tone": "stone",
        "status_label": "Idle",
        "status_title": "No recent turn activity",
        "has_attention": False,
        "attention_count": 0,
    }


def slugify_project_segment(value: str | None, fallback: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")
    return normalized or fallback


def project_route_segments(project: "GroupedProject") -> tuple[str, str]:
    if project.inferred_kind == "github":
        owner = project.organization or "project"
    else:
        owner = project.hosts[0] if project.hosts else project.organization or "project"

    project_name = project.repository or project.display_label or "project"
    return (
        slugify_project_segment(owner, "project"),
        slugify_project_segment(project_name, "project"),
    )


def project_short_key(group_key: str) -> str:
    key = trimmed(group_key) or "project"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:8]


def project_detail_href_for_route(owner_slug: str, project_slug: str) -> str:
    return f"/projects/{quote(owner_slug, safe='')}/{quote(project_slug, safe='')}"


def project_edit_href(detail_href: str) -> str:
    return f"{detail_href.rstrip('/')}/edit"


def assign_project_detail_hrefs(projects: list["GroupedProject"]) -> None:
    by_route: dict[tuple[str, str], list[GroupedProject]] = {}
    for project in projects:
        by_route.setdefault(project_route_segments(project), []).append(project)

    for (owner_slug, project_slug), grouped_projects in by_route.items():
        if len(grouped_projects) == 1:
            grouped_projects[0].detail_href = project_detail_href_for_route(owner_slug, project_slug)
            continue

        for project in grouped_projects:
            project.detail_href = project_detail_href_for_route(
                owner_slug,
                f"{project_slug}--{project_short_key(project.key)}",
            )


def build_project_route_map(projects: list["GroupedProject"]) -> dict[str, str]:
    route_projects = [
        GroupedProject(
            project_id=project.project_id,
            visibility=project.visibility,
            key=project.key,
            organization=project.organization,
            repository=project.repository,
            display_label=project.display_label,
            remote_url=project.remote_url,
            inferred_kind=project.inferred_kind,
            latest_timestamp=project.latest_timestamp,
            latest_session_id=project.latest_session_id,
            latest_summary=project.latest_summary,
            session_count=project.session_count,
            turn_count=project.turn_count,
            event_count=project.event_count,
            host_count=project.host_count,
            hosts=list(project.hosts),
            directories=list(project.directories),
            source_project_count=project.source_project_count,
            manual_override=project.manual_override,
            detail_href="",
        )
        for project in projects
    ]
    assign_project_detail_hrefs(route_projects)
    return {project.key: project.detail_href for project in route_projects}


def joined_session_query(
    where_clause: str = "",
    order_clause: str = "",
    select_clause: str = (
        "s.*, "
        + OVERRIDE_SELECT
        + ", p.id AS project_id, p.current_group_key AS current_project_group_key, "
        + "p.display_label AS current_project_display_label, p.visibility AS project_visibility"
    ),
) -> str:
    return f"""
        SELECT
            {select_clause}
        FROM sessions AS s
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        LEFT JOIN project_sources AS ps
            ON ps.match_project_key = s.inferred_project_key
        LEFT JOIN projects AS p
            ON p.id = ps.project_id
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
        "project_id": trimmed(row["project_id"]),
        "project_visibility": normalize_project_visibility(row["project_visibility"]),
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


def build_project_access_context(
    connection: sqlite3.Connection,
    *,
    auth_user: dict[str, Any] | None = None,
    auth_enabled: bool = False,
) -> ProjectAccessContext:
    if not auth_enabled:
        return ProjectAccessContext(
            auth_enabled=False,
            bypass=True,
            user_id=None,
            project_roles={},
        )

    user = auth_user or {}
    if bool(user.get("is_admin")) or str(user.get("role") or "").strip().lower() == "admin":
        return ProjectAccessContext(
            auth_enabled=True,
            bypass=True,
            user_id=str(user.get("user_id") or "").strip() or None,
            project_roles={},
        )

    user_id = str(user.get("user_id") or "").strip() or None
    if not user_id:
        return ProjectAccessContext(
            auth_enabled=True,
            bypass=False,
            user_id=None,
            project_roles={},
        )

    rows = connection.execute(
        """
        SELECT project_id, role
        FROM project_acl
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchall()
    return ProjectAccessContext(
        auth_enabled=True,
        bypass=False,
        user_id=user_id,
        project_roles={
            str(row["project_id"]): normalize_project_acl_role(row["role"])
            for row in rows
            if trimmed(row["project_id"])
        },
    )


def row_is_visible_to_project_access(
    row: sqlite3.Row | dict[str, Any],
    access: ProjectAccessContext | None,
) -> bool:
    if access is None or access.bypass:
        return True
    project_visibility = normalize_project_visibility(row["project_visibility"])
    if project_visibility != "private":
        return True
    project_id = trimmed(row["project_id"])
    return bool(project_id and project_id in access.project_roles)


def filter_rows_for_project_access(
    rows: list[sqlite3.Row],
    access: ProjectAccessContext | None,
) -> list[sqlite3.Row]:
    if access is None or access.bypass:
        return rows
    return [row for row in rows if row_is_visible_to_project_access(row, access)]


def can_edit_project(
    project_id: str | None,
    access: ProjectAccessContext | None,
) -> bool:
    if access is None or access.bypass:
        return True
    project_key = trimmed(project_id)
    if not project_key:
        return False
    return normalize_project_acl_role(access.project_roles.get(project_key)) == "editor"


def list_project_acl_members(
    connection: sqlite3.Connection,
    project_id: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            pa.project_id,
            pa.user_id,
            pa.role,
            pa.granted_by_user_id,
            pa.created_at,
            pa.updated_at,
            u.username,
            u.display_name,
            u.email,
            u.auth_source,
            u.disabled_at
        FROM project_acl AS pa
        INNER JOIN users AS u
            ON u.id = pa.user_id
        WHERE pa.project_id = ?
        ORDER BY
            CASE WHEN pa.role = 'editor' THEN 0 ELSE 1 END ASC,
            LOWER(COALESCE(NULLIF(TRIM(u.display_name), ''), u.username)) ASC
        """,
        (project_id,),
    ).fetchall()
    return [
        {
            "project_id": str(row["project_id"]),
            "user_id": str(row["user_id"]),
            "role": normalize_project_acl_role(row["role"]),
            "username": str(row["username"] or ""),
            "display_name": str(row["display_name"] or row["username"] or ""),
            "email": str(row["email"] or ""),
            "auth_source": str(row["auth_source"] or ""),
            "disabled_at": str(row["disabled_at"] or ""),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
        for row in rows
    ]


def update_project_visibility(
    connection: sqlite3.Connection,
    project_id: str,
    visibility: str,
) -> None:
    connection.execute(
        """
        UPDATE projects
        SET visibility = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            normalize_project_visibility(visibility),
            utc_now_iso(),
            project_id,
        ),
    )


def upsert_project_acl_member(
    connection: sqlite3.Connection,
    *,
    project_id: str,
    user_id: str,
    role: str,
    granted_by_user_id: str | None,
) -> None:
    now = utc_now_iso()
    connection.execute(
        """
        INSERT INTO project_acl (
            project_id,
            user_id,
            role,
            granted_by_user_id,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(project_id, user_id)
        DO UPDATE SET
            role = excluded.role,
            granted_by_user_id = excluded.granted_by_user_id,
            updated_at = excluded.updated_at
        """,
        (
            project_id,
            user_id,
            normalize_project_acl_role(role),
            trimmed(granted_by_user_id),
            now,
            now,
        ),
    )


def remove_project_acl_member(
    connection: sqlite3.Connection,
    *,
    project_id: str,
    user_id: str,
) -> None:
    connection.execute(
        """
        DELETE FROM project_acl
        WHERE project_id = ? AND user_id = ?
        """,
        (project_id, user_id),
    )


def _merge_project_acl_memberships(
    connection: sqlite3.Connection,
    *,
    from_project_id: str,
    to_project_id: str,
) -> None:
    if from_project_id == to_project_id:
        return
    rows = connection.execute(
        """
        SELECT user_id, role, granted_by_user_id, created_at, updated_at
        FROM project_acl
        WHERE project_id = ?
        """,
        (from_project_id,),
    ).fetchall()
    for row in rows:
        existing = connection.execute(
            """
            SELECT role
            FROM project_acl
            WHERE project_id = ? AND user_id = ?
            """,
            (to_project_id, str(row["user_id"])),
        ).fetchone()
        next_role = normalize_project_acl_role(row["role"])
        if existing is not None and _project_acl_role_rank(existing["role"]) >= _project_acl_role_rank(next_role):
            continue
        connection.execute(
            """
            INSERT INTO project_acl (
                project_id,
                user_id,
                role,
                granted_by_user_id,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, user_id)
            DO UPDATE SET
                role = excluded.role,
                granted_by_user_id = excluded.granted_by_user_id,
                updated_at = excluded.updated_at
            """,
            (
                to_project_id,
                str(row["user_id"]),
                next_role,
                trimmed(row["granted_by_user_id"]),
                str(row["created_at"] or utc_now_iso()),
                utc_now_iso(),
            ),
        )


def sync_project_registry(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        joined_session_query(
            visible_session_where(),
            "",
            """
            s.inferred_project_key,
            s.inferred_project_label,
            s.inferred_project_kind,
            s.source_host,
            s.cwd,
            s.cwd_name,
            s.git_repository_url,
            s.github_remote_url,
            s.github_org,
            s.github_repo,
            s.github_slug,
            p.id AS project_id,
            p.visibility AS project_visibility,
            """
            + OVERRIDE_SELECT,
        )
    ).fetchall()

    grouped: dict[str, dict[str, Any]] = {}
    active_source_keys: set[str] = set()
    for row in rows:
        project = effective_project_fields(row)
        source_key = str(project["inferred_project_key"] or "").strip()
        if not source_key:
            continue
        active_source_keys.add(source_key)
        entry = grouped.setdefault(
            str(project["effective_group_key"]),
            {
                "group_key": str(project["effective_group_key"]),
                "display_label": str(project["display_label"]),
                "source_keys": set(),
            },
        )
        entry["source_keys"].add(source_key)
        if not entry["display_label"] and project["display_label"]:
            entry["display_label"] = str(project["display_label"])

    existing_projects = connection.execute(
        """
        SELECT id, current_group_key, display_label, visibility, created_at
        FROM projects
        ORDER BY created_at ASC, id ASC
        """
    ).fetchall()
    project_by_id = {str(row["id"]): row for row in existing_projects}
    project_id_by_group_key = {
        str(row["current_group_key"]): str(row["id"])
        for row in existing_projects
        if trimmed(row["current_group_key"])
    }
    source_rows = connection.execute(
        """
        SELECT match_project_key, project_id
        FROM project_sources
        """
    ).fetchall()
    source_to_project_id = {
        str(row["match_project_key"]): str(row["project_id"])
        for row in source_rows
        if trimmed(row["match_project_key"]) and trimmed(row["project_id"])
    }

    now = utc_now_iso()
    claimed_project_ids: set[str] = set()
    target_sources: dict[str, str] = {}
    merged_project_pairs: set[tuple[str, str]] = set()
    project_targets: dict[str, dict[str, Any]] = {}

    for group_key in sorted(grouped):
        entry = grouped[group_key]
        source_keys = sorted(str(key) for key in entry["source_keys"] if trimmed(key))
        candidate_ids = [
            source_to_project_id[source_key]
            for source_key in source_keys
            if source_to_project_id.get(source_key) in project_by_id
        ]
        counts = Counter(candidate_ids)
        exact_project_id = project_id_by_group_key.get(group_key)

        chosen_project_id: str | None = None
        if exact_project_id and exact_project_id not in claimed_project_ids and (exact_project_id in counts or not counts):
            chosen_project_id = exact_project_id
        else:
            for project_id, _count in counts.most_common():
                if project_id not in claimed_project_ids:
                    chosen_project_id = project_id
                    break

        if chosen_project_id is None:
            chosen_project_id = uuid4().hex

        project_targets[chosen_project_id] = {
            "group_key": group_key,
            "display_label": str(entry["display_label"] or group_key),
            "is_new": chosen_project_id not in project_by_id,
        }

        claimed_project_ids.add(chosen_project_id)
        for source_key in source_keys:
            target_sources[source_key] = chosen_project_id
        for candidate_id in counts:
            if candidate_id != chosen_project_id:
                merged_project_pairs.add((candidate_id, chosen_project_id))

    for project_id, target in project_targets.items():
        if bool(target["is_new"]):
            connection.execute(
                """
                INSERT INTO projects (
                    id,
                    current_group_key,
                    display_label,
                    visibility,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, 'authenticated', ?, ?)
                """,
                (
                    project_id,
                    f"pending:{project_id}",
                    str(target["display_label"]),
                    now,
                    now,
                ),
            )
            continue

        existing_group_key = trimmed(project_by_id[project_id]["current_group_key"])
        if existing_group_key != str(target["group_key"]):
            connection.execute(
                """
                UPDATE projects
                SET current_group_key = ?, updated_at = ?
                WHERE id = ?
                """,
                (f"pending:{project_id}", now, project_id),
            )

    for project_id, target in project_targets.items():
        connection.execute(
            """
            UPDATE projects
            SET
                current_group_key = ?,
                display_label = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                str(target["group_key"]),
                str(target["display_label"]),
                now,
                project_id,
            ),
        )

    for from_project_id, to_project_id in sorted(merged_project_pairs):
        _merge_project_acl_memberships(
            connection,
            from_project_id=from_project_id,
            to_project_id=to_project_id,
        )

    for source_key, project_id in target_sources.items():
        connection.execute(
            """
            INSERT INTO project_sources (
                match_project_key,
                project_id,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(match_project_key)
            DO UPDATE SET
                project_id = excluded.project_id,
                updated_at = excluded.updated_at
            """,
            (source_key, project_id, now, now),
        )

    if active_source_keys:
        placeholders = ", ".join("?" for _ in active_source_keys)
        connection.execute(
            f"""
            DELETE FROM project_sources
            WHERE match_project_key NOT IN ({placeholders})
            """,
            sorted(active_source_keys),
        )
    else:
        connection.execute("DELETE FROM project_sources")

    connection.execute(
        """
        DELETE FROM projects
        WHERE id NOT IN (
            SELECT DISTINCT project_id
            FROM project_sources
        )
        """
    )


def fetch_session_stream_summaries(
    connection: sqlite3.Connection,
    session_ids: list[str],
) -> dict[str, str]:
    ids = sorted({session_id for session_id in session_ids if trimmed(session_id)})
    if not ids:
        return {}

    placeholders = ", ".join("?" for _ in ids)
    user_rows = connection.execute(
        f"""
        SELECT
            session_id,
            event_index,
            record_type,
            payload_type,
            kind,
            role,
            display_text
        FROM events
        WHERE session_id IN ({placeholders})
          AND kind = 'message'
          AND role = 'user'
          AND (
            (record_type = 'event_msg' AND payload_type = 'user_message')
            OR (record_type = 'response_item' AND payload_type = 'message')
          )
        ORDER BY session_id ASC, event_index ASC
        """,
        ids,
    ).fetchall()

    user_events_by_session: dict[str, list[sqlite3.Row]] = {}
    for row in user_rows:
        user_events_by_session.setdefault(row["session_id"], []).append(row)

    last_turn_starts: dict[str, int] = {}
    for session_id, rows in user_events_by_session.items():
        prefer_event_msg = prefers_event_msg_user_turns(rows)
        for row in rows:
            if is_user_turn_start(row, prefer_event_msg):
                last_turn_starts[session_id] = int(row["event_index"])

    if not last_turn_starts:
        return {}

    start_items = sorted(last_turn_starts.items())
    start_values = ", ".join("(?, ?)" for _ in start_items)
    start_params: list[object] = []
    for session_id, event_index in start_items:
        start_params.extend((session_id, event_index))

    rows = connection.execute(
        f"""
        WITH last_turn(session_id, start_index) AS (
            VALUES {start_values}
        )
        SELECT
            e.session_id,
            e.event_index,
            e.record_type,
            e.payload_type,
            e.kind,
            e.role,
            e.display_text
        FROM events AS e
        INNER JOIN last_turn AS lt
            ON lt.session_id = e.session_id
           AND e.event_index >= lt.start_index
        WHERE
          (
            (
                e.kind = 'message'
                AND e.role IN ('user', 'assistant')
                AND (
                    (e.record_type = 'event_msg' AND e.payload_type = 'user_message')
                    OR (e.record_type = 'event_msg' AND e.payload_type = 'agent_message')
                    OR (e.record_type = 'response_item' AND e.payload_type = 'message')
                )
            )
            OR (
                e.record_type = 'event_msg'
                AND e.payload_type IN ('task_complete', 'turn_aborted')
            )
            OR e.kind IN ('tool_call', 'tool_result')
          )
        ORDER BY e.session_id ASC, e.event_index ASC
        """,
        start_params,
    ).fetchall()

    tail_events_by_session: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        tail_events_by_session.setdefault(row["session_id"], []).append(row)

    return {
        session_id: summary
        for session_id, events in tail_events_by_session.items()
        if (summary := terminal_turn_summary(events)) is not None
    }


def fetch_session_user_turn_metadata(
    connection: sqlite3.Connection,
    session_ids: list[str],
) -> dict[str, dict[str, str | int]]:
    ids = sorted({session_id for session_id in session_ids if trimmed(session_id)})
    if not ids:
        return {}

    placeholders = ", ".join("?" for _ in ids)
    rows = connection.execute(
        f"""
        SELECT
            session_id,
            event_index,
            record_type,
            payload_type,
            kind,
            role,
            timestamp,
            display_text
        FROM events
        WHERE session_id IN ({placeholders})
          AND kind = 'message'
          AND role = 'user'
          AND (
            (record_type = 'event_msg' AND payload_type = 'user_message')
            OR (record_type = 'response_item' AND payload_type = 'message')
          )
        ORDER BY session_id ASC, event_index ASC
        """,
        ids,
    ).fetchall()

    rows_by_session: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        rows_by_session.setdefault(row["session_id"], []).append(row)

    metadata: dict[str, dict[str, str | int]] = {}
    for session_id, session_rows in rows_by_session.items():
        prefer_event_msg = prefers_event_msg_user_turns(session_rows)
        turn_count = 0
        last_message = ""
        last_timestamp = ""
        for row in session_rows:
            if not is_user_turn_start(row, prefer_event_msg):
                continue
            cleaned = strip_codex_wrappers(str(row["display_text"] or "")).strip()
            if not cleaned:
                continue
            turn_count += 1
            last_message = shorten(cleaned, 220)
            last_timestamp = str(row["timestamp"] or "")
        if turn_count or last_message:
            metadata[session_id] = {
                "message": last_message,
                "timestamp": last_timestamp,
                "turn_count": turn_count,
            }
    return metadata


def fetch_recent_session_turn_activity(
    connection: sqlite3.Connection,
    session_ids: list[str],
    since_timestamp: str,
) -> dict[str, dict[str, str | int]]:
    return fetch_recent_session_turn_activity_windows(
        connection,
        session_ids,
        since_timestamp,
    )


def fetch_recent_session_turn_activity_windows(
    connection: sqlite3.Connection,
    session_ids: list[str],
    since_timestamp: str,
    secondary_since_timestamp: str | None = None,
) -> dict[str, dict[str, str | int]]:
    ids = sorted({session_id for session_id in session_ids if trimmed(session_id)})
    if not ids:
        return {}

    since_date = activity_date_key(since_timestamp)
    if not since_date:
        return {}
    secondary_date = activity_date_key(secondary_since_timestamp) if secondary_since_timestamp else None
    placeholders = ", ".join("?" for _ in ids)
    secondary_select = (
        "SUM(CASE WHEN activity_date >= ? THEN turn_count ELSE 0 END) AS secondary_turn_count"
        if secondary_date
        else "0 AS secondary_turn_count"
    )
    params: list[Any] = [*ids]
    if secondary_date:
        params.append(secondary_date)
    params.append(since_date)
    rows = connection.execute(
        f"""
        SELECT
            session_id,
            SUM(turn_count) AS turn_count,
            MAX(latest_timestamp) AS latest_timestamp,
            {secondary_select}
        FROM session_turn_activity_daily
        WHERE session_id IN ({placeholders})
          AND activity_date >= ?
        GROUP BY session_id
        ORDER BY session_id ASC
        """,
        params,
    ).fetchall()

    metadata: dict[str, dict[str, str | int]] = {}
    for row in rows:
        turn_count = int(row["turn_count"] or 0)
        if turn_count:
            item: dict[str, str | int] = {
                "turn_count": turn_count,
                "latest_timestamp": str(row["latest_timestamp"] or ""),
            }
            if secondary_date:
                item["secondary_turn_count"] = int(row["secondary_turn_count"] or 0)
            metadata[str(row["session_id"])] = item
    return metadata


def fetch_session_issue_counts(
    connection: sqlite3.Connection,
    session_ids: list[str],
) -> dict[str, dict[str, int]]:
    ids = sorted({session_id for session_id in session_ids if trimmed(session_id)})
    if not ids:
        return {}

    placeholders = ", ".join("?" for _ in ids)
    rows = connection.execute(
        f"""
        SELECT
            session_id,
            SUM(CASE WHEN exit_code IS NOT NULL AND exit_code != 0 THEN 1 ELSE 0 END) AS command_failures,
            SUM(
                CASE
                    WHEN record_type = 'event_msg' AND payload_type = 'turn_aborted'
                    THEN 1
                    ELSE 0
                END
            ) AS aborted_turns
        FROM events
        WHERE session_id IN ({placeholders})
        GROUP BY session_id
        """,
        ids,
    ).fetchall()
    return {
        row["session_id"]: {
            "command_failures": int(row["command_failures"] or 0),
            "aborted_turns": int(row["aborted_turns"] or 0),
        }
        for row in rows
    }


def query_group_rows(
    connection: sqlite3.Connection,
    q: str | None = None,
    host: str | None = None,
    *,
    project_access: ProjectAccessContext | None = None,
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

    rows = connection.execute(
        joined_session_query(
            visible_session_where(conditions),
            "ORDER BY COALESCE(s.session_timestamp, s.started_at, s.imported_at) DESC",
            GROUP_ROW_SELECT,
        ),
        params,
    ).fetchall()
    return filter_rows_for_project_access(rows, project_access)


def query_group_rows_for_key(
    connection: sqlite3.Connection,
    group_key: str,
    *,
    project_access: ProjectAccessContext | None = None,
) -> list[sqlite3.Row]:
    key = trimmed(group_key)
    if not key:
        return []

    rows = connection.execute(
        joined_session_query(
            visible_session_where([GROUP_KEY_MATCH_SQL]),
            "ORDER BY COALESCE(s.session_timestamp, s.started_at, s.imported_at) DESC",
            GROUP_ROW_SELECT,
        ),
        (key, key),
    ).fetchall()
    return filter_rows_for_project_access(rows, project_access)


def resolve_project_detail_hrefs(
    connection: sqlite3.Connection,
    group_keys: list[str],
    *,
    project_access: ProjectAccessContext | None = None,
) -> dict[str, str]:
    keys = {trimmed(key) for key in group_keys if trimmed(key)}
    if not keys:
        return {}
    groups = build_grouped_projects(query_group_rows(connection, project_access=project_access))
    return {
        group.key: group.detail_href
        for group in groups
        if group.key in keys
    }


def fetch_turn_stream(
    connection: sqlite3.Connection,
    *,
    page: int = 1,
    page_size: int = 40,
    group_key: str | None = None,
    detail_href_override: str | None = None,
) -> dict[str, Any]:
    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(10, min(int(page_size or 40), 100))
    offset = (normalized_page - 1) * normalized_page_size

    params: list[Any] = []
    extra_conditions: list[str] = []
    normalized_group_key = trimmed(group_key)
    if normalized_group_key:
        extra_conditions.append(GROUP_KEY_MATCH_SQL)
        params.extend([normalized_group_key, normalized_group_key])

    where_clause = visible_session_where(extra_conditions)
    from_clause = f"""
        FROM session_turns AS st
        JOIN sessions AS s
            ON s.id = st.session_id
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        {where_clause}
    """
    order_clause = """
        ORDER BY COALESCE(
            st.latest_timestamp,
            st.response_timestamp,
            st.prompt_timestamp,
            s.last_turn_timestamp,
            s.session_timestamp,
            s.started_at,
            s.imported_at
        ) DESC,
        st.session_id DESC,
        st.turn_number DESC
    """

    total_row = connection.execute(
        f"SELECT COUNT(*) AS count {from_clause}",
        params,
    ).fetchone()
    total_count = int(total_row["count"] or 0) if total_row is not None else 0

    rows = connection.execute(
        f"""
        SELECT
            {TURN_STREAM_SELECT}
        {from_clause}
        {order_clause}
        LIMIT ? OFFSET ?
        """,
        [*params, normalized_page_size, offset],
    ).fetchall()

    href_by_group = (
        {normalized_group_key: detail_href_override}
        if normalized_group_key and detail_href_override
        else resolve_project_detail_hrefs(
            connection,
            [effective_project_fields(row)["effective_group_key"] for row in rows],
        )
    )

    items: list[dict[str, Any]] = []
    for row in rows:
        project = effective_project_fields(row)
        effective_group_key = project["effective_group_key"]
        detail_href = href_by_group.get(effective_group_key)
        if not detail_href:
            detail_href = f"/groups?key={quote(effective_group_key, safe='')}"

        response_state = trimmed(row["response_state"]) or "missing"
        viewer_warning = trimmed(row["import_warning"]) or ""
        canceled_turns = 1 if response_state == "canceled" else 0
        command_exit_count = int(row["failure_count"] or 0)
        signal_badges = build_command_exit_badges(command_exits=command_exit_count)
        timestamp = (
            trimmed(row["latest_timestamp"])
            or trimmed(row["response_timestamp"])
            or trimmed(row["prompt_timestamp"])
            or trimmed(row["session_timestamp"])
            or trimmed(row["started_at"])
            or trimmed(row["imported_at"])
        )
        items.append(
            {
                "session_id": str(row["session_id"]),
                "turn_number": int(row["turn_number"] or 0),
                "timestamp": timestamp,
                "prompt_excerpt": trimmed(row["prompt_excerpt"]) or "No prompt excerpt",
                "response_excerpt": trimmed(row["response_excerpt"]) or "No assistant response captured.",
                "response_state": response_state,
                "status_tone": "amber" if response_state == "canceled" else ("sky" if response_state == "update" else "emerald"),
                "status_label": "Canceled" if response_state == "canceled" else ("Update" if response_state == "update" else "Final"),
                "command_count": int(row["command_count"] or 0),
                "patch_count": int(row["patch_count"] or 0),
                "failure_count": command_exit_count,
                "files_touched_count": int(row["files_touched_count"] or 0),
                "viewer_warning": viewer_warning,
                "signal_badges": signal_badges,
                "project_label": project["display_label"],
                "project_detail_href": detail_href,
                "host": project["source_host"],
                "session_href": f"/sessions/{quote(str(row['session_id']), safe='')}?turn={int(row['turn_number'] or 0)}",
                "audit_href": f"/sessions/{quote(str(row['session_id']), safe='')}?view=audit&turn={int(row['turn_number'] or 0)}",
            }
        )

    return {
        "items": items,
        "page": normalized_page,
        "page_size": normalized_page_size,
        "total_count": total_count,
        "has_prev": normalized_page > 1,
        "has_next": offset + len(items) < total_count,
        "page_count": max((total_count + normalized_page_size - 1) // normalized_page_size, 1),
        "showing_from": offset + 1 if items else 0,
        "showing_to": offset + len(items),
    }


def paginate_items(
    items: list[Any],
    *,
    page: int = 1,
    page_size: int = 24,
) -> dict[str, Any]:
    normalized_page_size = max(int(page_size or 24), 1)
    total_count = len(items)
    page_count = max((total_count + normalized_page_size - 1) // normalized_page_size, 1)
    normalized_page = min(max(int(page or 1), 1), page_count)
    offset = (normalized_page - 1) * normalized_page_size
    page_items = items[offset : offset + normalized_page_size]
    return {
        "items": page_items,
        "page": normalized_page,
        "page_size": normalized_page_size,
        "total_count": total_count,
        "has_prev": normalized_page > 1,
        "has_next": offset + len(page_items) < total_count,
        "page_count": page_count,
        "showing_from": offset + 1 if page_items else 0,
        "showing_to": offset + len(page_items),
    }


@dataclass(slots=True)
class GroupedProject:
    project_id: str | None
    visibility: str
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
    turn_count: int
    event_count: int
    host_count: int
    hosts: list[str]
    directories: list[str]
    source_project_count: int
    manual_override: bool
    detail_href: str


def _collect_grouped_projects(
    rows: list[sqlite3.Row],
    summary_overrides: dict[str, str] | None = None,
    turn_metadata: dict[str, dict[str, str | int]] | None = None,
) -> list[GroupedProject]:
    grouped: dict[str, dict[str, Any]] = {}

    for row in rows:
        project = effective_project_fields(row)
        row_summary = (
            (summary_overrides or {}).get(row["id"])
            or trimmed(row["latest_turn_summary"])
            or row["summary"]
        )
        row_turn_count = int((turn_metadata or {}).get(row["id"], {}).get("turn_count", row["turn_count"]) or 0)
        row_latest_timestamp = (
            (turn_metadata or {}).get(row["id"], {}).get("timestamp")
            or row["last_turn_timestamp"]
            or row["session_timestamp"]
            or row["started_at"]
            or row["imported_at"]
        )
        group = grouped.setdefault(
            project["effective_group_key"],
            {
                "project_id": project["project_id"],
                "visibility": project["project_visibility"],
                "key": project["effective_group_key"],
                "organization": project["organization"],
                "repository": project["repository"],
                "display_label": project["display_label"],
                "remote_url": project["remote_url"],
                "inferred_kind": project["effective_project_kind"],
                "latest_timestamp": row_latest_timestamp,
                "latest_session_id": row["id"],
                "latest_summary": row_summary,
                "session_count": 0,
                "turn_count": 0,
                "event_count": 0,
                "hosts": set(),
                "directories": set(),
                "source_project_keys": set(),
                "manual_override": False,
            },
        )

        if row_latest_timestamp and (group["latest_timestamp"] is None or row_latest_timestamp > group["latest_timestamp"]):
            group["latest_timestamp"] = row_latest_timestamp
            group["latest_session_id"] = row["id"]
            group["latest_summary"] = row_summary

        group["session_count"] += 1
        group["turn_count"] += row_turn_count
        group["event_count"] += int(row["event_count"] or 0)
        if project["source_host"]:
            group["hosts"].add(project["source_host"])
        if project["cwd"]:
            group["directories"].add(project["cwd"])
        group["source_project_keys"].add(project["inferred_project_key"])
        group["manual_override"] = group["manual_override"] or project["manual_override"]

    return [
        GroupedProject(
            project_id=trimmed(group["project_id"]),
            visibility=normalize_project_visibility(group["visibility"]),
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
            turn_count=group["turn_count"],
            event_count=group["event_count"],
            host_count=len(group["hosts"]),
            hosts=sorted(group["hosts"]),
            directories=sorted(group["directories"]),
            source_project_count=len(group["source_project_keys"]),
            manual_override=group["manual_override"],
            detail_href="",
        )
        for group in grouped.values()
    ]


def build_grouped_projects(
    rows: list[sqlite3.Row],
    limit: int | None = None,
    route_rows: list[sqlite3.Row] | None = None,
    summary_overrides: dict[str, str] | None = None,
    turn_metadata: dict[str, dict[str, str | int]] | None = None,
) -> list[GroupedProject]:
    projects = _collect_grouped_projects(
        rows,
        summary_overrides=summary_overrides,
        turn_metadata=turn_metadata,
    )
    route_projects = projects if route_rows is None or route_rows is rows else _collect_grouped_projects(route_rows)
    route_map = build_project_route_map(route_projects)
    for project in projects:
        project.detail_href = route_map.get(
            project.key,
            project_detail_href_for_route(
                *project_route_segments(project),
            ),
        )

    projects.sort(key=lambda item: item.latest_timestamp or "", reverse=True)
    if limit is not None:
        return projects[:limit]
    return projects


def dashboard_stats(
    rows: list[sqlite3.Row],
    turn_metadata: dict[str, dict[str, str | int]] | None = None,
) -> dict[str, int]:
    hosts: set[str] = set()
    organizations: set[str] = set()
    project_keys: set[str] = set()
    for row in rows:
        project = effective_project_fields(row)
        if project["source_host"]:
            hosts.add(project["source_host"])
        if project["organization"]:
            organizations.add(project["organization"])
        if project["effective_group_key"]:
            project_keys.add(project["effective_group_key"])
    return {
        "sessions": len(rows),
        "turns": sum(int((turn_metadata or {}).get(row["id"], {}).get("turn_count", row["turn_count"]) or 0) for row in rows),
        "events": sum(int(row["event_count"] or 0) for row in rows),
        "projects": len(project_keys),
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
    *,
    sessions_page: int = 1,
    sessions_page_size: int = 24,
    project_access: ProjectAccessContext | None = None,
) -> dict[str, Any] | None:
    matching_rows = query_group_rows_for_key(connection, group_key, project_access=project_access)
    if not matching_rows:
        return None

    grouped_projects = build_grouped_projects(matching_rows)
    group = next((item for item in grouped_projects if item.key == group_key), None)
    if group is None:
        return None

    now = datetime.now().astimezone()
    recent_turn_activity = fetch_recent_session_turn_activity_windows(
        connection,
        [str(row["id"]) for row in matching_rows],
        (now - timedelta(days=7)).isoformat(),
    )

    source_groups: list[dict[str, Any]] = []
    by_source: dict[str, dict[str, Any]] = {}
    host_summaries: dict[str, dict[str, Any]] = {}
    signal_summary = {
        "canceled_turns": 0,
        "viewer_warnings": 0,
        "attention_sessions": 0,
    }
    attention_sessions: list[dict[str, Any]] = []
    all_sessions: list[dict[str, Any]] = []
    for row in matching_rows:
        project = effective_project_fields(row)
        aborted_turns = int(row["aborted_turn_count"] or 0)
        viewer_warning = trimmed(row["import_warning"]) or ""
        recent_turns = int(recent_turn_activity.get(str(row["id"]), {}).get("turn_count", 0) or 0)
        session_status = summarize_attention_status(
            recent_turn_count=recent_turns,
        )
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
        session_title = trimmed(row["last_user_message"]) or trimmed(row["latest_turn_summary"]) or row["summary"]
        session_timestamp = row["session_timestamp"] or row["started_at"] or row["imported_at"]
        session_when = trimmed(row["last_turn_timestamp"]) or session_timestamp or ""
        session_item = {
            "id": row["id"],
            "href": f"/sessions/{quote(str(row['id']), safe='')}",
            "summary": trimmed(row["latest_turn_summary"]) or row["summary"],
            "last_user_message": trimmed(row["last_user_message"]) or "",
            "last_turn_timestamp": trimmed(row["last_turn_timestamp"]) or "",
            "turn_count": int(row["turn_count"] or 0),
            "session_timestamp": session_timestamp,
            "event_count": row["event_count"],
            "host": project["source_host"],
            "cwd": project["cwd"],
            "aborted_turn_count": aborted_turns,
            "viewer_warning": viewer_warning,
            "has_viewer_warning": bool(viewer_warning),
            "recent_turn_count": recent_turns,
            "signal_badges": [],
            "needs_attention": bool(session_status["has_attention"]),
            "status_tone": str(session_status["status_tone"]),
            "status_label": str(session_status["status_label"]),
            "status_title": str(session_status["status_title"]),
            "project_label": project["display_label"],
        }
        source_group["sessions"].append(session_item)
        all_sessions.append(session_item)

        host_summary = host_summaries.setdefault(
            project["source_host"],
            {
                "source_host": project["source_host"],
                "last_seen_at": session_when,
                "session_count": 0,
                "turn_count": 0,
                "recent_turn_count": 0,
                "aborted_turn_count": 0,
                "latest_session": None,
                "latest_failed_session": None,
            },
        )
        host_summary["session_count"] += 1
        host_summary["turn_count"] += int(row["turn_count"] or 0)
        host_summary["recent_turn_count"] += recent_turns
        host_summary["aborted_turn_count"] += aborted_turns
        if session_when and session_when > str(host_summary["last_seen_at"] or ""):
            host_summary["last_seen_at"] = session_when
        if host_summary["latest_session"] is None or session_when > str(host_summary["latest_session"]["timestamp"] or ""):
            host_summary["latest_session"] = {
                "href": session_item["href"],
                "title": session_title or "Session",
                "timestamp": session_when,
            }
        if session_item["needs_attention"] and (
            host_summary["latest_failed_session"] is None
            or session_when > str(host_summary["latest_failed_session"]["timestamp"] or "")
        ):
            host_summary["latest_failed_session"] = {
                "href": session_item["href"],
                "title": session_title or "Session needing attention",
                "timestamp": session_when,
            }

        signal_summary["canceled_turns"] += aborted_turns
        if viewer_warning:
            signal_summary["viewer_warnings"] += 1
        if session_item["needs_attention"]:
            signal_summary["attention_sessions"] += 1
            attention_sessions.append(
                {
                    "id": row["id"],
                    "href": f"/sessions/{row['id']}",
                    "title": session_title or "Session needing attention",
                    "host": project["source_host"],
                    "timestamp": session_when,
                    "summary": trimmed(row["latest_turn_summary"]) or row["summary"] or "",
                    "aborted_turn_count": aborted_turns,
                    "viewer_warning": viewer_warning,
                    "signal_badges": session_item["signal_badges"],
                    "status_tone": session_item["status_tone"],
                    "status_label": session_item["status_label"],
                }
            )

    for source_group in by_source.values():
        source_group["sessions"].sort(
            key=lambda item: (item["last_user_message"] or "").lower(),
        )
        source_group["sessions"].sort(
            key=lambda item: item["last_turn_timestamp"] or item["session_timestamp"] or "",
            reverse=True,
        )
        source_groups.append(source_group)

    source_groups.sort(
        key=lambda item: (
            (
                item["sessions"][0]["last_turn_timestamp"]
                or item["sessions"][0]["session_timestamp"]
            )
            if item["sessions"]
            else ""
        ),
        reverse=True,
    )
    attention_sessions.sort(
        key=lambda item: (
            1 if item["viewer_warning"] else 0,
            int(item["aborted_turn_count"]),
            str(item["timestamp"] or ""),
        ),
        reverse=True,
    )
    all_sessions.sort(
        key=lambda item: item["last_turn_timestamp"] or item["session_timestamp"] or "",
        reverse=True,
    )

    recent_sessions = all_sessions[:8]
    all_sessions_page = paginate_items(
        all_sessions,
        page=sessions_page,
        page_size=sessions_page_size,
    )
    health_summary = summarize_attention_status(
        recent_turn_count=sum(int(item.get("turn_count", 0) or 0) for item in recent_turn_activity.values()),
    )
    latest_session = all_sessions[0] if all_sessions else None
    host_rows = sorted(
        host_summaries.values(),
        key=lambda item: (
            int(item["recent_turn_count"]),
            int(item["turn_count"]),
            str(item["last_seen_at"] or ""),
        ),
        reverse=True,
    )

    return {
        "group": group,
        "source_groups": source_groups,
        "signal_summary": signal_summary,
        "attention_sessions": attention_sessions,
        "recent_sessions": recent_sessions,
        "all_sessions_page": all_sessions_page,
        "host_summaries": host_rows,
        "status_strip": {
            "last_activity_at": group.latest_timestamp,
            "last_host": latest_session["host"] if latest_session else "",
            "recent_turn_count": sum(int(item.get("turn_count", 0) or 0) for item in recent_turn_activity.values()),
            "health_tone": str(health_summary["status_tone"]),
            "health_label": str(health_summary["status_label"]),
            "health_title": str(health_summary["status_title"]),
        },
    }


def resolve_project_detail_href(
    connection: sqlite3.Connection,
    group_key: str,
    *,
    project_access: ProjectAccessContext | None = None,
) -> str:
    groups = build_grouped_projects(query_group_rows(connection, project_access=project_access))
    for group in groups:
        if group.key == group_key:
            return group.detail_href
    return f"/groups?key={quote(group_key, safe='')}"


def resolve_group_key_from_detail_path(
    connection: sqlite3.Connection,
    owner_slug: str,
    project_slug: str,
    *,
    project_access: ProjectAccessContext | None = None,
) -> str | None:
    target = project_detail_href_for_route(owner_slug, project_slug)
    groups = build_grouped_projects(query_group_rows(connection, project_access=project_access))
    for group in groups:
        if group.detail_href == target:
            return group.key
    return None


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
