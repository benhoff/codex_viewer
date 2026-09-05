from __future__ import annotations

import hashlib
import posixpath
import re
import sqlite3
from datetime import UTC, datetime
from typing import Any

from .git_utils import normalize_git_remote


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    return candidate or None


def normalize_repository_root(value: str | None) -> str | None:
    candidate = trimmed(value)
    if not candidate:
        return None
    candidate = re.sub(r"/+", "/", candidate.replace("\\", "/"))
    normalized = posixpath.normpath(candidate)
    if re.match(r"^[A-Za-z]:/", normalized):
        normalized = normalized.casefold()
    return normalized.rstrip("/") or "/"


def normalize_repository_remote(value: str | None) -> dict[str, str] | None:
    remote = normalize_git_remote(value)
    if remote is None or bool(remote["is_local"]):
        return None
    host = str(remote["host"]).strip().casefold()
    path = str(remote["path"]).strip().strip("/")
    if host == "github.com":
        path = path.casefold()
    if not host or not path:
        return None
    identity = f"{host}/{path}"
    return {
        "identity": identity,
        "canonical_key": f"remote:{identity}",
        "host": host,
        "path": path,
        "label": identity,
        "canonical_url": f"https://{identity}",
    }


def normalize_repository_remote_filter(value: str | None) -> str | None:
    candidate = trimmed(value)
    if not candidate:
        return None
    normalized = normalize_repository_remote(candidate)
    if normalized is not None:
        return normalized["identity"]
    shorthand = candidate.strip().strip("/")
    if "/" in shorthand and "." in shorthand.split("/", 1)[0]:
        normalized = normalize_repository_remote(f"https://{shorthand}")
        if normalized is not None:
            return normalized["identity"]
    raise ValueError("remote must be a Git URL or normalized host/path")


def repository_root_sql(column: str) -> str:
    """Return SQLite text normalization equivalent to normalize_repository_root."""
    slashes = f"REPLACE(TRIM({column}), '\\', '/')"
    collapsed = (
        f"REPLACE(REPLACE(REPLACE({slashes}, '//', '/'), '//', '/'), '//', '/')"
    )
    stripped = f"RTRIM({collapsed}, '/')"
    return (
        f"CASE WHEN {stripped} = '' AND {collapsed} LIKE '/%' THEN '/' "
        f"WHEN {collapsed} GLOB '[A-Za-z]:/*' THEN LOWER({stripped}) "
        f"ELSE {stripped} END"
    )


def repository_id_for_key(canonical_key: str) -> str:
    return hashlib.sha256(f"repository\0{canonical_key}".encode("utf-8")).hexdigest()[:32]


def _alias_key(alias_type: str, alias_value: str) -> str:
    raw = f"{alias_type}\0{alias_value}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def resolve_repository_id(
    connection: sqlite3.Connection,
    repository_id: str | None,
) -> str | None:
    current = trimmed(repository_id)
    seen: set[str] = set()
    while current and current not in seen:
        seen.add(current)
        row = connection.execute(
            "SELECT merged_into_repository_id FROM repositories WHERE id = ?",
            (current,),
        ).fetchone()
        if row is None:
            return None
        target = trimmed(row["merged_into_repository_id"])
        if not target:
            return current
        current = target
    return None


def _repository_for_alias(
    connection: sqlite3.Connection,
    alias_type: str,
    alias_value: str | None,
) -> str | None:
    if not alias_value:
        return None
    row = connection.execute(
        """
        SELECT repository_id
        FROM repository_aliases
        WHERE alias_type = ? AND alias_value = ?
        """,
        (alias_type, alias_value),
    ).fetchone()
    return resolve_repository_id(connection, row["repository_id"]) if row else None


def _ensure_repository(
    connection: sqlite3.Connection,
    *,
    canonical_key: str,
    kind: str,
    remote_host: str = "",
    remote_path: str = "",
    display_label: str,
) -> str:
    row = connection.execute(
        "SELECT id FROM repositories WHERE canonical_key = ?",
        (canonical_key,),
    ).fetchone()
    if row is not None:
        resolved = resolve_repository_id(connection, str(row["id"]))
        if resolved:
            return resolved

    repository_id = repository_id_for_key(canonical_key)
    now = utc_now_iso()
    connection.execute(
        """
        INSERT INTO repositories (
            id,
            canonical_key,
            kind,
            remote_host,
            remote_path,
            display_label,
            merged_into_repository_id,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            kind = excluded.kind,
            remote_host = excluded.remote_host,
            remote_path = excluded.remote_path,
            display_label = excluded.display_label,
            updated_at = excluded.updated_at
        """,
        (
            repository_id,
            canonical_key,
            kind,
            remote_host,
            remote_path,
            display_label,
            now,
            now,
        ),
    )
    return repository_id


def _merge_repository(
    connection: sqlite3.Connection,
    *,
    source_repository_id: str,
    target_repository_id: str,
) -> str:
    source = resolve_repository_id(connection, source_repository_id)
    target = resolve_repository_id(connection, target_repository_id)
    if not source or not target or source == target:
        return target or source or target_repository_id

    connection.execute(
        "UPDATE sessions SET repository_id = ? WHERE repository_id = ?",
        (target, source),
    )
    connection.execute(
        "UPDATE projects SET repository_id = ? WHERE repository_id = ?",
        (target, source),
    )
    connection.execute(
        "UPDATE repository_aliases SET repository_id = ?, updated_at = ? WHERE repository_id = ?",
        (target, utc_now_iso(), source),
    )
    connection.execute(
        """
        UPDATE repositories
        SET merged_into_repository_id = ?, updated_at = ?
        WHERE id = ?
        """,
        (target, utc_now_iso(), source),
    )
    return target


def _ensure_alias(
    connection: sqlite3.Connection,
    *,
    repository_id: str,
    alias_type: str,
    alias_value: str | None,
    display_value: str = "",
    source_host: str = "",
    root: str = "",
    remote_url: str = "",
) -> str:
    if not alias_value:
        return repository_id
    existing = _repository_for_alias(connection, alias_type, alias_value)
    if existing:
        return existing
    now = utc_now_iso()
    connection.execute(
        """
        INSERT INTO repository_aliases (
            alias_key,
            repository_id,
            alias_type,
            alias_value,
            display_value,
            source_host,
            root,
            remote_url,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _alias_key(alias_type, alias_value),
            repository_id,
            alias_type,
            alias_value,
            display_value or alias_value,
            source_host,
            root,
            remote_url,
            now,
            now,
        ),
    )
    return repository_id


def _session_identity(row: sqlite3.Row) -> dict[str, str | None]:
    source_host = (trimmed(row["source_host"]) or "unknown-host").casefold()
    root = normalize_repository_root(trimmed(row["cwd"]))
    remote_url = (
        trimmed(row["override_remote_url"])
        or trimmed(row["github_remote_url"])
        or trimmed(row["git_repository_url"])
    )
    remote = normalize_repository_remote(remote_url)
    location_value = f"{source_host}:{root}" if root else None
    return {
        "source_host": source_host,
        "root": root,
        "remote_url": remote_url,
        "remote_identity": remote["identity"] if remote else None,
        "remote_key": remote["canonical_key"] if remote else None,
        "remote_host": remote["host"] if remote else None,
        "remote_path": remote["path"] if remote else None,
        "remote_label": remote["label"] if remote else None,
        "remote_canonical_url": remote["canonical_url"] if remote else None,
        "location_value": location_value,
        "location_key": f"location:{location_value}" if location_value else None,
    }


def sync_repository_registry(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        """
        SELECT
            s.id,
            s.source_host,
            s.cwd,
            s.git_repository_url,
            s.github_remote_url,
            s.inferred_project_key,
            o.override_remote_url,
            ps.project_id
        FROM sessions AS s
        LEFT JOIN ignored_project_sources AS i
            ON i.match_project_key = s.inferred_project_key
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        LEFT JOIN project_sources AS ps
            ON ps.match_project_key = s.inferred_project_key
        WHERE i.match_project_key IS NULL
        ORDER BY s.id ASC
        """
    ).fetchall()
    identified = [(row, _session_identity(row)) for row in rows]
    identified.sort(
        key=lambda item: (
            not bool(item[1]["remote_identity"]),
            str(item[0]["id"]),
        )
    )

    for row, identity in identified:
        remote_identity = trimmed(identity["remote_identity"])
        location_value = trimmed(identity["location_value"])
        remote_repository_id = _repository_for_alias(
            connection,
            "remote",
            remote_identity,
        )
        location_repository_id = _repository_for_alias(
            connection,
            "location",
            location_value,
        )

        if remote_identity:
            repository_id = remote_repository_id
            if repository_id is None:
                repository_id = _ensure_repository(
                    connection,
                    canonical_key=str(identity["remote_key"]),
                    kind="remote",
                    remote_host=str(identity["remote_host"] or ""),
                    remote_path=str(identity["remote_path"] or ""),
                    display_label=str(identity["remote_label"] or remote_identity),
                )
            if location_repository_id and location_repository_id != repository_id:
                location_row = connection.execute(
                    "SELECT kind FROM repositories WHERE id = ?",
                    (location_repository_id,),
                ).fetchone()
                if location_row is not None and str(location_row["kind"]) == "location":
                    repository_id = _merge_repository(
                        connection,
                        source_repository_id=location_repository_id,
                        target_repository_id=repository_id,
                    )
            repository_id = _ensure_alias(
                connection,
                repository_id=repository_id,
                alias_type="remote",
                alias_value=remote_identity,
                display_value=str(identity["remote_label"] or remote_identity),
                remote_url=str(identity["remote_canonical_url"] or ""),
            )
        elif location_value:
            repository_id = location_repository_id or _ensure_repository(
                connection,
                canonical_key=str(identity["location_key"]),
                kind="location",
                display_label=location_value,
            )
        else:
            connection.execute(
                "UPDATE sessions SET repository_id = NULL WHERE id = ?",
                (str(row["id"]),),
            )
            continue

        if location_value:
            existing_location = _repository_for_alias(
                connection,
                "location",
                location_value,
            )
            if existing_location is None:
                _ensure_alias(
                    connection,
                    repository_id=repository_id,
                    alias_type="location",
                    alias_value=location_value,
                    display_value=location_value,
                    source_host=str(identity["source_host"] or ""),
                    root=str(identity["root"] or ""),
                )
        source_key = trimmed(row["inferred_project_key"])
        if (
            source_key
            and _repository_for_alias(connection, "project_source", source_key) is None
        ):
            _ensure_alias(
                connection,
                repository_id=repository_id,
                alias_type="project_source",
                alias_value=source_key,
                display_value=source_key,
            )
        connection.execute(
            "UPDATE sessions SET repository_id = ? WHERE id = ?",
            (repository_id, str(row["id"])),
        )

    project_rows = connection.execute("SELECT id FROM projects").fetchall()
    for project_row in project_rows:
        project_id = str(project_row["id"])
        repository_rows = connection.execute(
            """
            SELECT DISTINCT s.repository_id
            FROM project_sources AS ps
            JOIN sessions AS s
                ON s.inferred_project_key = ps.match_project_key
            WHERE ps.project_id = ?
              AND s.repository_id IS NOT NULL
            """,
            (project_id,),
        ).fetchall()
        repository_ids = {
            resolve_repository_id(connection, row["repository_id"])
            for row in repository_rows
        }
        repository_ids.discard(None)
        project_repository_id = next(iter(repository_ids)) if len(repository_ids) == 1 else None
        connection.execute(
            """
            UPDATE projects
            SET repository_id = ?, updated_at = ?
            WHERE id = ? AND repository_id IS NOT ?
            """,
            (
                project_repository_id,
                utc_now_iso(),
                project_id,
                project_repository_id,
            ),
        )


def repository_registry_needs_sync(connection: sqlite3.Connection) -> bool:
    rows = connection.execute(
        """
        SELECT
            s.repository_id,
            s.source_host,
            s.cwd,
            s.git_repository_url,
            s.github_remote_url,
            o.override_remote_url
        FROM sessions AS s
        LEFT JOIN ignored_project_sources AS i
            ON i.match_project_key = s.inferred_project_key
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        WHERE i.match_project_key IS NULL
          AND COALESCE(TRIM(s.cwd), TRIM(s.git_repository_url), TRIM(s.github_remote_url), '') <> ''
        """
    ).fetchall()
    for row in rows:
        identity = _session_identity(row)
        alias_value = trimmed(identity["remote_identity"]) or trimmed(
            identity["location_value"]
        )
        if not alias_value:
            if resolve_repository_id(connection, row["repository_id"]):
                return True
            continue
        expected_repository_id = _repository_for_alias(
            connection,
            "remote" if identity["remote_identity"] else "location",
            alias_value,
        )
        current_repository_id = resolve_repository_id(connection, row["repository_id"])
        if not current_repository_id or current_repository_id != expected_repository_id:
            return True
    return connection.execute(
        """
        SELECT 1
        FROM projects AS p
        WHERE p.repository_id IS NOT (
              SELECT CASE
                  WHEN COUNT(DISTINCT s.repository_id) = 1
                  THEN MIN(s.repository_id)
              END
              FROM project_sources AS ps
              JOIN sessions AS s ON s.inferred_project_key = ps.match_project_key
              WHERE ps.project_id = p.id AND s.repository_id IS NOT NULL
          )
        LIMIT 1
        """
    ).fetchone() is not None


def _access_condition(project_access: Any) -> tuple[str | None, list[str]]:
    if project_access is None or bool(getattr(project_access, "bypass", False)):
        return None, []
    roles = getattr(project_access, "project_roles", {}) or {}
    allowed = sorted(str(project_id) for project_id in roles if trimmed(project_id))
    if not allowed:
        return "p.visibility != 'private'", []
    placeholders = ", ".join("?" for _ in allowed)
    return f"(p.visibility != 'private' OR p.id IN ({placeholders}))", allowed


def repository_snapshot(
    connection: sqlite3.Connection,
    repository_id: str | None,
) -> dict[str, Any] | None:
    resolved = resolve_repository_id(connection, repository_id)
    if not resolved:
        return None
    row = connection.execute(
        """
        SELECT id, canonical_key, kind, remote_host, remote_path, display_label
        FROM repositories
        WHERE id = ?
        """,
        (resolved,),
    ).fetchone()
    if row is None:
        return None
    aliases = connection.execute(
        """
        SELECT alias_type, alias_value, display_value, source_host, root, remote_url
        FROM repository_aliases
        WHERE repository_id = ?
        ORDER BY alias_type ASC, alias_value ASC
        """,
        (resolved,),
    ).fetchall()
    return {
        "id": resolved,
        "canonical_key": str(row["canonical_key"]),
        "kind": str(row["kind"]),
        "host": trimmed(row["remote_host"]),
        "path": trimmed(row["remote_path"]),
        "label": str(row["display_label"] or row["canonical_key"]),
        "remote": (
            f"https://{row['remote_host']}/{row['remote_path']}"
            if trimmed(row["remote_host"]) and trimmed(row["remote_path"])
            else None
        ),
        "aliases": [
            {
                "type": str(alias["alias_type"]),
                "value": str(alias["display_value"] or alias["alias_value"]),
                "host": trimmed(alias["source_host"]),
                "root": trimmed(alias["root"]),
                "remote": trimmed(alias["remote_url"]),
            }
            for alias in aliases
        ],
    }


def list_repository_projects(
    connection: sqlite3.Connection,
    *,
    repository_id: str | None = None,
    remote: str | None = None,
    root: str | None = None,
    host: str | None = None,
    page: int = 1,
    page_size: int = 50,
    project_access: Any = None,
) -> dict[str, Any]:
    conditions: list[str] = []
    params: list[Any] = []
    resolved_repository_id = (
        resolve_repository_id(connection, repository_id) if repository_id else None
    )
    if repository_id and not resolved_repository_id:
        return {
            "items": [],
            "total_count": 0,
            "page": 1,
            "page_size": page_size,
            "has_next": False,
        }
    if resolved_repository_id:
        conditions.append(
            """
            EXISTS (
                SELECT 1
                FROM project_sources AS fps
                JOIN sessions AS fs ON fs.inferred_project_key = fps.match_project_key
                WHERE fps.project_id = p.id AND fs.repository_id = ?
            )
            """
        )
        params.append(resolved_repository_id)
    if remote:
        conditions.append(
            """
            EXISTS (
                SELECT 1
                FROM project_sources AS fps
                JOIN sessions AS fs ON fs.inferred_project_key = fps.match_project_key
                JOIN repository_aliases AS fra ON fra.repository_id = fs.repository_id
                WHERE fps.project_id = p.id
                  AND fra.alias_type = 'remote'
                  AND fra.alias_value = ?
            )
            """
        )
        params.append(remote)
    if root:
        normalized_session_root = repository_root_sql("fs.cwd")
        conditions.append(
            f"""
            EXISTS (
                SELECT 1
                FROM project_sources AS fps
                JOIN sessions AS fs ON fs.inferred_project_key = fps.match_project_key
                WHERE fps.project_id = p.id
                  AND {normalized_session_root} = ?
            )
            """
        )
        params.append(root)
    if host:
        conditions.append(
            """
            EXISTS (
                SELECT 1
                FROM project_sources AS fps
                JOIN sessions AS fs ON fs.inferred_project_key = fps.match_project_key
                WHERE fps.project_id = p.id AND fs.source_host = ?
            )
            """
        )
        params.append(host)
    access_condition, access_params = _access_condition(project_access)
    if access_condition:
        conditions.append(access_condition)
        params.extend(access_params)
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    normalized_page = max(1, int(page or 1))
    normalized_page_size = max(1, min(int(page_size or 50), 100))
    offset = (normalized_page - 1) * normalized_page_size
    rows = connection.execute(
        f"""
        WITH project_summary AS (
            SELECT
                p.id,
                p.current_group_key,
                p.display_label,
                p.visibility,
                p.repository_id,
                COUNT(DISTINCT s.id) AS session_count,
                MIN(COALESCE(s.session_timestamp, s.started_at, s.imported_at)) AS first_session_at,
                MAX(COALESCE(s.session_timestamp, s.started_at, s.imported_at)) AS last_session_at
            FROM projects AS p
            LEFT JOIN project_sources AS ps ON ps.project_id = p.id
            LEFT JOIN sessions AS s ON s.inferred_project_key = ps.match_project_key
            {where_clause}
            GROUP BY p.id
        )
        SELECT *, COUNT(*) OVER () AS total_count
        FROM project_summary
        ORDER BY display_label ASC, current_group_key ASC, id ASC
        LIMIT ? OFFSET ?
        """,
        [*params, normalized_page_size, offset],
    ).fetchall()
    total_count = int(rows[0]["total_count"] or 0) if rows else 0
    items: list[dict[str, Any]] = []
    for row in rows:
        project_id = str(row["id"])
        sources = connection.execute(
            """
            SELECT
                ps.match_project_key,
                s.source_host,
                s.cwd,
                o.override_remote_url,
                s.git_repository_url,
                s.github_remote_url,
                s.repository_id,
                COUNT(DISTINCT s.id) AS session_count,
                MIN(COALESCE(s.session_timestamp, s.started_at, s.imported_at)) AS first_session_at,
                MAX(COALESCE(s.session_timestamp, s.started_at, s.imported_at)) AS last_session_at
            FROM project_sources AS ps
            LEFT JOIN sessions AS s ON s.inferred_project_key = ps.match_project_key
            LEFT JOIN project_overrides AS o
                ON o.match_project_key = ps.match_project_key
            WHERE ps.project_id = ?
            GROUP BY ps.match_project_key, s.source_host, s.cwd, o.override_remote_url,
                     s.git_repository_url, s.github_remote_url, s.repository_id
            ORDER BY ps.match_project_key ASC, s.source_host ASC, s.cwd ASC
            """,
            (project_id,),
        ).fetchall()
        source_payload = [
            {
                "project_key": str(source["match_project_key"]),
                "repository_id": resolve_repository_id(
                    connection,
                    source["repository_id"],
                ),
                "host": trimmed(source["source_host"]),
                "root": normalize_repository_root(trimmed(source["cwd"])),
                "remote": trimmed(source["override_remote_url"])
                or trimmed(source["git_repository_url"])
                or trimmed(source["github_remote_url"]),
                "session_count": int(source["session_count"] or 0),
                "first_session_at": trimmed(source["first_session_at"]),
                "last_session_at": trimmed(source["last_session_at"]),
            }
            for source in sources
        ]
        repository_ids = sorted(
            {
                str(source["repository_id"])
                for source in source_payload
                if source["repository_id"]
            }
        )
        canonical_id = resolve_repository_id(connection, row["repository_id"])
        items.append(
            {
                "id": project_id,
                "key": str(row["current_group_key"]),
                "label": str(row["display_label"] or row["current_group_key"]),
                "visibility": str(row["visibility"] or "authenticated"),
                "repository_id": canonical_id,
                "repository_ids": repository_ids,
                "repository": repository_snapshot(connection, canonical_id),
                "sources": source_payload,
                "session_count": int(row["session_count"] or 0),
                "first_session_at": trimmed(row["first_session_at"]),
                "last_session_at": trimmed(row["last_session_at"]),
            }
        )
    return {
        "items": items,
        "total_count": total_count,
        "page": normalized_page,
        "page_size": normalized_page_size,
        "has_next": bool(rows and offset + len(rows) < total_count),
    }
