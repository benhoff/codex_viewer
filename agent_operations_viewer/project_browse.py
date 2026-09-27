"""Compact, transactionally maintained data for project browsing.

Full session text stays in sessions. Triggers keep bounded previews and metadata
current even for deletes, reimports and maintenance writes outside the importer.
The catalog cache is versioned by committed browsing changes, never by a TTL;
access roles form part of its key and visibility changes invalidate it as well.
"""
from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
import sqlite3
from threading import RLock
from typing import Any, TYPE_CHECKING
from uuid import uuid4
from urllib.parse import quote

from .approval_reviews import approval_review_sql

if TYPE_CHECKING:
    from .projects import GroupedProject, ProjectAccessContext

# Keep these fields aligned with the session fields consumed by GROUP_ROW_SELECT
# and TURN_STREAM_SELECT. No raw transcript, search text or event payloads.
BROWSE_COLUMNS = tuple('''id session_timestamp started_at imported_at updated_at
summary import_warning event_count turn_count last_user_message last_turn_timestamp
latest_turn_summary command_failure_count aborted_turn_count latest_usage_timestamp
latest_input_tokens latest_cached_input_tokens latest_output_tokens latest_reasoning_output_tokens
latest_total_tokens latest_context_window latest_context_remaining_percent
latest_primary_limit_used_percent latest_primary_limit_resets_at latest_secondary_limit_used_percent
latest_secondary_limit_resets_at latest_rate_limit_name latest_rate_limit_reached_type
source_host cwd cwd_name git_branch git_commit_hash git_repository_url github_remote_url
github_org github_repo github_slug repository_id forked_from_id agent_nickname agent_role
agent_path memory_mode inferred_project_kind inferred_project_key inferred_project_label
environment_rollup_version'''.split())
PREVIEW_COLUMNS = {'summary', 'last_user_message', 'latest_turn_summary'}


def ensure_browse_schema(connection: sqlite3.Connection, *, rebuild: bool = False) -> None:
    exists = connection.execute("SELECT 1 FROM sqlite_master WHERE name='session_browse' AND type='table'").fetchone()
    types = {r['name']: r['type'] for r in connection.execute('PRAGMA table_info(sessions)')}
    definitions = ', '.join(f'{name} {types[name]}' + (' PRIMARY KEY' if name == 'id' else '') for name in BROWSE_COLUMNS)
    connection.execute(f'CREATE TABLE IF NOT EXISTS session_browse ({definitions})')
    browse_columns = {r['name'] for r in connection.execute('PRAGMA table_info(session_browse)')}
    if 'is_approval_review' not in browse_columns:
        connection.execute('ALTER TABLE session_browse ADD COLUMN is_approval_review INTEGER NOT NULL DEFAULT 0')
        rebuild = True
        for suffix in ('insert', 'update', 'delete'):
            connection.execute(f'DROP TRIGGER IF EXISTS browse_session_{suffix}')
    connection.execute('CREATE TABLE IF NOT EXISTS project_browse_revision (id INTEGER PRIMARY KEY CHECK(id=1), epoch TEXT NOT NULL, revision INTEGER NOT NULL)')
    connection.execute('INSERT OR IGNORE INTO project_browse_revision VALUES (1, ?, 0)', (uuid4().hex,))
    names = ', '.join((*BROWSE_COLUMNS, 'is_approval_review'))
    watched_names = ', '.join((*BROWSE_COLUMNS, 'raw_meta_json'))

    def values(prefix: str) -> str:
        fields = [f'substr({prefix}{name}, 1, 1024)' if name in PREVIEW_COLUMNS else f'{prefix}{name}' for name in BROWSE_COLUMNS]
        return ', '.join([*fields, approval_review_sql(f'{prefix}raw_meta_json')])
    bump = 'UPDATE project_browse_revision SET revision=revision+1 WHERE id=1;'
    if not exists or rebuild:
        connection.execute('DELETE FROM session_browse')
        connection.execute(f'INSERT INTO session_browse ({names}) SELECT {values("")} FROM sessions')
        connection.execute(bump)
    connection.execute('CREATE INDEX IF NOT EXISTS idx_browse_project ON session_browse(inferred_project_key)')
    connection.execute("""
        CREATE INDEX IF NOT EXISTS idx_browse_activity ON session_browse(
            COALESCE(NULLIF(last_turn_timestamp, ''), session_timestamp, started_at, imported_at) DESC,
            id DESC
        )
    """)
    # UPDATE OF avoids invalidation for raw-artifact bookkeeping. The WHEN test
    # also avoids writes for unchanged summaries in periodic metadata refreshes.
    changed = ' OR '.join(f'OLD.{name} IS NOT NEW.{name}' for name in BROWSE_COLUMNS)
    changed += f' OR ({approval_review_sql("OLD.raw_meta_json")}) IS NOT ({approval_review_sql("NEW.raw_meta_json")})'
    for event, suffix, guard, body in [
        ('INSERT', 'insert', '', f'INSERT OR REPLACE INTO session_browse ({names}) VALUES ({values("NEW.")});'),
        (f'UPDATE OF {watched_names}', 'update', f'WHEN {changed}', f'DELETE FROM session_browse WHERE id=OLD.id; INSERT OR REPLACE INTO session_browse ({names}) VALUES ({values("NEW.")});'),
        ('DELETE', 'delete', '', 'DELETE FROM session_browse WHERE id=OLD.id;'),
    ]:
        connection.execute(f'CREATE TRIGGER IF NOT EXISTS browse_session_{suffix} AFTER {event} ON sessions {guard} BEGIN {body} {bump} END')
    for table in ('projects', 'project_sources', 'project_overrides', 'ignored_project_sources'):
        for event in ('INSERT', 'UPDATE', 'DELETE'):
            connection.execute(f'CREATE TRIGGER IF NOT EXISTS browse_{table}_{event.lower()} AFTER {event} ON {table} BEGIN {bump} END')


@dataclass
class ProjectCatalog:
    rows: list[sqlite3.Row]
    groups: list[GroupedProject]
    rows_by_key: dict[str, list[sqlite3.Row]]
    key_by_session: dict[str, str]
    key_by_route: dict[str, str]
    stats: dict[str, int]


_CACHE: OrderedDict[tuple, ProjectCatalog] = OrderedDict()
_CACHE_LOCK = RLock()


def project_catalog(
    connection: sqlite3.Connection,
    project_access: ProjectAccessContext | None = None,
    *,
    show_approval_reviews: bool = True,
) -> ProjectCatalog:
    from .projects import (GROUP_ROW_SELECT, build_grouped_projects, dashboard_stats,
                           effective_project_fields, filter_rows_for_project_access,
                           joined_session_query, visible_session_where)
    revision = connection.execute('SELECT epoch, revision FROM project_browse_revision WHERE id=1').fetchone()
    access_key = None if project_access is None or project_access.bypass else (
        project_access.auth_enabled, project_access.user_id, tuple(sorted(project_access.project_roles.items())))
    database = next((row[2] for row in connection.execute('PRAGMA database_list') if row[1] == 'main'), '')
    key = (database, *revision, access_key, show_approval_reviews)
    # Never publish uncommitted data from a writer under a reusable revision key.
    cacheable = not connection.in_transaction
    with _CACHE_LOCK:
        found = _CACHE.get(key) if cacheable else None
        if found is not None:
            _CACHE.move_to_end(key)
            return found
    sql = joined_session_query(visible_session_where(),
        'ORDER BY COALESCE(s.session_timestamp, s.started_at, s.imported_at) DESC', GROUP_ROW_SELECT + ', s.is_approval_review')
    all_rows = filter_rows_for_project_access(connection.execute(sql.replace('FROM sessions AS s', 'FROM session_browse AS s')).fetchall(), project_access)
    rows = all_rows if show_approval_reviews else [row for row in all_rows if not row['is_approval_review']]
    # Route collisions must resolve identically with either filter setting.
    groups = build_grouped_projects(rows, route_rows=all_rows)
    by_key: dict[str, list[sqlite3.Row]] = {}
    key_by_session: dict[str, str] = {}
    for row in rows:
        group_key = effective_project_fields(row)['effective_group_key']
        by_key.setdefault(group_key, []).append(row)
        key_by_session[row['id']] = group_key
    result = ProjectCatalog(rows, groups, by_key, key_by_session,
                            {g.detail_href: g.key for g in groups}, dashboard_stats(rows))
    if cacheable:
        with _CACHE_LOCK:
            # Keep only the current revision for this database/access scope.
            for stale in list(_CACHE):
                if stale[:2] == key[:2] and stale[3:] == key[3:]:
                    del _CACHE[stale]
            _CACHE[key] = result
            _CACHE.move_to_end(key)
            while len(_CACHE) > 16:
                _CACHE.popitem(last=False)
    return result


def route_groups(
    connection: sqlite3.Connection,
    project_access: ProjectAccessContext | None = None,
) -> list[GroupedProject]:
    # Callers historically receive mutable GroupedProject instances.
    return deepcopy(project_catalog(connection, project_access).groups)


def browse_project_detail(
    connection: sqlite3.Connection,
    key: str,
    *,
    project_access: ProjectAccessContext | None = None,
    sessions_page: int = 1,
    sessions_page_size: int = 6,
    view: str = 'activity',
    detail_href: str = '',
    show_approval_reviews: bool = False,
) -> dict[str, Any] | None:
    from .projects import apply_project_session_preview, effective_project_fields
    catalog = project_catalog(connection, project_access, show_approval_reviews=show_approval_reviews)
    group = next((g for g in catalog.groups if g.key == key), None)
    if group is None:
        # A project containing only reviews still has a reachable toggle.
        all_catalog = project_catalog(connection, project_access)
        group = next((g for g in all_catalog.groups if g.key == key), None)
        if group is None:
            return None
        group = deepcopy(group)
        group.latest_timestamp = None
        group.session_count = group.turn_count = group.event_count = 0
    group = deepcopy(group)
    group.detail_href = detail_href or group.detail_href
    session_ids = [row['id'] for row in catalog.rows_by_key.get(key, [])]
    items = []
    page = max(1, int(sessions_page))
    size = max(1, int(sessions_page_size))
    if view == 'sessions' and session_ids:
        placeholders = ','.join('?' for _ in session_ids)
        # Only the visible page is converted into presentation objects.
        selected = connection.execute(f'''SELECT id FROM session_browse
            WHERE id IN ({placeholders})
            ORDER BY COALESCE(NULLIF(last_turn_timestamp, ''), session_timestamp, started_at, imported_at) DESC, id DESC
            LIMIT ? OFFSET ?''', [*session_ids, size, (page-1)*size]).fetchall()
        by_id = {row['id']: row for row in catalog.rows_by_key[key]}
        for selected_row in selected:
            row = by_id[selected_row['id']]
            item = {'id': row['id'], 'href': f"/sessions/{quote(str(row['id']), safe='')}",
                    'summary': row['latest_turn_summary'] or row['summary'],
                    'last_user_message': row['last_user_message'],
                    'last_turn_timestamp': row['last_turn_timestamp'],
                    'session_timestamp': row['session_timestamp'] or row['started_at'] or row['imported_at'],
                    'host': effective_project_fields(row)['source_host'], 'turn_count': row['turn_count']}
            apply_project_session_preview(item)
            items.append(item)
    return {
        'group': group,
        'session_ids': session_ids,
        'host_summaries': [{'source_host': host} for host in group.hosts],
        'status_strip': {'last_activity_at': group.latest_timestamp},
        'all_sessions_page': {
            'items': items,
            'page': page,
            'has_prev': page > 1,
            'has_next': page * size < len(session_ids),
        },
    }
