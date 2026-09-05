from __future__ import annotations

import re
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .projects import (
    TURN_SEARCH_HIGHLIGHT_END,
    TURN_SEARCH_HIGHLIGHT_START,
    TURN_STREAM_SELECT,
    ProjectAccessContext,
    build_session_signal_badges,
    effective_project_fields,
    project_access_condition_sql,
    trimmed,
    visible_session_where,
)
from .repositories import (
    normalize_repository_remote_filter,
    normalize_repository_root,
    repository_root_sql,
    resolve_repository_id,
)
from .search_query import SEARCH_MODES, SearchQueryPlan, plan_search_query
from .turn_index import SEARCH_CHUNK_VERSION, TURN_INDEX_VERSION, TURN_SEARCH_VERSION


TURN_TIMESTAMP_SQL = """
    COALESCE(
        st.latest_timestamp,
        st.response_timestamp,
        st.prompt_timestamp,
        s.last_turn_timestamp,
        s.session_timestamp,
        s.started_at,
        s.imported_at
    )
"""

SESSION_TIMESTAMP_SQL = """
    COALESCE(
        NULLIF(s.session_timestamp, ''),
        NULLIF(s.started_at, ''),
        NULLIF(s.imported_at, '')
    )
"""

SEARCH_SORT_MODES = frozenset({"relevance", "time_asc", "time_desc"})
SEARCH_GROUP_MODES = frozenset({"none", "session"})
SEARCH_FIELDS = frozenset(
    {"prompt", "response", "activity", "commands", "paths", "commit_ids", "tool_output"}
)
SEARCH_FACETS = frozenset({"project", "session", "date", "branch", "matched_field"})
SEARCH_FIELD_COLUMNS = {
    "prompt": "prompt_text",
    "response": "response_text",
    "activity": "event_text",
    "commands": "command_text",
    "paths": "path_text",
    "commit_ids": "commit_id_text",
    "tool_output": "tool_output_text",
}
SEARCH_CHUNK_FIELDS = frozenset(
    {"prompt", "response", "activity", "commands", "tool_output"}
)


@dataclass(frozen=True)
class SearchMatchSpec:
    turn_expression: str | None
    chunk_expression: str | None
    chunk_fields: tuple[str, ...]

    @property
    def params(self) -> list[Any]:
        params: list[Any] = []
        if self.turn_expression:
            params.append(self.turn_expression)
        if self.chunk_expression:
            params.append(self.chunk_expression)
            params.extend(self.chunk_fields)
        return params


def normalize_search_values(
    values: str | Sequence[str] | None,
    *,
    supported: frozenset[str],
    label: str,
) -> tuple[str, ...]:
    if values is None:
        return ()
    raw_values = values.split(",") if isinstance(values, str) else values
    normalized: list[str] = []
    for raw_value in raw_values:
        value = str(raw_value or "").strip().lower()
        if not value:
            continue
        if value not in supported:
            raise ValueError(f"Unsupported search {label}: {value}")
        if value not in normalized:
            normalized.append(value)
    return tuple(normalized)


def _search_match_spec(
    expression: str | None,
    fields: tuple[str, ...],
) -> SearchMatchSpec | None:
    if not expression:
        return None
    selected_fields = fields or tuple(SEARCH_FIELD_COLUMNS)
    turn_columns = [SEARCH_FIELD_COLUMNS[field] for field in selected_fields]
    if not fields:
        turn_columns.insert(0, "project_text")
    turn_expression = f"{{{' '.join(turn_columns)}}} : ({expression})"
    chunk_fields = tuple(field for field in selected_fields if field in SEARCH_CHUNK_FIELDS)
    return SearchMatchSpec(
        turn_expression=turn_expression,
        chunk_expression=expression if chunk_fields else None,
        chunk_fields=chunk_fields,
    )


def _empty_search_coverage(*, state: str = "empty") -> dict[str, Any]:
    return {
        "first_session_at": None,
        "last_session_at": None,
        "last_indexed_at": None,
        "sessions_total": 0,
        "sessions_indexed": 0,
        "turns_total": 0,
        "turns_indexed": 0,
        "pending_reindex_sessions": 0,
        "projects_searched": [],
        "index_versions": {
            "turn": TURN_INDEX_VERSION,
            "turn_search": TURN_SEARCH_VERSION,
            "search_chunk": SEARCH_CHUNK_VERSION,
        },
        "freshness": {
            "state": state,
            "indexed_at_known_sessions": 0,
            "indexed_at_unknown_sessions": 0,
        },
    }


def _normalize_search_options(
    sort: str,
    group_by: str,
    max_hits_per_session: int,
) -> tuple[str, str, int]:
    normalized_sort = str(sort or "relevance").strip().lower()
    normalized_group_by = str(group_by or "none").strip().lower()
    if normalized_sort not in SEARCH_SORT_MODES:
        raise ValueError(f"Unsupported search sort: {sort}")
    if normalized_group_by not in SEARCH_GROUP_MODES:
        raise ValueError(f"Unsupported search grouping: {group_by}")
    normalized_max_hits = max(1, min(int(max_hits_per_session or 3), 100))
    return normalized_sort, normalized_group_by, normalized_max_hits


def _matched_candidate_ctes(
    match: SearchMatchSpec,
    *,
    include_evidence: bool = True,
) -> str:
    if include_evidence:
        highlight_args = (
            f"'{TURN_SEARCH_HIGHLIGHT_START}', "
            f"'{TURN_SEARCH_HIGHLIGHT_END}', ' … '"
        )
        turn_project = f"snippet(session_turn_search, 0, {highlight_args}, 10)"
        turn_prompt = f"snippet(session_turn_search, 1, {highlight_args}, 18)"
        turn_response = f"snippet(session_turn_search, 2, {highlight_args}, 18)"
        turn_event = f"snippet(session_turn_search, 3, {highlight_args}, 18)"
        turn_command = f"snippet(session_turn_search, 4, {highlight_args}, 18)"
        turn_path = f"snippet(session_turn_search, 5, {highlight_args}, 18)"
        turn_commit = f"snippet(session_turn_search, 6, {highlight_args}, 18)"
        turn_tool_output = f"snippet(session_turn_search, 7, {highlight_args}, 18)"
        chunk_project = f"snippet(session_search_chunk_fts, 1, {highlight_args}, 10)"
        chunk_snippet = f"snippet(session_search_chunk_fts, 0, {highlight_args}, 24)"
    else:
        turn_project = "NULL"
        turn_prompt = "NULL"
        turn_response = "NULL"
        turn_event = "NULL"
        turn_command = "NULL"
        turn_path = "NULL"
        turn_commit = "NULL"
        turn_tool_output = "NULL"
        chunk_project = "NULL"
        chunk_snippet = "NULL"
    candidate_queries: list[str] = []
    if match.turn_expression:
        candidate_queries.append(
            f"""
            SELECT
                session_id,
                turn_number,
                'turn' AS match_source,
                bm25(session_turn_search, 1.0, 5.0, 4.0, 2.0, 4.0, 5.0, 5.0, 3.0) AS search_rank,
                {turn_project} AS project_snippet,
                {turn_prompt} AS prompt_snippet,
                {turn_response} AS response_snippet,
                {turn_event} AS event_snippet,
                {turn_command} AS command_snippet,
                {turn_path} AS path_snippet,
                {turn_commit} AS commit_snippet,
                {turn_tool_output} AS tool_output_snippet,
                NULL AS chunk_snippet,
                NULL AS chunk_id,
                NULL AS chunk_field,
                NULL AS chunk_index,
                NULL AS chunk_start_offset,
                NULL AS chunk_end_offset
            FROM session_turn_search
            WHERE session_turn_search MATCH ?
            """
        )
    if match.chunk_expression:
        placeholders = ", ".join("?" for _field in match.chunk_fields)
        candidate_queries.append(
            f"""
            SELECT
                session_search_chunk_fts.session_id,
                session_search_chunk_fts.turn_number,
                'chunk' AS match_source,
                bm25(session_search_chunk_fts, 5.0, 1.0) AS search_rank,
                {chunk_project} AS project_snippet,
                NULL AS prompt_snippet,
                NULL AS response_snippet,
                NULL AS event_snippet,
                NULL AS command_snippet,
                NULL AS path_snippet,
                NULL AS commit_snippet,
                NULL AS tool_output_snippet,
                {chunk_snippet} AS chunk_snippet,
                chunks.chunk_id,
                chunks.field AS chunk_field,
                chunks.chunk_index,
                chunks.start_offset AS chunk_start_offset,
                chunks.end_offset AS chunk_end_offset
            FROM session_search_chunk_fts
            JOIN session_search_chunks AS chunks
                ON chunks.chunk_id = session_search_chunk_fts.chunk_id
            WHERE session_search_chunk_fts MATCH ?
              AND chunks.field IN ({placeholders})
            """
        )
    raw_query = "\nUNION ALL\n".join(candidate_queries)
    return f"""
        raw_candidates AS (
            {raw_query}
        ),
        ranked_candidates AS (
            SELECT
                raw_candidates.*,
                ROW_NUMBER() OVER (
                    PARTITION BY session_id, turn_number
                    ORDER BY
                        search_rank ASC,
                        CASE WHEN match_source = 'chunk' THEN 0 ELSE 1 END ASC,
                        COALESCE(chunk_index, 0) ASC
                ) AS candidate_rank
            FROM raw_candidates
        )
    """


def _empty_search_page(
    page_size: int,
    *,
    retrieval: dict[str, Any] | None = None,
    group_by: str = "none",
    coverage: dict[str, Any] | None = None,
    facets: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    return {
        "items": [],
        "groups": [],
        "page": 1,
        "page_size": page_size,
        "total_count": 0,
        "session_count": 0,
        "pagination_total": 0,
        "pagination_unit": "session" if group_by == "session" else "hit",
        "has_prev": False,
        "has_next": False,
        "page_count": 1,
        "showing_from": 0,
        "showing_to": 0,
        "retrieval": retrieval or {},
        "coverage": coverage or _empty_search_coverage(),
        "facets": facets or {},
    }


def _snippet_has_hit(snippet: object) -> bool:
    return (
        isinstance(snippet, str)
        and TURN_SEARCH_HIGHLIGHT_START in snippet
        and TURN_SEARCH_HIGHLIGHT_END in snippet
    )


def _matched_snippet(
    row: sqlite3.Row,
    *,
    has_match_expression: bool,
) -> tuple[str, str]:
    if not has_match_expression:
        fallback = trimmed(row["response_excerpt"]) or trimmed(row["prompt_excerpt"])
        return "history", fallback or "No turn excerpt available."
    if str(row["match_source"] or "") == "chunk" and _snippet_has_hit(row["chunk_snippet"]):
        return trimmed(row["chunk_field"]) or "activity", str(row["chunk_snippet"] or "").strip()
    snippets = [
        ("prompt", row["prompt_snippet"]),
        ("response", row["response_snippet"]),
        ("commands", row["command_snippet"]),
        ("paths", row["path_snippet"]),
        ("commit_ids", row["commit_snippet"]),
        ("tool_output", row["tool_output_snippet"]),
        ("activity", row["event_snippet"]),
        ("project", row["project_snippet"]),
    ]
    for field, snippet in snippets:
        if _snippet_has_hit(snippet):
            return field, str(snippet or "").strip()
    for field, snippet in snippets:
        if trimmed(snippet):
            return field, str(snippet or "").strip()
    fallback = trimmed(row["prompt_excerpt"]) or trimmed(row["response_excerpt"]) or "No matching snippet."
    return "prompt", fallback


def plain_search_snippet(value: object) -> str:
    return (
        str(value or "")
        .replace(TURN_SEARCH_HIGHLIGHT_START, "")
        .replace(TURN_SEARCH_HIGHLIGHT_END, "")
        .strip()
    )


def _response_status(response_state: str) -> tuple[str, str]:
    if response_state == "canceled":
        return "amber", "Canceled"
    if response_state == "update":
        return "sky", "Update"
    if response_state == "missing":
        return "stone", "Missing"
    return "emerald", "Final"


def _normalized_project_alias(value: object) -> str:
    return str(value or "").strip().casefold().strip("/")


def _project_match_score(row: sqlite3.Row, hint: str) -> int:
    reference = _normalized_project_alias(hint)
    if not reference:
        return 0
    score = 0
    for raw_alias in (row["id"], row["current_group_key"], row["display_label"]):
        alias = _normalized_project_alias(raw_alias)
        if not alias:
            continue
        if alias == reference:
            score = max(score, 300)
        basename = re.split(r"[:/]", alias)[-1].strip()
        if basename == reference:
            score = max(score, 250)
    return score


def _resolve_project_scope(
    connection: sqlite3.Connection,
    *,
    plan: SearchQueryPlan,
    explicit_project_id: str | None,
    project_access: ProjectAccessContext | None,
) -> tuple[str | None, dict[str, Any]]:
    normalized_explicit = trimmed(explicit_project_id)
    if normalized_explicit:
        return normalized_explicit, {
            "hint": plan.project_hint,
            "resolution": "explicit",
            "id": normalized_explicit,
            "key": None,
            "label": None,
        }
    if not plan.project_hint:
        return None, {
            "hint": None,
            "resolution": "not_requested",
            "id": None,
            "key": None,
            "label": None,
        }

    conditions: list[str] = []
    params: list[Any] = []
    access_condition, access_params = project_access_condition_sql(project_access)
    if access_condition:
        conditions.append(access_condition)
        params.extend(access_params)
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = connection.execute(
        f"""
        SELECT p.id, p.current_group_key, p.display_label
        FROM projects AS p
        {where_clause}
        ORDER BY p.current_group_key ASC, p.id ASC
        """,
        params,
    ).fetchall()
    scored = [
        (_project_match_score(row, plan.project_hint), row)
        for row in rows
    ]
    best_score = max((score for score, _row in scored), default=0)
    best_rows = [row for score, row in scored if score == best_score and score > 0]
    if len(best_rows) != 1:
        return None, {
            "hint": plan.project_hint,
            "resolution": "ambiguous" if best_rows else "unmatched",
            "id": None,
            "key": None,
            "label": None,
        }

    matched = best_rows[0]
    return str(matched["id"]), {
        "hint": plan.project_hint,
        "resolution": "matched",
        "id": str(matched["id"]),
        "key": str(matched["current_group_key"]),
        "label": str(matched["display_label"] or matched["current_group_key"]),
    }


def _base_search_conditions(
    *,
    project_id: str | None,
    repository_id: str | None,
    remote: str | None,
    root: str | None,
    host: str | None,
    from_timestamp: str | None,
    to_timestamp: str | None,
    project_access: ProjectAccessContext | None,
) -> tuple[list[str], list[Any]]:
    conditions: list[str] = []
    params: list[Any] = []
    if project_id:
        conditions.append("p.id = ?")
        params.append(project_id)
    if repository_id:
        conditions.append("COALESCE(s.repository_id, p.repository_id) = ?")
        params.append(repository_id)
    if remote:
        conditions.append(
            """
            EXISTS (
                SELECT 1
                FROM repository_aliases AS search_remote_alias
                WHERE search_remote_alias.repository_id = COALESCE(s.repository_id, p.repository_id)
                  AND search_remote_alias.alias_type = 'remote'
                  AND search_remote_alias.alias_value = ?
            )
            """
        )
        params.append(remote)
    if root:
        conditions.append(f"{repository_root_sql('s.cwd')} = ?")
        params.append(root)
    if host:
        conditions.append("s.source_host = ?")
        params.append(host)
    if from_timestamp:
        conditions.append(f"julianday({TURN_TIMESTAMP_SQL}) >= julianday(?)")
        params.append(from_timestamp)
    if to_timestamp:
        conditions.append(f"julianday({TURN_TIMESTAMP_SQL}) <= julianday(?)")
        params.append(to_timestamp)

    access_condition, access_params = project_access_condition_sql(project_access)
    if access_condition:
        conditions.append(access_condition)
        params.extend(access_params)
    return conditions, params


def _search_coverage(
    connection: sqlite3.Connection,
    *,
    base_conditions: list[str],
    base_params: list[Any],
) -> dict[str, Any]:
    fully_indexed_sql = f"""
        COALESCE(s.turn_index_version, 0) >= {TURN_INDEX_VERSION}
        AND COALESCE(s.turn_search_version, 0) >= {TURN_SEARCH_VERSION}
        AND COALESCE(s.search_chunk_version, 0) >= {SEARCH_CHUNK_VERSION}
    """
    project_key_sql = """
        COALESCE(
            NULLIF(TRIM(o.override_group_key), ''),
            NULLIF(TRIM(p.current_group_key), ''),
            NULLIF(TRIM(s.inferred_project_key), ''),
            ''
        )
    """
    project_label_sql = f"""
        COALESCE(
            NULLIF(TRIM(o.override_display_label), ''),
            NULLIF(TRIM(p.display_label), ''),
            NULLIF(TRIM(s.inferred_project_label), ''),
            {project_key_sql}
        )
    """
    rows = connection.execute(
        f"""
        SELECT
            p.id AS project_id,
            CASE
                WHEN COUNT(DISTINCT COALESCE(s.repository_id, p.repository_id)) = 1
                THEN MIN(COALESCE(s.repository_id, p.repository_id))
            END AS repository_id,
            {project_key_sql} AS project_key,
            {project_label_sql} AS project_label,
            strftime(
                '%Y-%m-%dT%H:%M:%SZ',
                MIN(julianday({SESSION_TIMESTAMP_SQL}))
            ) AS first_session_at,
            strftime(
                '%Y-%m-%dT%H:%M:%SZ',
                MAX(julianday({SESSION_TIMESTAMP_SQL}))
            ) AS last_session_at,
            strftime(
                '%Y-%m-%dT%H:%M:%SZ',
                MAX(
                    CASE WHEN {fully_indexed_sql}
                    THEN julianday(NULLIF(TRIM(s.search_indexed_at), ''))
                    END
                )
            ) AS last_indexed_at,
            COUNT(DISTINCT s.id) AS sessions_total,
            COUNT(DISTINCT CASE WHEN {fully_indexed_sql} THEN s.id END) AS sessions_indexed,
            COUNT(*) AS turns_total,
            COUNT(CASE WHEN {fully_indexed_sql} THEN 1 END) AS turns_indexed,
            COUNT(
                DISTINCT CASE
                    WHEN {fully_indexed_sql}
                     AND NULLIF(TRIM(s.search_indexed_at), '') IS NOT NULL
                    THEN s.id
                END
            ) AS indexed_at_known_sessions
        FROM session_turns AS st
        JOIN sessions AS s
            ON s.id = st.session_id
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        LEFT JOIN project_sources AS ps
            ON ps.match_project_key = s.inferred_project_key
        LEFT JOIN projects AS p
            ON p.id = ps.project_id
        {visible_session_where(base_conditions)}
        GROUP BY p.id, {project_key_sql}, {project_label_sql}
        ORDER BY project_label ASC, project_key ASC, project_id ASC
        """,
        base_params,
    ).fetchall()
    if not rows:
        return _empty_search_coverage()

    sessions_total = sum(int(row["sessions_total"] or 0) for row in rows)
    sessions_indexed = sum(int(row["sessions_indexed"] or 0) for row in rows)
    turns_total = sum(int(row["turns_total"] or 0) for row in rows)
    turns_indexed = sum(int(row["turns_indexed"] or 0) for row in rows)
    indexed_at_known_sessions = sum(
        int(row["indexed_at_known_sessions"] or 0) for row in rows
    )
    indexed_at_unknown_sessions = max(
        sessions_indexed - indexed_at_known_sessions,
        0,
    )
    pending_reindex_sessions = max(sessions_total - sessions_indexed, 0)
    first_timestamps = [
        str(row["first_session_at"])
        for row in rows
        if row["first_session_at"]
    ]
    last_timestamps = [
        str(row["last_session_at"])
        for row in rows
        if row["last_session_at"]
    ]
    indexed_timestamps = [
        str(row["last_indexed_at"])
        for row in rows
        if row["last_indexed_at"]
    ]
    if pending_reindex_sessions:
        freshness_state = "pending_reindex"
    elif indexed_at_unknown_sessions:
        freshness_state = "timestamp_unknown"
    else:
        freshness_state = "current"

    projects = []
    for row in rows:
        project_sessions_total = int(row["sessions_total"] or 0)
        project_sessions_indexed = int(row["sessions_indexed"] or 0)
        projects.append(
            {
                "id": str(row["project_id"]) if row["project_id"] else None,
                "repository_id": (
                    str(row["repository_id"]) if row["repository_id"] else None
                ),
                "key": trimmed(row["project_key"]),
                "label": trimmed(row["project_label"]),
                "session_count": project_sessions_total,
                "turn_count": int(row["turns_total"] or 0),
                "sessions_indexed": project_sessions_indexed,
                "pending_reindex_sessions": max(
                    project_sessions_total - project_sessions_indexed,
                    0,
                ),
            }
        )

    return {
        "first_session_at": min(first_timestamps) if first_timestamps else None,
        "last_session_at": max(last_timestamps) if last_timestamps else None,
        "last_indexed_at": max(indexed_timestamps) if indexed_timestamps else None,
        "sessions_total": sessions_total,
        "sessions_indexed": sessions_indexed,
        "turns_total": turns_total,
        "turns_indexed": turns_indexed,
        "pending_reindex_sessions": pending_reindex_sessions,
        "projects_searched": projects,
        "index_versions": {
            "turn": TURN_INDEX_VERSION,
            "turn_search": TURN_SEARCH_VERSION,
            "search_chunk": SEARCH_CHUNK_VERSION,
        },
        "freshness": {
            "state": freshness_state,
            "indexed_at_known_sessions": indexed_at_known_sessions,
            "indexed_at_unknown_sessions": indexed_at_unknown_sessions,
        },
    }


def _turn_search_from_clause(conditions: list[str]) -> str:
    return f"""
        FROM session_turn_search
        JOIN session_turns AS st
            ON st.session_id = session_turn_search.session_id
           AND st.turn_number = session_turn_search.turn_number
        JOIN sessions AS s
            ON s.id = st.session_id
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        LEFT JOIN project_sources AS ps
            ON ps.match_project_key = s.inferred_project_key
        LEFT JOIN projects AS p
            ON p.id = ps.project_id
        {visible_session_where(conditions)}
    """


def _matched_search_from_clause(
    match: SearchMatchSpec,
    conditions: list[str],
) -> str:
    return f"""
        FROM ranked_candidates AS search_candidates
        JOIN session_turns AS st
            ON st.session_id = search_candidates.session_id
           AND st.turn_number = search_candidates.turn_number
        JOIN sessions AS s
            ON s.id = st.session_id
        LEFT JOIN project_overrides AS o
            ON o.match_project_key = s.inferred_project_key
        LEFT JOIN project_sources AS ps
            ON ps.match_project_key = s.inferred_project_key
        LEFT JOIN projects AS p
            ON p.id = ps.project_id
        {visible_session_where(["search_candidates.candidate_rank = 1", *conditions])}
    """


def _search_stage_counts(
    connection: sqlite3.Connection,
    *,
    base_conditions: list[str],
    base_params: list[Any],
    match: SearchMatchSpec | None,
) -> tuple[int, int]:
    with_clause = ""
    if match:
        with_clause = f"WITH {_matched_candidate_ctes(match, include_evidence=False)}"
        from_clause = _matched_search_from_clause(match, base_conditions)
        params = [*match.params, *base_params]
    else:
        from_clause = _turn_search_from_clause(base_conditions)
        params = list(base_params)
    row = connection.execute(
        f"""
        {with_clause}
        SELECT
            COUNT(*) AS match_count,
            COUNT(DISTINCT st.session_id) AS session_count
        {from_clause}
        """,
        params,
    ).fetchone()
    if row is None:
        return 0, 0
    return int(row["match_count"] or 0), int(row["session_count"] or 0)


def _retrieval_metadata(
    *,
    plan: SearchQueryPlan,
    strategy: str,
    project: dict[str, Any],
    stage_counts: dict[str, int | None],
) -> dict[str, Any]:
    return {
        "strategy": strategy,
        "mode": plan.mode,
        "intent": plan.intent,
        "time_focus": plan.time_focus,
        "status_focus": plan.status_focus,
        "natural_language": plan.natural_language,
        "terms": list(plan.relaxed_terms),
        "project": project,
        "stage_counts": stage_counts,
        "index": {
            "mode": "hybrid_lexical",
            "chunk_version": SEARCH_CHUNK_VERSION,
        },
    }


def _search_facets(
    connection: sqlite3.Connection,
    *,
    base_conditions: list[str],
    base_params: list[Any],
    match: SearchMatchSpec | None,
    requested: tuple[str, ...],
) -> dict[str, list[dict[str, Any]]]:
    if not requested:
        return {}
    if match:
        with_prefix = (
            f"WITH {_matched_candidate_ctes(match, include_evidence='matched_field' in requested)},"
        )
        source = "ranked_candidates AS search_candidates"
        source_join = """
            JOIN session_turns AS st
                ON st.session_id = search_candidates.session_id
               AND st.turn_number = search_candidates.turn_number
        """
        conditions = ["search_candidates.candidate_rank = 1", *base_conditions]
        matched_field_sql = f"""
            CASE
                WHEN search_candidates.match_source = 'chunk'
                    THEN COALESCE(search_candidates.chunk_field, 'activity')
                WHEN INSTR(COALESCE(search_candidates.prompt_snippet, ''), '{TURN_SEARCH_HIGHLIGHT_START}') > 0 THEN 'prompt'
                WHEN INSTR(COALESCE(search_candidates.response_snippet, ''), '{TURN_SEARCH_HIGHLIGHT_START}') > 0 THEN 'response'
                WHEN INSTR(COALESCE(search_candidates.command_snippet, ''), '{TURN_SEARCH_HIGHLIGHT_START}') > 0 THEN 'commands'
                WHEN INSTR(COALESCE(search_candidates.path_snippet, ''), '{TURN_SEARCH_HIGHLIGHT_START}') > 0 THEN 'paths'
                WHEN INSTR(COALESCE(search_candidates.commit_snippet, ''), '{TURN_SEARCH_HIGHLIGHT_START}') > 0 THEN 'commit_ids'
                WHEN INSTR(COALESCE(search_candidates.tool_output_snippet, ''), '{TURN_SEARCH_HIGHLIGHT_START}') > 0 THEN 'tool_output'
                WHEN INSTR(COALESCE(search_candidates.event_snippet, ''), '{TURN_SEARCH_HIGHLIGHT_START}') > 0 THEN 'activity'
                ELSE 'project'
            END
        """
        params = [*match.params, *base_params]
    else:
        with_prefix = "WITH"
        source = "session_turn_search"
        source_join = """
            JOIN session_turns AS st
                ON st.session_id = session_turn_search.session_id
               AND st.turn_number = session_turn_search.turn_number
        """
        conditions = base_conditions
        matched_field_sql = "'history'"
        params = list(base_params)

    project_key_sql = """
        COALESCE(
            NULLIF(TRIM(o.override_group_key), ''),
            NULLIF(TRIM(p.current_group_key), ''),
            NULLIF(TRIM(s.inferred_project_key), ''),
            ''
        )
    """
    project_label_sql = f"""
        COALESCE(
            NULLIF(TRIM(o.override_display_label), ''),
            NULLIF(TRIM(p.display_label), ''),
            NULLIF(TRIM(s.inferred_project_label), ''),
            {project_key_sql}
        )
    """
    aggregate_ctes: list[str] = []
    selects: list[str] = []
    if "project" in requested:
        aggregate_ctes.append(
            """
            project_facet AS (
                SELECT project_id, project_key, project_label, COUNT(*) AS match_count
                FROM matched_rows
                GROUP BY project_id, project_key, project_label
                ORDER BY match_count DESC, project_label ASC
                LIMIT 100
            )
            """
        )
        selects.append(
            "SELECT 'project' AS facet, project_key AS value, project_label AS label, project_id, match_count FROM project_facet"
        )
    for facet, column in (
        ("session", "session_id"),
        ("date", "match_date"),
        ("branch", "branch"),
        ("matched_field", "matched_field"),
    ):
        if facet not in requested:
            continue
        cte_name = f"{facet}_facet"
        aggregate_ctes.append(
            f"""
            {cte_name} AS (
                SELECT {column} AS value, COUNT(*) AS match_count
                FROM matched_rows
                WHERE NULLIF(TRIM({column}), '') IS NOT NULL
                GROUP BY {column}
                ORDER BY match_count DESC, value ASC
                LIMIT 100
            )
            """
        )
        selects.append(
            f"SELECT '{facet}' AS facet, value, value AS label, NULL AS project_id, match_count FROM {cte_name}"
        )

    rows = connection.execute(
        f"""
        {with_prefix}
        matched_rows AS (
            SELECT
                st.session_id,
                strftime('%Y-%m-%d', julianday({TURN_TIMESTAMP_SQL})) AS match_date,
                COALESCE(NULLIF(TRIM(s.git_branch), ''), '') AS branch,
                p.id AS project_id,
                {project_key_sql} AS project_key,
                {project_label_sql} AS project_label,
                {matched_field_sql} AS matched_field
            FROM {source}
            {source_join}
            JOIN sessions AS s ON s.id = st.session_id
            LEFT JOIN project_overrides AS o
                ON o.match_project_key = s.inferred_project_key
            LEFT JOIN project_sources AS ps
                ON ps.match_project_key = s.inferred_project_key
            LEFT JOIN projects AS p
                ON p.id = ps.project_id
            {visible_session_where(conditions)}
        ),
        {','.join(aggregate_ctes)}
        {' UNION ALL '.join(selects)}
        """,
        params,
    ).fetchall()
    result = {facet: [] for facet in requested}
    for row in rows:
        item: dict[str, Any] = {
            "value": str(row["value"] or ""),
            "count": int(row["match_count"] or 0),
        }
        if row["label"]:
            item["label"] = str(row["label"])
        if row["project_id"]:
            item["project_id"] = str(row["project_id"])
        result[str(row["facet"])].append(item)
    return result


def _turn_order_expression(
    *,
    sort: str,
    prefer_recent: bool,
    rank_expression: str,
) -> str:
    timestamp_expression = f"julianday({TURN_TIMESTAMP_SQL})"
    if sort == "time_asc":
        return f"{timestamp_expression} ASC, st.session_id ASC, st.turn_number ASC"
    if sort == "time_desc":
        return f"{timestamp_expression} DESC, st.session_id DESC, st.turn_number DESC"
    if prefer_recent:
        return (
            f"{timestamp_expression} DESC, {rank_expression} ASC, "
            "st.session_id DESC, st.turn_number DESC"
        )
    return (
        f"{rank_expression} ASC, {timestamp_expression} DESC, "
        "st.session_id DESC, st.turn_number DESC"
    )


def _session_order_expression(*, sort: str, prefer_recent: bool) -> str:
    if sort == "time_asc":
        return "earliest_timestamp ASC, group_session_id ASC"
    if sort == "time_desc":
        return "latest_timestamp DESC, group_session_id DESC"
    if prefer_recent:
        return "latest_timestamp DESC, best_rank ASC, group_session_id DESC"
    return "best_rank ASC, latest_timestamp DESC, group_session_id DESC"


def _select_grouped_sessions(
    connection: sqlite3.Connection,
    *,
    base_conditions: list[str],
    base_params: list[Any],
    match: SearchMatchSpec | None,
    page_size: int,
    offset: int,
    sort: str,
    prefer_recent: bool,
) -> list[sqlite3.Row]:
    session_order = _session_order_expression(sort=sort, prefer_recent=prefer_recent)
    if match:
        ranked_conditions = ["search_candidates.candidate_rank = 1", *base_conditions]
        return connection.execute(
            f"""
            WITH {_matched_candidate_ctes(match, include_evidence=False)}
            SELECT
                st.session_id AS group_session_id,
                COUNT(*) AS match_count,
                MIN(search_candidates.search_rank) AS best_rank,
                MIN(julianday({TURN_TIMESTAMP_SQL})) AS earliest_timestamp,
                MAX(julianday({TURN_TIMESTAMP_SQL})) AS latest_timestamp
            FROM ranked_candidates AS search_candidates
            JOIN session_turns AS st
                ON st.session_id = search_candidates.session_id
               AND st.turn_number = search_candidates.turn_number
            JOIN sessions AS s
                ON s.id = st.session_id
            LEFT JOIN project_overrides AS o
                ON o.match_project_key = s.inferred_project_key
            LEFT JOIN project_sources AS ps
                ON ps.match_project_key = s.inferred_project_key
            LEFT JOIN projects AS p
                ON p.id = ps.project_id
            {visible_session_where(ranked_conditions)}
            GROUP BY st.session_id
            ORDER BY {session_order}
            LIMIT ? OFFSET ?
            """,
            [*match.params, *base_params, page_size, offset],
        ).fetchall()

    return connection.execute(
        f"""
        SELECT
            st.session_id AS group_session_id,
            COUNT(*) AS match_count,
            0.0 AS best_rank,
            MIN(julianday({TURN_TIMESTAMP_SQL})) AS earliest_timestamp,
            MAX(julianday({TURN_TIMESTAMP_SQL})) AS latest_timestamp
        {_turn_search_from_clause(base_conditions)}
        GROUP BY st.session_id
        ORDER BY {session_order}
        LIMIT ? OFFSET ?
        """,
        [*base_params, page_size, offset],
    ).fetchall()


def _selected_sessions_cte(rows: list[sqlite3.Row]) -> tuple[str, list[Any]]:
    values = ", ".join("(?, ?, ?)" for _row in rows)
    params: list[Any] = []
    for group_order, row in enumerate(rows):
        params.extend(
            [
                str(row["group_session_id"]),
                group_order,
                int(row["match_count"] or 0),
            ]
        )
    return (
        f"selected_sessions(session_id, group_order, match_count) AS (VALUES {values})",
        params,
    )


def _run_search_stage(
    connection: sqlite3.Connection,
    *,
    base_conditions: list[str],
    base_params: list[Any],
    match: SearchMatchSpec | None,
    total_count: int,
    session_count: int,
    page: int,
    page_size: int,
    prefer_recent: bool,
    sort: str,
    group_by: str,
    max_hits_per_session: int,
    retrieval: dict[str, Any],
    coverage: dict[str, Any],
    facets: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    normalized_page = max(int(page or 1), 1)
    pagination_total = session_count if group_by == "session" else total_count
    page_count = max((pagination_total + page_size - 1) // page_size, 1)
    normalized_page = min(normalized_page, page_count)
    offset = (normalized_page - 1) * page_size

    selected_session_rows: list[sqlite3.Row] = []
    if group_by == "session":
        selected_session_rows = _select_grouped_sessions(
            connection,
            base_conditions=base_conditions,
            base_params=base_params,
            match=match,
            page_size=page_size,
            offset=offset,
            sort=sort,
            prefer_recent=prefer_recent,
        )

    if group_by == "session" and not selected_session_rows:
        rows = []
    elif match and group_by == "none":
        matched_order = _turn_order_expression(
            sort=sort,
            prefer_recent=prefer_recent,
            rank_expression="search_candidates.search_rank",
        )
        ranked_conditions = ["search_candidates.candidate_rank = 1", *base_conditions]
        rows = connection.execute(
            f"""
            WITH {_matched_candidate_ctes(match)}
            SELECT
                {TURN_STREAM_SELECT},
                search_candidates.search_rank,
                search_candidates.project_snippet,
                search_candidates.prompt_snippet,
                search_candidates.response_snippet,
                search_candidates.event_snippet,
                search_candidates.command_snippet,
                search_candidates.path_snippet,
                search_candidates.commit_snippet,
                search_candidates.tool_output_snippet,
                search_candidates.chunk_snippet,
                search_candidates.match_source,
                search_candidates.chunk_id,
                search_candidates.chunk_field,
                search_candidates.chunk_index,
                search_candidates.chunk_start_offset,
                search_candidates.chunk_end_offset
            FROM ranked_candidates AS search_candidates
            JOIN session_turns AS st
                ON st.session_id = search_candidates.session_id
               AND st.turn_number = search_candidates.turn_number
            JOIN sessions AS s
                ON s.id = st.session_id
            LEFT JOIN project_overrides AS o
                ON o.match_project_key = s.inferred_project_key
            LEFT JOIN project_sources AS ps
                ON ps.match_project_key = s.inferred_project_key
            LEFT JOIN projects AS p
                ON p.id = ps.project_id
            {visible_session_where(ranked_conditions)}
            ORDER BY {matched_order}
            LIMIT ? OFFSET ?
            """,
            [*match.params, *base_params, page_size, offset],
        ).fetchall()
    elif match:
        selected_cte, selected_params = _selected_sessions_cte(selected_session_rows)
        grouped_order = _turn_order_expression(
            sort=sort,
            prefer_recent=prefer_recent,
            rank_expression="search_candidates.search_rank",
        )
        ranked_conditions = ["search_candidates.candidate_rank = 1", *base_conditions]
        rows = connection.execute(
            f"""
            WITH {_matched_candidate_ctes(match)},
            {selected_cte},
            ranked_hits AS (
                SELECT
                    {TURN_STREAM_SELECT},
                    search_candidates.search_rank,
                    search_candidates.project_snippet,
                    search_candidates.prompt_snippet,
                    search_candidates.response_snippet,
                    search_candidates.event_snippet,
                    search_candidates.command_snippet,
                    search_candidates.path_snippet,
                    search_candidates.commit_snippet,
                    search_candidates.tool_output_snippet,
                    search_candidates.chunk_snippet,
                    search_candidates.match_source,
                    search_candidates.chunk_id,
                    search_candidates.chunk_field,
                    search_candidates.chunk_index,
                    search_candidates.chunk_start_offset,
                    search_candidates.chunk_end_offset,
                    selected_sessions.match_count AS group_match_count,
                    selected_sessions.group_order,
                    ROW_NUMBER() OVER (
                        PARTITION BY st.session_id
                        ORDER BY {grouped_order}
                    ) AS session_hit_rank
                FROM ranked_candidates AS search_candidates
                JOIN session_turns AS st
                    ON st.session_id = search_candidates.session_id
                   AND st.turn_number = search_candidates.turn_number
                JOIN selected_sessions
                    ON selected_sessions.session_id = st.session_id
                JOIN sessions AS s
                    ON s.id = st.session_id
                LEFT JOIN project_overrides AS o
                    ON o.match_project_key = s.inferred_project_key
                LEFT JOIN project_sources AS ps
                    ON ps.match_project_key = s.inferred_project_key
                LEFT JOIN projects AS p
                    ON p.id = ps.project_id
                {visible_session_where(ranked_conditions)}
            )
            SELECT *
            FROM ranked_hits
            WHERE session_hit_rank <= ?
            ORDER BY group_order ASC, session_hit_rank ASC
            """,
            [
                *match.params,
                *selected_params,
                *base_params,
                max_hits_per_session,
            ],
        ).fetchall()
    elif group_by == "none":
        search_columns = """
            0.0 AS search_rank,
            NULL AS project_snippet,
            NULL AS prompt_snippet,
            NULL AS response_snippet,
            NULL AS event_snippet,
            NULL AS command_snippet,
            NULL AS path_snippet,
            NULL AS commit_snippet,
            NULL AS tool_output_snippet,
            NULL AS chunk_snippet,
            'history' AS match_source,
            NULL AS chunk_id,
            NULL AS chunk_field,
            NULL AS chunk_index,
            NULL AS chunk_start_offset,
            NULL AS chunk_end_offset
        """
        history_order = _turn_order_expression(
            sort=sort,
            prefer_recent=True,
            rank_expression="0.0",
        )
        rows = connection.execute(
            f"""
            SELECT
                {TURN_STREAM_SELECT},
                {search_columns}
            {_turn_search_from_clause(base_conditions)}
            ORDER BY {history_order}
            LIMIT ? OFFSET ?
            """,
            [*base_params, page_size, offset],
        ).fetchall()
    else:
        selected_cte, selected_params = _selected_sessions_cte(selected_session_rows)
        grouped_order = _turn_order_expression(
            sort=sort,
            prefer_recent=True,
            rank_expression="0.0",
        )
        rows = connection.execute(
            f"""
            WITH {selected_cte},
            ranked_hits AS (
                SELECT
                    {TURN_STREAM_SELECT},
                    0.0 AS search_rank,
                    NULL AS project_snippet,
                    NULL AS prompt_snippet,
                    NULL AS response_snippet,
                    NULL AS event_snippet,
                    NULL AS command_snippet,
                    NULL AS path_snippet,
                    NULL AS commit_snippet,
                    NULL AS tool_output_snippet,
                    NULL AS chunk_snippet,
                    'history' AS match_source,
                    NULL AS chunk_id,
                    NULL AS chunk_field,
                    NULL AS chunk_index,
                    NULL AS chunk_start_offset,
                    NULL AS chunk_end_offset,
                    selected_sessions.match_count AS group_match_count,
                    selected_sessions.group_order,
                    ROW_NUMBER() OVER (
                        PARTITION BY st.session_id
                        ORDER BY {grouped_order}
                    ) AS session_hit_rank
                FROM session_turn_search
                JOIN session_turns AS st
                    ON st.session_id = session_turn_search.session_id
                   AND st.turn_number = session_turn_search.turn_number
                JOIN selected_sessions
                    ON selected_sessions.session_id = st.session_id
                JOIN sessions AS s
                    ON s.id = st.session_id
                LEFT JOIN project_overrides AS o
                    ON o.match_project_key = s.inferred_project_key
                LEFT JOIN project_sources AS ps
                    ON ps.match_project_key = s.inferred_project_key
                LEFT JOIN projects AS p
                    ON p.id = ps.project_id
                {visible_session_where(base_conditions)}
            )
            SELECT *
            FROM ranked_hits
            WHERE session_hit_rank <= ?
            ORDER BY group_order ASC, session_hit_rank ASC
            """,
            [*selected_params, *base_params, max_hits_per_session],
        ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        project = effective_project_fields(row)
        response_state = trimmed(row["response_state"]) or "missing"
        status_tone, status_label = _response_status(response_state)
        timestamp = (
            trimmed(row["latest_timestamp"])
            or trimmed(row["response_timestamp"])
            or trimmed(row["prompt_timestamp"])
            or trimmed(row["session_timestamp"])
            or trimmed(row["started_at"])
            or trimmed(row["imported_at"])
        )
        matched_field, marked_snippet = _matched_snippet(
            row,
            has_match_expression=bool(match),
        )
        failure_count = int(row["failure_count"] or 0)
        search_rank = float(row["search_rank"] or 0.0)
        match_source = str(row["match_source"] or "turn")
        chunk_evidence = None
        if match_source == "chunk" and trimmed(row["chunk_id"]):
            chunk_evidence = {
                "id": str(row["chunk_id"]),
                "field": trimmed(row["chunk_field"]) or matched_field,
                "index": int(row["chunk_index"] or 0),
                "start_offset": int(row["chunk_start_offset"] or 0),
                "end_offset": int(row["chunk_end_offset"] or 0),
            }
        items.append(
            {
                "session_id": str(row["session_id"]),
                "turn_number": int(row["turn_number"] or 0),
                "timestamp": timestamp,
                "prompt_excerpt": trimmed(row["prompt_excerpt"]) or "No prompt excerpt",
                "response_excerpt": trimmed(row["response_excerpt"]) or "No assistant response captured.",
                "response_state": response_state,
                "status_tone": status_tone,
                "status_label": status_label,
                "command_count": int(row["command_count"] or 0),
                "patch_count": int(row["patch_count"] or 0),
                "failure_count": failure_count,
                "files_touched_count": int(row["files_touched_count"] or 0),
                "signal_badges": build_session_signal_badges(
                    row,
                    command_exits=failure_count,
                    aborted_turns=int(row["aborted_turn_count"] or 0),
                    viewer_warning=trimmed(row["import_warning"]),
                ),
                "project_id": project["project_id"],
                "project_key": project["effective_group_key"],
                "project_label": project["display_label"],
                "host": project["source_host"],
                "repository": {
                    "id": trimmed(row["repository_id"]),
                    "remote": trimmed(row["git_repository_url"])
                    or trimmed(row["github_remote_url"]),
                    "root": project["cwd"],
                    "branch": trimmed(row["git_branch"]),
                    "head": trimmed(row["git_commit_hash"]),
                    # Dirty state is not captured by the current ingestion schema.
                    "dirty": None,
                },
                "matched_field": matched_field,
                "marked_snippet": marked_snippet,
                "snippet": plain_search_snippet(marked_snippet),
                "score": -search_rank,
                "match_source": match_source,
                "chunk": chunk_evidence,
                "group_match_count": (
                    int(row["group_match_count"] or 0)
                    if "group_match_count" in row.keys()
                    else None
                ),
            }
        )

    groups: list[dict[str, Any]] = []
    if group_by == "session":
        for item in items:
            if not groups or groups[-1]["session_id"] != item["session_id"]:
                groups.append(
                    {
                        "session_id": item["session_id"],
                        "match_count": int(item["group_match_count"] or 0),
                        "items": [],
                    }
                )
            groups[-1]["items"].append(item)

    returned_units = len(groups) if group_by == "session" else len(items)
    return {
        "items": items,
        "groups": groups,
        "page": normalized_page,
        "page_size": page_size,
        "total_count": total_count,
        "session_count": session_count,
        "pagination_total": pagination_total,
        "pagination_unit": "session" if group_by == "session" else "hit",
        "has_prev": normalized_page > 1,
        "has_next": bool(returned_units and offset + returned_units < pagination_total),
        "page_count": page_count,
        "showing_from": offset + 1 if returned_units else 0,
        "showing_to": offset + returned_units,
        "retrieval": retrieval,
        "coverage": coverage,
        "facets": facets,
    }


def search_turn_hits_raw(
    connection: sqlite3.Connection,
    q: str,
    *,
    page: int = 1,
    page_size: int = 20,
    project_id: str | None = None,
    repository_id: str | None = None,
    remote: str | None = None,
    root: str | None = None,
    host: str | None = None,
    from_timestamp: str | None = None,
    to_timestamp: str | None = None,
    sort: str = "relevance",
    group_by: str = "none",
    max_hits_per_session: int = 3,
    mode: str = "all",
    fields: str | Sequence[str] | None = None,
    facets: str | Sequence[str] | None = None,
    project_access: ProjectAccessContext | None = None,
) -> dict[str, Any]:
    """Return search-domain data without HTML or route-specific links."""

    normalized_page_size = max(1, min(int(page_size or 20), 100))
    normalized_sort, normalized_group_by, normalized_max_hits = _normalize_search_options(
        sort,
        group_by,
        max_hits_per_session,
    )
    normalized_mode = str(mode or "all").strip().lower()
    if normalized_mode not in SEARCH_MODES:
        raise ValueError(f"Unsupported search mode: {mode}")
    normalized_fields = normalize_search_values(
        fields,
        supported=SEARCH_FIELDS,
        label="field",
    )
    normalized_facets = normalize_search_values(
        facets,
        supported=SEARCH_FACETS,
        label="facet",
    )
    plan = plan_search_query(q, mode=normalized_mode)
    effective_project_id, project_resolution = _resolve_project_scope(
        connection,
        plan=plan,
        explicit_project_id=project_id,
        project_access=project_access,
    )
    if plan.project_hint and not effective_project_id:
        retrieval = _retrieval_metadata(
            plan=plan,
            strategy="no_match",
            project=project_resolution,
            stage_counts={
                "strict": None,
                "relaxed": None,
                "project_history": None,
            },
        )
        return _empty_search_page(
            normalized_page_size,
            retrieval=retrieval,
            group_by=normalized_group_by,
            coverage=_empty_search_coverage(state="unresolved_scope"),
        )
    normalized_repository_id = trimmed(repository_id)
    if normalized_repository_id:
        normalized_repository_id = (
            resolve_repository_id(connection, normalized_repository_id)
            or normalized_repository_id
        )
    normalized_remote = normalize_repository_remote_filter(remote)
    normalized_root = normalize_repository_root(root)
    base_conditions, base_params = _base_search_conditions(
        project_id=effective_project_id,
        repository_id=normalized_repository_id,
        remote=normalized_remote,
        root=normalized_root,
        host=trimmed(host),
        from_timestamp=trimmed(from_timestamp),
        to_timestamp=trimmed(to_timestamp),
        project_access=project_access,
    )
    coverage = _search_coverage(
        connection,
        base_conditions=base_conditions,
        base_params=base_params,
    )
    stage_counts: dict[str, int | None] = {
        "strict": None,
        "relaxed": None,
        "project_history": None,
    }
    if not plan.strict_expression:
        retrieval = _retrieval_metadata(
            plan=plan,
            strategy="no_match",
            project=project_resolution,
            stage_counts=stage_counts,
        )
        return _empty_search_page(
            normalized_page_size,
            retrieval=retrieval,
            group_by=normalized_group_by,
            coverage=coverage,
        )

    strict_match = _search_match_spec(plan.strict_expression, normalized_fields)
    strict_count, strict_session_count = _search_stage_counts(
        connection,
        base_conditions=base_conditions,
        base_params=base_params,
        match=strict_match,
    )
    stage_counts["strict"] = strict_count
    selected_strategy = "strict" if strict_count else "no_match"
    selected_match = strict_match if strict_count else None
    selected_count = strict_count
    selected_session_count = strict_session_count

    should_try_relaxed = bool(
        not normalized_fields
        and normalized_mode == "all"
        and plan.relaxed_expression
        and plan.relaxed_expression != plan.strict_expression
        and (
            strict_count == 0
            or (plan.is_abstract and not plan.content_terms)
        )
    )
    if should_try_relaxed:
        relaxed_match = _search_match_spec(plan.relaxed_expression, normalized_fields)
        relaxed_count, relaxed_session_count = _search_stage_counts(
            connection,
            base_conditions=base_conditions,
            base_params=base_params,
            match=relaxed_match,
        )
        stage_counts["relaxed"] = relaxed_count
        if relaxed_count and (not strict_count or relaxed_count > strict_count):
            selected_strategy = "relaxed"
            selected_match = relaxed_match
            selected_count = relaxed_count
            selected_session_count = relaxed_session_count

    if (
        selected_count == 0
        and not normalized_fields
        and plan.allows_history_fallback
        and effective_project_id
    ):
        history_count, history_session_count = _search_stage_counts(
            connection,
            base_conditions=base_conditions,
            base_params=base_params,
            match=None,
        )
        stage_counts["project_history"] = history_count
        if history_count:
            selected_strategy = "project_history"
            selected_match = None
            selected_count = history_count
            selected_session_count = history_session_count

    retrieval = _retrieval_metadata(
        plan=plan,
        strategy=selected_strategy,
        project=project_resolution,
        stage_counts=stage_counts,
    )
    if selected_count == 0:
        return _empty_search_page(
            normalized_page_size,
            retrieval=retrieval,
            group_by=normalized_group_by,
            coverage=coverage,
        )
    facet_values = _search_facets(
        connection,
        base_conditions=base_conditions,
        base_params=base_params,
        match=selected_match,
        requested=normalized_facets,
    )
    return _run_search_stage(
        connection,
        base_conditions=base_conditions,
        base_params=base_params,
        match=selected_match,
        total_count=selected_count,
        session_count=selected_session_count,
        page=page,
        page_size=normalized_page_size,
        prefer_recent=plan.prefer_recent,
        sort=normalized_sort,
        group_by=normalized_group_by,
        max_hits_per_session=normalized_max_hits,
        retrieval=retrieval,
        coverage=coverage,
        facets=facet_values,
    )
