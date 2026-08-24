from __future__ import annotations

import re
import sqlite3
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
from .search_query import SearchQueryPlan, plan_search_query
from .turn_index import SEARCH_CHUNK_VERSION


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


def _empty_search_page(
    page_size: int,
    *,
    retrieval: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "items": [],
        "page": 1,
        "page_size": page_size,
        "total_count": 0,
        "has_prev": False,
        "has_next": False,
        "page_count": 1,
        "showing_from": 0,
        "showing_to": 0,
        "retrieval": retrieval or {},
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


def _matched_search_from_clause(conditions: list[str]) -> str:
    return f"""
        FROM (
            SELECT session_id, turn_number
            FROM session_turn_search
            WHERE session_turn_search MATCH ?
            UNION
            SELECT session_id, turn_number
            FROM session_search_chunk_fts
            WHERE session_search_chunk_fts MATCH ?
        ) AS search_candidates
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
        {visible_session_where(conditions)}
    """


def _search_stage_count(
    connection: sqlite3.Connection,
    *,
    base_conditions: list[str],
    base_params: list[Any],
    match_expression: str | None,
) -> int:
    if match_expression:
        from_clause = _matched_search_from_clause(base_conditions)
        params = [match_expression, match_expression, *base_params]
    else:
        from_clause = _turn_search_from_clause(base_conditions)
        params = list(base_params)
    row = connection.execute(
        f"SELECT COUNT(*) AS count {from_clause}",
        params,
    ).fetchone()
    return int(row["count"] or 0) if row is not None else 0


def _retrieval_metadata(
    *,
    plan: SearchQueryPlan,
    strategy: str,
    project: dict[str, Any],
    stage_counts: dict[str, int | None],
) -> dict[str, Any]:
    return {
        "strategy": strategy,
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


def _run_search_stage(
    connection: sqlite3.Connection,
    *,
    base_conditions: list[str],
    base_params: list[Any],
    match_expression: str | None,
    total_count: int,
    page: int,
    page_size: int,
    prefer_recent: bool,
    retrieval: dict[str, Any],
) -> dict[str, Any]:
    normalized_page = max(int(page or 1), 1)
    page_count = max((total_count + page_size - 1) // page_size, 1)
    normalized_page = min(normalized_page, page_count)
    offset = (normalized_page - 1) * page_size

    if match_expression:
        if prefer_recent:
            matched_order_clause = f"""
                ORDER BY {TURN_TIMESTAMP_SQL} DESC,
                    search_candidates.search_rank ASC,
                    st.session_id DESC,
                    st.turn_number DESC
            """
        else:
            matched_order_clause = f"""
                ORDER BY search_candidates.search_rank ASC,
                    {TURN_TIMESTAMP_SQL} DESC,
                    st.session_id DESC,
                    st.turn_number DESC
            """
        ranked_conditions = ["search_candidates.candidate_rank = 1", *base_conditions]
        rows = connection.execute(
            f"""
            WITH raw_candidates AS (
                SELECT
                    session_id,
                    turn_number,
                    'turn' AS match_source,
                    bm25(session_turn_search, 1.0, 5.0, 4.0, 2.0) AS search_rank,
                    snippet(session_turn_search, 0, '{TURN_SEARCH_HIGHLIGHT_START}', '{TURN_SEARCH_HIGHLIGHT_END}', ' … ', 10) AS project_snippet,
                    snippet(session_turn_search, 1, '{TURN_SEARCH_HIGHLIGHT_START}', '{TURN_SEARCH_HIGHLIGHT_END}', ' … ', 18) AS prompt_snippet,
                    snippet(session_turn_search, 2, '{TURN_SEARCH_HIGHLIGHT_START}', '{TURN_SEARCH_HIGHLIGHT_END}', ' … ', 18) AS response_snippet,
                    snippet(session_turn_search, 3, '{TURN_SEARCH_HIGHLIGHT_START}', '{TURN_SEARCH_HIGHLIGHT_END}', ' … ', 18) AS event_snippet,
                    NULL AS chunk_snippet,
                    NULL AS chunk_id,
                    NULL AS chunk_field,
                    NULL AS chunk_index,
                    NULL AS chunk_start_offset,
                    NULL AS chunk_end_offset
                FROM session_turn_search
                WHERE session_turn_search MATCH ?

                UNION ALL

                SELECT
                    session_search_chunk_fts.session_id,
                    session_search_chunk_fts.turn_number,
                    'chunk' AS match_source,
                    bm25(session_search_chunk_fts, 5.0, 1.0) AS search_rank,
                    snippet(session_search_chunk_fts, 1, '{TURN_SEARCH_HIGHLIGHT_START}', '{TURN_SEARCH_HIGHLIGHT_END}', ' … ', 10) AS project_snippet,
                    NULL AS prompt_snippet,
                    NULL AS response_snippet,
                    NULL AS event_snippet,
                    snippet(session_search_chunk_fts, 0, '{TURN_SEARCH_HIGHLIGHT_START}', '{TURN_SEARCH_HIGHLIGHT_END}', ' … ', 24) AS chunk_snippet,
                    chunks.chunk_id,
                    chunks.field AS chunk_field,
                    chunks.chunk_index,
                    chunks.start_offset AS chunk_start_offset,
                    chunks.end_offset AS chunk_end_offset
                FROM session_search_chunk_fts
                JOIN session_search_chunks AS chunks
                    ON chunks.chunk_id = session_search_chunk_fts.chunk_id
                WHERE session_search_chunk_fts MATCH ?
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
            SELECT
                {TURN_STREAM_SELECT},
                search_candidates.search_rank,
                search_candidates.project_snippet,
                search_candidates.prompt_snippet,
                search_candidates.response_snippet,
                search_candidates.event_snippet,
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
            {matched_order_clause}
            LIMIT ? OFFSET ?
            """,
            [match_expression, match_expression, *base_params, page_size, offset],
        ).fetchall()
    else:
        search_columns = """
            0.0 AS search_rank,
            NULL AS project_snippet,
            NULL AS prompt_snippet,
            NULL AS response_snippet,
            NULL AS event_snippet,
            NULL AS chunk_snippet,
            'history' AS match_source,
            NULL AS chunk_id,
            NULL AS chunk_field,
            NULL AS chunk_index,
            NULL AS chunk_start_offset,
            NULL AS chunk_end_offset
        """
        order_clause = f"""
            ORDER BY {TURN_TIMESTAMP_SQL} DESC,
                search_rank ASC,
                st.session_id DESC,
                st.turn_number DESC
        """
        rows = connection.execute(
            f"""
            SELECT
                {TURN_STREAM_SELECT},
                {search_columns}
            {_turn_search_from_clause(base_conditions)}
            {order_clause}
            LIMIT ? OFFSET ?
            """,
            [*base_params, page_size, offset],
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
            has_match_expression=bool(match_expression),
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
                "matched_field": matched_field,
                "marked_snippet": marked_snippet,
                "snippet": plain_search_snippet(marked_snippet),
                "score": -search_rank,
                "match_source": match_source,
                "chunk": chunk_evidence,
            }
        )

    return {
        "items": items,
        "page": normalized_page,
        "page_size": page_size,
        "total_count": total_count,
        "has_prev": normalized_page > 1,
        "has_next": offset + len(items) < total_count,
        "page_count": page_count,
        "showing_from": offset + 1 if items else 0,
        "showing_to": offset + len(items),
        "retrieval": retrieval,
    }


def search_turn_hits_raw(
    connection: sqlite3.Connection,
    q: str,
    *,
    page: int = 1,
    page_size: int = 20,
    project_id: str | None = None,
    host: str | None = None,
    from_timestamp: str | None = None,
    to_timestamp: str | None = None,
    project_access: ProjectAccessContext | None = None,
) -> dict[str, Any]:
    """Return search-domain data without HTML or route-specific links."""

    normalized_page_size = max(1, min(int(page_size or 20), 100))
    plan = plan_search_query(q)
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
        return _empty_search_page(normalized_page_size, retrieval=retrieval)
    base_conditions, base_params = _base_search_conditions(
        project_id=effective_project_id,
        host=trimmed(host),
        from_timestamp=trimmed(from_timestamp),
        to_timestamp=trimmed(to_timestamp),
        project_access=project_access,
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
        return _empty_search_page(normalized_page_size, retrieval=retrieval)

    strict_count = _search_stage_count(
        connection,
        base_conditions=base_conditions,
        base_params=base_params,
        match_expression=plan.strict_expression,
    )
    stage_counts["strict"] = strict_count
    selected_strategy = "strict" if strict_count else "no_match"
    selected_expression = plan.strict_expression if strict_count else None
    selected_count = strict_count

    should_try_relaxed = bool(
        plan.relaxed_expression
        and plan.relaxed_expression != plan.strict_expression
        and (
            strict_count == 0
            or (plan.is_abstract and not plan.content_terms)
        )
    )
    if should_try_relaxed:
        relaxed_count = _search_stage_count(
            connection,
            base_conditions=base_conditions,
            base_params=base_params,
            match_expression=plan.relaxed_expression,
        )
        stage_counts["relaxed"] = relaxed_count
        if relaxed_count and (not strict_count or relaxed_count > strict_count):
            selected_strategy = "relaxed"
            selected_expression = plan.relaxed_expression
            selected_count = relaxed_count

    if (
        selected_count == 0
        and plan.allows_history_fallback
        and effective_project_id
    ):
        history_count = _search_stage_count(
            connection,
            base_conditions=base_conditions,
            base_params=base_params,
            match_expression=None,
        )
        stage_counts["project_history"] = history_count
        if history_count:
            selected_strategy = "project_history"
            selected_expression = None
            selected_count = history_count

    retrieval = _retrieval_metadata(
        plan=plan,
        strategy=selected_strategy,
        project=project_resolution,
        stage_counts=stage_counts,
    )
    if selected_count == 0:
        return _empty_search_page(normalized_page_size, retrieval=retrieval)
    return _run_search_stage(
        connection,
        base_conditions=base_conditions,
        base_params=base_params,
        match_expression=selected_expression,
        total_count=selected_count,
        page=page,
        page_size=normalized_page_size,
        prefer_recent=plan.prefer_recent,
        retrieval=retrieval,
    )
