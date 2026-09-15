"""Cross-machine assessment discovery with bounded, source-backed page metrics."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import sqlite3
from urllib.parse import quote, urlencode

from .machine_aliases import list_machine_display_aliases, machine_display_name
from .llm_grader import read_run
from .projects import ProjectAccessContext, project_access_condition_sql
from .task_assessment import DEFAULT_POLICY, report_for_source, task_source, validate_policy

PAGE_SIZE = 10
PROJECT_KEY = "COALESCE(p.id, s.inferred_project_key)"
PROJECT_LABEL = "COALESCE(NULLIF(p.display_label, ''), NULLIF(o.override_display_label, ''), s.inferred_project_label)"
ACTIVITY = "COALESCE(s.last_turn_timestamp, s.started_at, s.session_timestamp, s.imported_at)"
JOINS = """
    FROM sessions s
    LEFT JOIN project_sources ps ON ps.match_project_key = s.inferred_project_key
    LEFT JOIN projects p ON p.id = ps.project_id
    LEFT JOIN project_overrides o ON o.match_project_key = s.inferred_project_key
"""


def dashboard_data(connection: sqlite3.Connection, *, owner: str, access: ProjectAccessContext,
                   machine: str = "", project: str = "", review: str = "all",
                   days: int = 0, page: int = 1) -> dict:
    """Apply ACLs before counts, options and pagination; never scan fleet traces."""
    access_sql, access_params = project_access_condition_sql(access)
    visible_where = access_sql or "1 = 1"
    aliases = list_machine_display_aliases(connection)
    machines = [dict(row) for row in connection.execute(
        f"SELECT DISTINCT s.source_host {JOINS} WHERE {visible_where} ORDER BY s.source_host", access_params)]
    for item in machines:
        item["label"] = machine_display_name(item["source_host"], aliases.get(item["source_host"])) or "Unknown machine"
    projects = [dict(row) for row in connection.execute(
        f"SELECT DISTINCT {PROJECT_KEY} AS id, {PROJECT_LABEL} AS label {JOINS} "
        f"WHERE {visible_where} ORDER BY label, id", access_params)]
    conditions = [visible_where]
    params: list = list(access_params)
    if machine:
        conditions.append("s.source_host = ?")
        params.append(machine)
    if project:
        conditions.append(f"{PROJECT_KEY} = ?")
        params.append(project)
    if days:
        conditions.append(f"julianday({ACTIVITY}) >= julianday(?)")
        params.append((datetime.now(UTC) - timedelta(days=days)).isoformat())
    # The owner/session/range index bounds each lookup without reading snapshots.
    latest_review = "(SELECT MAX(r.id) FROM task_assessment_revisions r WHERE r.owner_scope = ? AND r.session_id = s.id)"
    cte = f"""WITH candidates AS (
        SELECT s.id, s.summary, s.source_host, s.model_provider, s.source, s.forked_from_id,
               s.cli_version, s.cwd, s.git_commit_hash, s.git_repository_url,
               s.turn_count, s.event_count, {ACTIVITY} AS activity_at,
               {PROJECT_KEY} AS project_id, {PROJECT_LABEL} AS project_label,
               {latest_review} AS review_id
        {JOINS} WHERE {' AND '.join(conditions)}
    ), filtered AS (SELECT * FROM candidates
        {"WHERE review_id IS NOT NULL" if review == "reviewed" else "WHERE review_id IS NULL" if review == "unreviewed" else ""})
    """
    query_params = [owner, *params]
    systems = [dict(row) for row in connection.execute(cte + """
        SELECT source_host, COUNT(*) AS sessions, SUM(turn_count) AS turns,
               COUNT(review_id) AS reviewed, MAX(activity_at) AS latest_activity
        FROM filtered GROUP BY source_host ORDER BY sessions DESC, source_host
    """, query_params)]
    for item in systems:
        item["label"] = machine_display_name(item["source_host"], aliases.get(item["source_host"])) or "Unknown machine"
        item["href"] = "/assessments?" + urlencode({"view": "sessions", "machine": item["source_host"], "project": project, "review": review, "days": days})
    total = sum(item["sessions"] for item in systems)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, pages)
    rows = [dict(row) for row in connection.execute(cte + """
        SELECT f.*, r.start_turn AS review_start, r.end_turn AS review_end,
               r.created_at AS reviewed_at, r.evidence_digest AS review_digest, r.review_json
        FROM filtered f LEFT JOIN task_assessment_revisions r ON r.id = f.review_id
        ORDER BY f.activity_at DESC, f.id DESC LIMIT ? OFFSET ?
    """, [*query_params, PAGE_SIZE, (page - 1) * PAGE_SIZE])]
    policy = validate_policy(DEFAULT_POLICY)
    for item in rows:
        session_href = "/sessions/" + quote(item["id"], safe="")
        item["session_href"] = session_href
        item["machine_label"] = machine_display_name(item["source_host"], aliases.get(item["source_host"])) or "Unknown machine"
        item["metrics"] = None
        item["coverage"] = "unknown"
        item["limitation"] = None
        source = None
        # Do not silently truncate large sessions or substitute cumulative rollups.
        try:
            source = task_source(connection, item, 1, item["turn_count"])
            metrics = report_for_source(source, policy)["metrics"]
            # Evidence and intervals are available in the assessment, not duplicated
            # in a fleet export. Keep configuration observations for inspection.
            item["metrics"] = {key: value for key, value in metrics.items() if key != "intervals"}
            item["coverage"] = metrics["usage_coverage"]
        except ValueError as exc:
            item["coverage"] = "range_required" if item["turn_count"] else "not_indexed"
            item["limitation"] = str(exc)
        except LookupError as exc:
            item["coverage"] = "not_indexed"
            item["limitation"] = str(exc)
        item["assessment_href"] = (session_href + "/assessment?" + urlencode({
            "start_turn": 1, "end_turn": item["turn_count"]}) if source else
            session_href + "?view=conversation#chunk-picker") if item["turn_count"] else None
        item["review"] = None
        item["grade_run"] = read_run(connection, owner, item["id"], 1, item["turn_count"])
        if item["grade_run"]:
            item["grade_run"]["stale"] = source is None or item["grade_run"]["evidence_digest"] != source["evidence_digest"]
        if item["review_id"]:
            fields = json.loads(item.pop("review_json"))
            review_source = source
            if item["review_start"] != 1 or item["review_end"] != item["turn_count"]:
                try:
                    review_source = task_source(connection, item, item["review_start"], item["review_end"])
                except (ValueError, LookupError):
                    review_source = None
            item["review"] = {
                "id": item["review_id"], "start_turn": item["review_start"], "end_turn": item["review_end"],
                "outcome": fields["outcome"], "created_at": item["reviewed_at"],
                "stale": review_source is None or item["review_digest"] != review_source["evidence_digest"],
                "href": session_href + "/assessment?" + urlencode({"start_turn": item["review_start"], "end_turn": item["review_end"]}),
                "snapshot_href": session_href + "/assessment?" + urlencode({"start_turn": item["review_start"], "end_turn": item["review_end"], "revision": item["review_id"]}),
            }
            if review_source is None:
                item["review"]["href"] = item["review"]["snapshot_href"]
        else:
            item.pop("review_json", None)
    filters = {"view": "sessions", "machine": machine, "project": project, "review": review, "days": days}

    def page_href(target: int) -> str:
        return "/assessments?" + urlencode({**filters, "page": target})
    return {
        "sessions": rows, "systems": systems, "machines": machines, "projects": projects,
        "filters": filters, "total": total, "machine_count": len(systems),
        "reviewed_count": sum(item["reviewed"] for item in systems),
        "page": page, "pages": pages, "page_size": PAGE_SIZE,
        "previous_href": page_href(page - 1) if page > 1 else None,
        "next_href": page_href(page + 1) if page < pages else None,
        "export_href": "/assessments.json?" + urlencode({**filters, "page": page}),
        "policy": policy, "generated_at": datetime.now(UTC).isoformat(),
        "scope": "All accessible synced sessions; reviews belong to the current user. Metrics cover this page only, selected session only; child work excluded.",
    }


def grading_dashboard_data(connection, *, owner, access, active_run_ids=(), machine="",
                           project="", status="all", days=0, page=1):
    """Latest attempt per personal task range, without loading source snapshots."""
    access_sql, access_params = project_access_condition_sql(access)
    visible = access_sql or "1 = 1"
    aliases = list_machine_display_aliases(connection)
    machines = [dict(r) for r in connection.execute(
        f"SELECT DISTINCT s.source_host {JOINS} WHERE {visible} ORDER BY s.source_host", access_params)]
    for item in machines:
        item["label"] = machine_display_name(item["source_host"], aliases.get(item["source_host"])) or "Unknown machine"
    projects = [dict(r) for r in connection.execute(
        f"SELECT DISTINCT {PROJECT_KEY} AS id, {PROJECT_LABEL} AS label {JOINS} WHERE {visible} ORDER BY label, id", access_params)]
    conditions, params = [visible], list(access_params)
    for value, expression in ((machine, "s.source_host"), (project, PROJECT_KEY)):
        if value:
            conditions.append(f"{expression} = ?")
            params.append(value)
    if days:
        conditions.append("julianday(g.created_at) >= julianday(?)")
        params.append((datetime.now(UTC) - timedelta(days=days)).isoformat())
    # IDs come only from the current process's worker registry, never request text.
    active_sql = ','.join(str(int(i)) for i in active_run_ids) or 'NULL'
    cte = f"""WITH latest AS (
        SELECT MAX(id) AS id FROM task_grader_runs WHERE owner_scope = ?
        GROUP BY session_id, start_turn, end_turn
    ), candidates AS (
        SELECT g.id, s.id AS session_id, s.summary, s.source_host,
               {PROJECT_KEY} AS project_id, {PROJECT_LABEL} AS project_label,
               g.start_turn, g.end_turn, g.created_at,
               CASE WHEN g.status = 'running' AND g.id NOT IN ({active_sql if active_run_ids else '0'})
                    THEN 'interrupted' ELSE g.status END AS status
        {JOINS} JOIN task_grader_runs g ON g.session_id = s.id
        JOIN latest ON latest.id = g.id WHERE {' AND '.join(conditions)}
    ) """
    query_params = [owner, *params]
    counts = {r['status']: r['count'] for r in connection.execute(
        cte + "SELECT status, COUNT(*) AS count FROM candidates GROUP BY status", query_params)}
    total = sum(counts.values()) if status == 'all' else counts.get(status, 0)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, pages)
    where = "" if status == 'all' else "WHERE c.status = ?"
    rows = [dict(r) for r in connection.execute(cte + f"""
        SELECT c.*, CASE WHEN c.status = 'completed' THEN json_extract(g.result_json, '$.demand.outcome') END AS outcome,
               substr(json_extract(g.result_json, '$.demand.verification_notes'), 1, 400) AS explanation,
               substr(json_extract(g.result_json, '$.error'), 1, 400) AS error,
               COALESCE(json_array_length(g.result_json, '$.batches'), 0) AS total_batches,
               (SELECT COUNT(*) FROM json_each(g.result_json, '$.batches') b
                WHERE json_extract(b.value, '$.status') = 'completed') AS completed_batches
        FROM candidates c JOIN task_grader_runs g ON g.id = c.id {where}
        ORDER BY c.id DESC LIMIT ? OFFSET ?
    """, [*query_params, *([status] if status != 'all' else []), PAGE_SIZE, (page - 1) * PAGE_SIZE])]
    for item in rows:
        base = '/sessions/' + quote(item['session_id'], safe='')
        item['machine_label'] = machine_display_name(item['source_host'], aliases.get(item['source_host'])) or 'Unknown machine'
        item['href'] = base + '/assessment?' + urlencode({'start_turn': item['start_turn'], 'end_turn': item['end_turn']}) + '#llm-grader'
        item['result_href'] = base + f"/assessment/grader/{item['id']}"
        if not item['total_batches']:
            item['total_batches'] = 1
            item['completed_batches'] = int(item['status'] == 'completed')
    filters = {'view': 'runs', 'machine': machine, 'project': project, 'status': status, 'days': days}
    def href(target):
        return '/assessments?' + urlencode({**filters, 'page': target})
    return {'runs': rows, 'machines': machines, 'projects': projects, 'filters': filters,
            'counts': counts, 'total': total, 'page': page, 'pages': pages,
            'previous_href': href(page - 1) if page > 1 else None,
            'next_href': href(page + 1) if page < pages else None,
            'export_href': '/assessments.json?' + urlencode({**filters, 'page': page}),
            'scope': 'Latest grading attempt for each accessible personal task range. Outcomes describe the saved evidence; open a range to check for changes.'}
