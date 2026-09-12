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
        item["href"] = "/assessments?" + urlencode({"machine": item["source_host"], "project": project, "review": review, "days": days})
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
        item["assessment_href"] = session_href + "/assessment?" + urlencode({
            "start_turn": 1, "end_turn": item["turn_count"] if source else 1}) if item["turn_count"] else None
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
    filters = {"machine": machine, "project": project, "review": review, "days": days}

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
