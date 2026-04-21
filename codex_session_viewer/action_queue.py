from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

from .projects import effective_project_fields
from .session_view import build_turns, parse_record_payload, parse_timestamp
from .text_utils import shorten

ACTION_QUEUE_SCORE_THRESHOLD = 40
MAX_ACTION_QUEUE_CANDIDATE_SESSIONS = 48
MAX_ACTION_QUEUE_TURNS_PER_SESSION = 3

EVENT_VIEW_COLUMNS = """
    e.session_id,
    e.event_index,
    e.timestamp,
    e.record_type,
    e.payload_type,
    e.kind,
    e.role,
    e.title,
    e.display_text,
    e.detail_text,
    e.tool_name,
    e.call_id,
    e.command_text,
    e.exit_code,
    e.record_json
"""

EXPLORATORY_PARSED_COMMAND_TYPES = {"search", "list_files", "read"}

MODULE_NOT_FOUND_RE = re.compile(r"no module named ['\"]([^'\"]+)['\"]", re.IGNORECASE)


def build_homepage_action_queue(
    connection: sqlite3.Connection,
    rows: list[sqlite3.Row],
    repo_groups: list[object],
    *,
    limit: int = 6,
    candidate_session_limit: int = MAX_ACTION_QUEUE_CANDIDATE_SESSIONS,
    turns_per_session: int = MAX_ACTION_QUEUE_TURNS_PER_SESSION,
) -> list[dict[str, object]]:
    candidate_rows = [row for row in rows[: max(candidate_session_limit, 0)] if str(row["id"] or "").strip()]
    if not candidate_rows:
        return []

    session_ids = [str(row["id"]) for row in candidate_rows]
    turn_windows = _fetch_recent_turn_windows(connection, session_ids, turns_per_session=turns_per_session)
    if not turn_windows:
        return []

    events_by_session = _fetch_recent_session_events(connection, session_ids, turns_per_session=turns_per_session)
    if not events_by_session:
        return []

    session_analyses: list[dict[str, object]] = []
    for row in candidate_rows:
        session_id = str(row["id"])
        events = events_by_session.get(session_id)
        window = turn_windows.get(session_id)
        if not events or window is None:
            continue
        turns = build_turns(
            events,
            cwd=str(row["cwd"] or "").strip() or None,
            starting_turn_number=int(window["oldest_turn_number"]),
        )
        if not turns:
            continue
        session_analyses.append(
            {
                "row": row,
                "project": effective_project_fields(row),
                "turns": turns,
            }
        )

    if not session_analyses:
        return []

    project_href_by_key = {str(group.key): group.detail_href for group in repo_groups}
    verification_successes = _collect_latest_verification_successes(session_analyses)

    issue_candidates: list[dict[str, object]] = []
    for analysis in session_analyses:
        row = analysis["row"]
        project = analysis["project"]
        turns = analysis["turns"]
        for issue in _extract_turn_issues(
            row=row,
            project=project,
            turns=turns,
            verification_successes=verification_successes,
        ):
            issue["project_href"] = project_href_by_key.get(str(project["effective_group_key"]), "/")
            turn_number = int(issue.get("turn_number") or 0)
            session_id = str(row["id"])
            issue["session_href"] = (
                f"/sessions/{quote(session_id, safe='')}?view=audit&turn={turn_number}&focus=1"
                if turn_number > 0
                else f"/sessions/{quote(session_id, safe='')}"
            )
            issue_candidates.append(issue)

    return _dedupe_and_rank_issue_candidates(issue_candidates, limit=limit)


def _fetch_recent_turn_windows(
    connection: sqlite3.Connection,
    session_ids: list[str],
    *,
    turns_per_session: int,
) -> dict[str, sqlite3.Row]:
    if not session_ids:
        return {}

    placeholders = ", ".join("?" for _ in session_ids)
    rows = connection.execute(
        f"""
        WITH ranked AS (
            SELECT
                session_id,
                turn_number,
                start_event_index,
                end_event_index,
                latest_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY session_id
                    ORDER BY turn_number DESC
                ) AS rn
            FROM session_turns
            WHERE session_id IN ({placeholders})
        )
        SELECT
            session_id,
            MIN(turn_number) AS oldest_turn_number,
            MAX(turn_number) AS newest_turn_number,
            MIN(start_event_index) AS start_event_index,
            MAX(end_event_index) AS end_event_index,
            MAX(latest_timestamp) AS latest_timestamp
        FROM ranked
        WHERE rn <= ?
        GROUP BY session_id
        """,
        (*session_ids, max(int(turns_per_session or 1), 1)),
    ).fetchall()
    return {str(row["session_id"]): row for row in rows}


def _fetch_recent_session_events(
    connection: sqlite3.Connection,
    session_ids: list[str],
    *,
    turns_per_session: int,
) -> dict[str, list[sqlite3.Row]]:
    if not session_ids:
        return {}

    placeholders = ", ".join("?" for _ in session_ids)
    rows = connection.execute(
        f"""
        WITH ranked AS (
            SELECT
                session_id,
                turn_number,
                start_event_index,
                end_event_index,
                ROW_NUMBER() OVER (
                    PARTITION BY session_id
                    ORDER BY turn_number DESC
                ) AS rn
            FROM session_turns
            WHERE session_id IN ({placeholders})
        ),
        selected AS (
            SELECT
                session_id,
                MIN(start_event_index) AS start_event_index,
                MAX(end_event_index) AS end_event_index
            FROM ranked
            WHERE rn <= ?
            GROUP BY session_id
        )
        SELECT
            {EVENT_VIEW_COLUMNS}
        FROM events AS e
        JOIN selected AS s
          ON s.session_id = e.session_id
         AND e.event_index BETWEEN s.start_event_index AND s.end_event_index
        ORDER BY e.session_id ASC, e.event_index ASC
        """,
        (*session_ids, max(int(turns_per_session or 1), 1)),
    ).fetchall()

    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["session_id"]), []).append(row)
    return grouped


def _collect_latest_verification_successes(
    session_analyses: list[dict[str, object]],
) -> dict[tuple[str, str, str], datetime]:
    successes: dict[tuple[str, str, str], datetime] = {}
    for analysis in session_analyses:
        project = analysis["project"]
        repo_key = str(project["effective_group_key"] or "")
        host = str(project["source_host"] or "")
        turns = analysis["turns"]
        for turn in turns:
            verdict = turn.get("audit_verification_verdict", {})
            if str(verdict.get("status") or "") not in {"passed", "passed_with_warnings"}:
                continue
            timestamp = _issue_timestamp(turn)
            if timestamp is None:
                continue
            for label in _verification_labels(turn):
                marker = (repo_key, host, label)
                previous = successes.get(marker)
                if previous is None or timestamp > previous:
                    successes[marker] = timestamp
    return successes


def _extract_turn_issues(
    *,
    row: sqlite3.Row,
    project: dict[str, object],
    turns: list[dict[str, object]],
    verification_successes: dict[tuple[str, str, str], datetime],
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    repo_key = str(project["effective_group_key"] or "")
    host = str(project["source_host"] or "")

    for turn in turns:
        turn_items: list[dict[str, object]] = []

        verification_issue = _build_verification_failed_issue(row=row, project=project, turn=turn)
        if verification_issue is not None:
            labels = list(verification_issue.get("verification_labels") or [])
            issue_timestamp = _issue_timestamp(turn)
            superseded = False
            if issue_timestamp is not None:
                for label in labels:
                    success_timestamp = verification_successes.get((repo_key, host, label))
                    if success_timestamp is not None and success_timestamp > issue_timestamp:
                        superseded = True
                        break
            if not superseded:
                turn_items.append(verification_issue)

        claim_issue = _build_claim_mismatch_issue(row=row, project=project, turn=turn)
        if claim_issue is not None:
            turn_items.append(claim_issue)

        risky_issue = _build_risky_unverified_issue(row=row, project=project, turn=turn)
        if risky_issue is not None:
            turn_items.append(risky_issue)

        setup_issue = _build_setup_blocker_issue(row=row, project=project, turn=turn)
        if setup_issue is not None:
            turn_items.append(setup_issue)

        approval_issue = _build_approval_blocked_issue(row=row, project=project, turn=turn)
        if approval_issue is not None:
            turn_items.append(approval_issue)

        guardian_issue = _build_guardian_denied_issue(row=row, project=project, turn=turn)
        if guardian_issue is not None:
            turn_items.append(guardian_issue)

        mcp_issue = _build_mcp_startup_issue(row=row, project=project, turn=turn)
        if mcp_issue is not None:
            turn_items.append(mcp_issue)

        has_verification_failure = verification_issue is not None
        aborted_issue = _build_aborted_after_changes_issue(
            row=row,
            project=project,
            turn=turn,
            has_verification_failure=has_verification_failure,
        )
        if aborted_issue is not None:
            turn_items.append(aborted_issue)

        items.extend(turn_items)

    return items


def _build_verification_failed_issue(
    *,
    row: sqlite3.Row,
    project: dict[str, object],
    turn: dict[str, object],
) -> dict[str, object] | None:
    verdict = turn.get("audit_verification_verdict", {})
    if str(verdict.get("status") or "") != "failed":
        return None

    summary = turn.get("audit_summary", {})
    patch_count = int(summary.get("patch_count") or 0)
    file_count = int(summary.get("files_touched_count") or 0)
    if patch_count <= 0 and file_count <= 0:
        return None

    labels = _verification_labels(turn)
    label_summary = ", ".join(labels[:2]) if labels else "Recorded verification commands"
    detail_parts = [f"{label_summary} exited non-zero"]
    if patch_count > 0:
        detail_parts.append(f"after {_count_label(patch_count, 'patch')}")
    elif file_count > 0:
        detail_parts.append(f"after {_count_label(file_count, 'file change')}")

    return _build_issue(
        row=row,
        project=project,
        turn=turn,
        issue_kind="verification_failed",
        signature="verification_failed:" + "|".join(labels or ["verification"]),
        status_tone="rose",
        status_label="Verification failed",
        title="Verification failed after code changes",
        status_title=" · ".join(detail_parts),
        next_action=f"Fix the failure and rerun {label_summary.lower()}.",
        severity=45,
        files=_turn_files_touched(turn),
        badges=[
            _badge(f"{_count_label(len(labels) or 1, 'check')} failed", "rose"),
            _badge(_count_label(max(patch_count, file_count), "changed item"), "stone"),
        ],
        verification_labels=labels,
    )


def _build_claim_mismatch_issue(
    *,
    row: sqlite3.Row,
    project: dict[str, object],
    turn: dict[str, object],
) -> dict[str, object] | None:
    warnings = turn.get("audit_response_evidence", {}).get("warnings") or []
    if not isinstance(warnings, list) or not warnings:
        return None

    mismatch_count = len([warning for warning in warnings if isinstance(warning, str) and warning.strip()])
    if mismatch_count <= 0:
        return None

    return _build_issue(
        row=row,
        project=project,
        turn=turn,
        issue_kind="claim_evidence_mismatch",
        signature="claim_evidence_mismatch",
        status_tone="rose",
        status_label="Evidence mismatch",
        title="Response claims do not match the recorded work",
        status_title=shorten(str(warnings[0]), 140),
        next_action="Review the response against the commands, patches, and verification evidence before trusting it.",
        severity=40,
        files=_turn_files_touched(turn),
        badges=[_badge(_count_label(mismatch_count, "mismatch"), "rose")],
    )


def _build_risky_unverified_issue(
    *,
    row: sqlite3.Row,
    project: dict[str, object],
    turn: dict[str, object],
) -> dict[str, object] | None:
    risky_files = turn.get("audit_risky_files") or []
    if not isinstance(risky_files, list) or not risky_files:
        return None

    paths = [str(item.get("path") or "").strip() for item in risky_files if isinstance(item, dict)]
    kinds = sorted(
        {
            str(item.get("kind") or "").strip()
            for item in risky_files
            if isinstance(item, dict) and str(item.get("kind") or "").strip()
        }
    )
    if not paths:
        return None

    detail = ", ".join(paths[:2])
    if len(paths) > 2:
        detail += f" +{len(paths) - 2} more"

    return _build_issue(
        row=row,
        project=project,
        turn=turn,
        issue_kind="risky_changes_unverified",
        signature="risky_changes_unverified:" + "|".join(paths),
        status_tone="amber",
        status_label="Needs verification",
        title="Risky changes were left unverified",
        status_title=detail,
        next_action="Run targeted verification for the risky files changed in this turn.",
        severity=35,
        files=paths,
        badges=[
            _badge(_count_label(len(paths), "risky file"), "amber"),
            _badge(", ".join(kinds[:2]) or "Risky scope", "stone"),
        ],
    )


def _build_setup_blocker_issue(
    *,
    row: sqlite3.Row,
    project: dict[str, object],
    turn: dict[str, object],
) -> dict[str, object] | None:
    blockers: dict[str, dict[str, str]] = {}
    for command in turn.get("audit_failed_commands", []):
        blocker = _setup_blocker_from_command(command)
        if blocker is None:
            continue
        blockers.setdefault(blocker["signature"], blocker)

    if not blockers:
        return None

    ordered_blockers = list(blockers.values())
    detail = "; ".join(blocker["title"] for blocker in ordered_blockers[:2])
    if len(ordered_blockers) > 2:
        detail += f" +{len(ordered_blockers) - 2} more"

    if len(ordered_blockers) == 1:
        next_action = ordered_blockers[0]["next_action"]
    else:
        next_action = "Install the missing prerequisites and rerun the blocked command or verification step."

    return _build_issue(
        row=row,
        project=project,
        turn=turn,
        issue_kind="setup_blocker",
        signature="setup_blocker:" + "|".join(sorted(blockers)),
        status_tone="rose",
        status_label="Setup blocker",
        title="Environment setup blocked the requested work",
        status_title=detail,
        next_action=next_action,
        severity=25,
        files=_turn_files_touched(turn),
        badges=[
            _badge(_count_label(len(blockers), "blocker"), "rose"),
            _badge("Missing prerequisite", "stone"),
        ],
    )


def _build_approval_blocked_issue(
    *,
    row: sqlite3.Row,
    project: dict[str, object],
    turn: dict[str, object],
) -> dict[str, object] | None:
    if str(turn.get("response_state") or "") == "final":
        return None

    approval_events = _event_payloads(
        turn,
        {"exec_approval_request", "apply_patch_approval_request", "request_permissions"},
    )
    if not approval_events:
        return None

    signatures: list[str] = []
    detail_parts: list[str] = []
    files: list[str] = []
    for payload_type, payload in approval_events:
        if payload_type == "request_permissions":
            permission_shape = _describe_permissions(payload.get("permissions"))
            signatures.append(f"request_permissions:{permission_shape}")
            detail_parts.append(f"Requested {permission_shape}")
            continue

        if payload_type == "apply_patch_approval_request":
            change_paths = _payload_change_paths(payload)
            files.extend(change_paths)
            grant_root = str(payload.get("grant_root") or "").strip()
            if grant_root:
                signatures.append(f"patch_approval:{grant_root}")
                detail_parts.append(f"Patch needed write access under {grant_root}")
            else:
                signatures.append("patch_approval")
                detail_parts.append("Patch required approval before it could run")
            continue

        permission_shape = _describe_permissions(payload.get("additional_permissions"))
        command_label = _payload_command_label(payload)
        signature = f"exec_approval:{permission_shape or command_label or 'command'}"
        signatures.append(signature)
        if permission_shape:
            detail_parts.append(f"Command needed {permission_shape}")
        elif command_label:
            detail_parts.append(f"{command_label} required approval")

    detail = " · ".join(_unique_strings(detail_parts)[:3]) or "A requested action was blocked on approval."
    return _build_issue(
        row=row,
        project=project,
        turn=turn,
        issue_kind="approval_blocked",
        signature="approval_blocked:" + "|".join(sorted(signatures)),
        status_tone="amber",
        status_label="Approval blocked",
        title="Approval or permissions blocked this turn",
        status_title=detail,
        next_action="Grant the requested access or adjust the session policy, then retry the blocked action.",
        severity=35,
        files=files or _turn_files_touched(turn),
        badges=[
            _badge(_count_label(len(signatures), "approval"), "amber"),
            _badge(str(turn.get("response_state") or "incomplete").replace("_", " "), "stone"),
        ],
    )


def _build_guardian_denied_issue(
    *,
    row: sqlite3.Row,
    project: dict[str, object],
    turn: dict[str, object],
) -> dict[str, object] | None:
    guardian_events = []
    for payload_type, payload in _event_payloads(turn, {"guardian_assessment"}):
        status = str(payload.get("status") or "").strip().lower()
        risk_level = str(payload.get("risk_level") or "").strip().lower()
        if status not in {"denied", "timed_out"}:
            continue
        if risk_level not in {"high", "critical"}:
            continue
        guardian_events.append(payload)

    if not guardian_events:
        return None

    first = guardian_events[0]
    action = first.get("action") if isinstance(first.get("action"), dict) else {}
    action_type = str(action.get("type") or "").strip().lower() or "action"
    rationale = str(first.get("rationale") or "").strip()
    detail = rationale or f"Guardian {str(first.get('status') or '').replace('_', ' ')} a {action_type.replace('_', ' ')} request."

    files = _guardian_action_files(action)
    risk_level = str(first.get("risk_level") or "").strip().lower() or "high"
    status = str(first.get("status") or "").strip().lower() or "denied"
    return _build_issue(
        row=row,
        project=project,
        turn=turn,
        issue_kind="guardian_denied",
        signature=f"guardian_denied:{status}:{risk_level}:{action_type}",
        status_tone="rose",
        status_label="Guardian denied",
        title="High-risk action was denied by guardian review",
        status_title=shorten(detail, 160),
        next_action="Review the requested action and either authorize it explicitly or change approach.",
        severity=50,
        files=files or _turn_files_touched(turn),
        badges=[
            _badge(risk_level, "rose"),
            _badge(action_type.replace("_", " "), "stone"),
        ],
    )


def _build_mcp_startup_issue(
    *,
    row: sqlite3.Row,
    project: dict[str, object],
    turn: dict[str, object],
) -> dict[str, object] | None:
    failures: list[dict[str, str]] = []
    for _, payload in _event_payloads(turn, {"mcp_startup_complete"}):
        failed_items = payload.get("failed")
        if not isinstance(failed_items, list):
            continue
        for item in failed_items:
            if not isinstance(item, dict):
                continue
            server = str(item.get("server") or "").strip()
            error = str(item.get("error") or "").strip()
            if server:
                failures.append({"server": server, "error": error})

    if not failures:
        return None

    detail = "; ".join(
        f"{item['server']}: {shorten(item['error'] or 'startup failed', 80)}"
        for item in failures[:2]
    )
    if len(failures) > 2:
        detail += f" +{len(failures) - 2} more"

    servers = sorted({item["server"] for item in failures})
    return _build_issue(
        row=row,
        project=project,
        turn=turn,
        issue_kind="mcp_startup_failed",
        signature="mcp_startup_failed:" + "|".join(servers),
        status_tone="rose",
        status_label="MCP failed",
        title="MCP startup failed for required tools",
        status_title=detail,
        next_action="Fix the failing MCP server configuration and retry the workflow.",
        severity=30,
        files=_turn_files_touched(turn),
        badges=[
            _badge(_count_label(len(servers), "server"), "rose"),
            _badge("Tooling unavailable", "stone"),
        ],
    )


def _build_aborted_after_changes_issue(
    *,
    row: sqlite3.Row,
    project: dict[str, object],
    turn: dict[str, object],
    has_verification_failure: bool,
) -> dict[str, object] | None:
    if str(turn.get("response_state") or "") != "canceled":
        return None

    abort_reason = str(turn.get("abort_reason") or "").strip().lower()
    if abort_reason == "replaced":
        return None

    summary = turn.get("audit_summary", {})
    patch_count = int(summary.get("patch_count") or 0)
    if patch_count <= 0 and not has_verification_failure:
        return None

    detail_parts: list[str] = []
    if abort_reason:
        detail_parts.append(abort_reason.replace("_", " "))
    if patch_count > 0:
        detail_parts.append(f"after {_count_label(patch_count, 'patch')}")
    if has_verification_failure:
        detail_parts.append("with failed verification pending")

    return _build_issue(
        row=row,
        project=project,
        turn=turn,
        issue_kind="aborted_after_changes",
        signature="aborted_after_changes:" + (abort_reason or "canceled"),
        status_tone="amber",
        status_label="Interrupted",
        title="Turn ended before changed work was closed out",
        status_title=" · ".join(detail_parts) or "The turn stopped after making changes.",
        next_action="Resume the work or rerun verification before relying on the recorded changes.",
        severity=20,
        files=_turn_files_touched(turn),
        badges=[
            _badge("turn aborted", "amber"),
            _badge(str(patch_count or 0) + " patches", "stone") if patch_count > 0 else _badge("verification pending", "stone"),
        ],
    )


def _dedupe_and_rank_issue_candidates(
    issue_candidates: list[dict[str, object]],
    *,
    limit: int,
) -> list[dict[str, object]]:
    if not issue_candidates:
        return []

    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
    for item in issue_candidates:
        fingerprint = (
            str(item.get("project_key") or ""),
            str(item.get("host") or ""),
            str(item.get("issue_kind") or ""),
            str(item.get("signature") or ""),
        )
        grouped.setdefault(fingerprint, []).append(item)

    now = datetime.now(tz=UTC)
    ranked: list[dict[str, object]] = []
    for duplicates in grouped.values():
        duplicates.sort(key=_issue_sort_key, reverse=True)
        primary = dict(duplicates[0])
        recent_repeat_count = sum(
            1
            for item in duplicates
            if (timestamp := parse_timestamp(str(item.get("timestamp") or ""))) is not None
            and (now - timestamp.astimezone(UTC)) <= timedelta(days=1)
        )
        score = _score_issue(
            primary,
            now=now,
            recent_repeat_count=recent_repeat_count,
        )
        if score < ACTION_QUEUE_SCORE_THRESHOLD:
            continue
        badges = list(primary.get("signal_badges") or [])
        if len(duplicates) > 1:
            badges.append(_badge(_count_label(len(duplicates), "repeat"), "stone"))
        primary["signal_badges"] = badges
        primary["attention_score"] = score
        ranked.append(primary)

    ranked.sort(key=lambda item: (int(item.get("attention_score") or 0), _issue_sort_key(item)), reverse=True)
    return ranked[:limit]


def _score_issue(
    issue: dict[str, object],
    *,
    now: datetime,
    recent_repeat_count: int,
) -> int:
    severity = int(issue.get("severity") or 0)
    score = severity
    score += _recency_points(str(issue.get("timestamp") or ""), now=now)
    score += _repo_risk_points(issue.get("files") or [])
    score += min(max(recent_repeat_count - 1, 0) * 5, 15)
    score -= int(issue.get("noise_penalty") or 0)
    return max(0, min(score, 100))


def _recency_points(timestamp_value: str, *, now: datetime) -> int:
    timestamp = parse_timestamp(timestamp_value)
    if timestamp is None:
        return 0
    age = now - timestamp.astimezone(UTC)
    if age <= timedelta(days=1):
        return 20
    if age <= timedelta(days=7):
        return 10
    return 5


def _repo_risk_points(files: object) -> int:
    if not isinstance(files, list):
        return 0
    scores = [_path_risk_points(str(path or "")) for path in files if str(path or "").strip()]
    return max(scores, default=0)


def _path_risk_points(path: str) -> int:
    normalized = path.strip().lower().lstrip("./")
    if not normalized:
        return 0

    parts = [part for part in Path(normalized).parts if part not in {"", "."}]
    basename = parts[-1] if parts else normalized
    doc_suffixes = {".md", ".rst", ".txt", ".adoc"}
    code_suffixes = {
        ".c",
        ".cc",
        ".cpp",
        ".cs",
        ".go",
        ".h",
        ".hpp",
        ".java",
        ".js",
        ".jsx",
        ".mjs",
        ".py",
        ".rb",
        ".rs",
        ".swift",
        ".ts",
        ".tsx",
    }

    if basename in {
        "cargo.toml",
        "cargo.lock",
        "module.bazel.lock",
        "pyproject.toml",
        "package.json",
        "pnpm-lock.yaml",
        "yarn.lock",
        "poetry.lock",
        "requirements.txt",
        "requirements-dev.txt",
    }:
        return 20
    if normalized.startswith(".github/workflows/"):
        return 20
    if any(
        part in {
            "auth",
            "config",
            "core",
            "deploy",
            "deployment",
            "infra",
            "k8s",
            "migrations",
            "ops",
            "permissions",
            "protocol",
            "security",
            "terraform",
        }
        for part in parts
    ):
        return 20
    if any(part in {"api", "app", "cli", "client", "routes", "server", "templates", "tui", "ui", "web"} for part in parts):
        return 10
    suffix = Path(normalized).suffix.lower()
    if suffix in code_suffixes:
        return 5
    if suffix in doc_suffixes:
        return 0
    return 5 if suffix else 0


def _setup_blocker_from_command(command: object) -> dict[str, str] | None:
    if not isinstance(command, dict):
        return None

    if str(command.get("command_intent") or "") == "inspect":
        return None

    parsed_command_types = {
        str(item).strip().lower()
        for item in (command.get("parsed_command_types") or [])
        if str(item).strip()
    }
    if parsed_command_types and parsed_command_types.issubset(EXPLORATORY_PARSED_COMMAND_TYPES):
        return None

    command_label = _command_label(command)
    command_text = str(command.get("display_command_text") or command.get("command_text") or "").strip()
    output_text = "\n".join(
        part
        for part in [
            str(command.get("output_text") or "").strip(),
            str(command.get("detail_text") or "").strip(),
        ]
        if part
    )
    output_lower = output_text.lower()

    module_match = MODULE_NOT_FOUND_RE.search(output_text)
    if module_match:
        module_name = module_match.group(1).strip().lower()
        return {
            "signature": f"missing_module:{module_name}",
            "title": f"Python dependency `{module_name}` is missing",
            "next_action": f"Install `{module_name}` in the environment and rerun the blocked command.",
        }

    if "ensurepip" in output_lower or "python3-venv" in output_lower:
        return {
            "signature": "missing_venv_support",
            "title": "Python venv support is missing",
            "next_action": "Install `python3-venv` or equivalent venv support and retry environment bootstrap.",
        }

    primary_token = _command_primary_token(command_text or command_label)
    if primary_token and ("command not found" in output_lower or re.search(rf"\b{re.escape(primary_token)}: not found\b", output_lower)):
        return {
            "signature": f"missing_binary:{primary_token}",
            "title": f"`{primary_token}` is unavailable",
            "next_action": f"Install `{primary_token}` and rerun the blocked command.",
        }

    return None


def _build_issue(
    *,
    row: sqlite3.Row,
    project: dict[str, object],
    turn: dict[str, object],
    issue_kind: str,
    signature: str,
    status_tone: str,
    status_label: str,
    title: str,
    status_title: str,
    next_action: str,
    severity: int,
    files: list[str],
    badges: list[dict[str, str]],
    verification_labels: list[str] | None = None,
    noise_penalty: int = 0,
) -> dict[str, object]:
    timestamp = _issue_timestamp_value(turn, row)
    return {
        "issue_kind": issue_kind,
        "signature": signature,
        "severity": severity,
        "noise_penalty": noise_penalty,
        "session_id": str(row["id"]),
        "turn_number": int(turn.get("number") or 0),
        "project_key": str(project["effective_group_key"] or ""),
        "project_label": str(project["display_label"] or "Session"),
        "host": str(project["source_host"] or ""),
        "timestamp": timestamp,
        "status_tone": status_tone,
        "status_label": status_label,
        "title": title,
        "status_title": status_title,
        "next_action": next_action,
        "signal_badges": [badge for badge in badges if badge.get("label")],
        "files": [path for path in files if path],
        "verification_labels": list(verification_labels or []),
    }


def _event_payloads(
    turn: dict[str, object],
    payload_types: set[str],
) -> list[tuple[str, dict[str, object]]]:
    matches: list[tuple[str, dict[str, object]]] = []
    for event in turn.get("merged_detail_events", []):
        if not isinstance(event, dict):
            continue
        payload_type = str(event.get("payload_type") or "").strip().lower()
        if payload_type not in payload_types:
            continue
        payload = parse_record_payload(event.get("record_json"))
        if isinstance(payload, dict):
            matches.append((payload_type, payload))
    return matches


def _issue_timestamp(turn: dict[str, object]) -> datetime | None:
    return parse_timestamp(_issue_timestamp_value(turn, None))


def _issue_timestamp_value(turn: dict[str, object], row: sqlite3.Row | None) -> str:
    for candidate in (
        turn.get("latest_event_timestamp"),
        turn.get("response_timestamp"),
        turn.get("prompt_timestamp"),
        row["last_turn_timestamp"] if row is not None else None,
        row["session_timestamp"] if row is not None else None,
        row["started_at"] if row is not None else None,
        row["imported_at"] if row is not None else None,
    ):
        value = str(candidate or "").strip()
        if value:
            return value
    return ""


def _issue_sort_key(item: dict[str, object]) -> tuple[datetime, int]:
    timestamp = parse_timestamp(str(item.get("timestamp") or ""))
    if timestamp is None:
        timestamp = datetime.fromtimestamp(0, tz=UTC)
    return (timestamp.astimezone(UTC), int(item.get("severity") or 0))


def _verification_labels(turn: dict[str, object]) -> list[str]:
    labels = [
        _command_label(command)
        for command in turn.get("audit_verification_commands", [])
    ]
    deduped = []
    seen: set[str] = set()
    for label in labels:
        if not label or label in seen:
            continue
        seen.add(label)
        deduped.append(label)
    return deduped


def _command_label(command: object) -> str:
    if not isinstance(command, dict):
        return ""
    for candidate in (
        command.get("display_command_text"),
        command.get("primary_command_label"),
        command.get("command_text"),
        command.get("summary_text"),
    ):
        value = str(candidate or "").strip()
        if value:
            return shorten(value, 80)
    return ""


def _payload_command_label(payload: dict[str, object]) -> str:
    parsed_commands = payload.get("parsed_cmd")
    if isinstance(parsed_commands, list):
        for item in parsed_commands:
            if not isinstance(item, dict):
                continue
            command_text = str(item.get("cmd") or "").strip()
            if command_text:
                return shorten(command_text, 80)
    command = payload.get("command")
    if isinstance(command, list):
        joined = " ".join(str(part).strip() for part in command if str(part).strip())
        if joined:
            return shorten(joined, 80)
    return ""


def _payload_change_paths(payload: dict[str, object]) -> list[str]:
    changes = payload.get("changes")
    if not isinstance(changes, dict):
        return []
    return sorted(str(path).strip() for path in changes if str(path).strip())


def _guardian_action_files(action: dict[str, object]) -> list[str]:
    action_type = str(action.get("type") or "").strip().lower()
    if action_type == "apply_patch":
        files = action.get("files")
        if isinstance(files, list):
            return sorted(str(path).strip() for path in files if str(path).strip())
    return []


def _describe_permissions(value: object) -> str:
    if not isinstance(value, dict):
        return "additional permissions"
    parts: list[str] = []
    if value.get("file_system") is not None:
        parts.append("filesystem access")
    if value.get("network") is not None:
        parts.append("network access")
    return " and ".join(parts) if parts else "additional permissions"


def _turn_files_touched(turn: dict[str, object]) -> list[str]:
    files = turn.get("audit_files_touched") or []
    if not isinstance(files, list):
        return []
    return [str(path).strip() for path in files if str(path).strip()]


def _command_primary_token(command_text: str) -> str:
    text = command_text.strip()
    if not text:
        return ""
    return Path(text.split()[0]).name


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def _badge(label: str, tone: str) -> dict[str, str]:
    return {"label": label, "tone": tone}


def _count_label(count: int, noun: str) -> str:
    normalized_count = max(int(count or 0), 0)
    if normalized_count == 1:
        return f"1 {noun}"
    return f"{normalized_count} {noun}s"
