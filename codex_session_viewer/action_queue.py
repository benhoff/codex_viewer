from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .action_queue_state import (
    action_queue_fingerprint,
    fetch_action_queue_states,
    filter_action_queue_items,
)
from .projects import effective_project_fields
from .session_view import build_turns, parse_record_payload, parse_timestamp
from .text_utils import shorten

ACTION_QUEUE_SCORE_THRESHOLD = 40
MAX_ACTION_QUEUE_CANDIDATE_SESSIONS = 48
MAX_ACTION_QUEUE_TURNS_PER_SESSION = 3
ACTION_QUEUE_ROLLUP_VERSION = 1

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


@dataclass
class VerificationSuccessMarkers:
    by_repo_host: dict[tuple[str, str], datetime]
    by_label: dict[tuple[str, str, str], datetime]
    by_file: dict[tuple[str, str, str], datetime]


def replace_session_action_queue_rollups(
    connection: sqlite3.Connection,
    session_id: str,
    events: Sequence[sqlite3.Row | dict[str, Any] | object] | None = None,
) -> None:
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return

    session_rows = _fetch_action_queue_session_rows(connection, [normalized_session_id])
    session_row = session_rows.get(normalized_session_id)
    _delete_action_queue_rollups(connection, [normalized_session_id])
    if session_row is None:
        return

    turn_events = _normalize_action_queue_events(
        events
        if events is not None
        else _fetch_action_queue_events(connection, [normalized_session_id]).get(normalized_session_id, [])
    )
    turns = build_turns(
        turn_events,
        cwd=_normalized_string(_row_value(session_row, "cwd")) or None,
    )
    project = _materialization_project_fields(session_row)
    issues = _extract_turn_issues(
        row=session_row,
        project=project,
        turns=turns,
        verification_successes=_empty_verification_success_markers(),
        apply_verification_clears=False,
    )

    signal_inserts = _materialized_signal_inserts(normalized_session_id, issues)
    if signal_inserts:
        connection.executemany(
            """
            INSERT INTO action_queue_signals (
                session_id,
                turn_number,
                issue_kind,
                signature,
                timestamp,
                severity,
                noise_penalty,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            signal_inserts,
        )

    success_inserts = _materialized_verification_success_inserts(
        normalized_session_id,
        turns,
        session_row,
    )
    if success_inserts:
        connection.executemany(
            """
            INSERT INTO action_queue_verification_successes (
                session_id,
                turn_number,
                timestamp,
                labels_json,
                files_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            success_inserts,
        )

    connection.execute(
        "UPDATE sessions SET action_queue_rollup_version = ? WHERE id = ?",
        (ACTION_QUEUE_ROLLUP_VERSION, normalized_session_id),
    )


def backfill_action_queue_rollups(connection: sqlite3.Connection) -> int:
    stale_rows = connection.execute(
        """
        SELECT id
        FROM sessions
        WHERE COALESCE(action_queue_rollup_version, 0) < ?
        ORDER BY id ASC
        """,
        (ACTION_QUEUE_ROLLUP_VERSION,),
    ).fetchall()
    session_ids = [str(row["id"] or "").strip() for row in stale_rows if str(row["id"] or "").strip()]
    if not session_ids:
        return 0

    session_rows = _fetch_action_queue_session_rows(connection, session_ids)
    events_by_session = _fetch_action_queue_events(connection, session_ids)
    _delete_action_queue_rollups(connection, session_ids)

    signal_inserts: list[tuple[Any, ...]] = []
    success_inserts: list[tuple[Any, ...]] = []
    for session_id in session_ids:
        session_row = session_rows.get(session_id)
        if session_row is None:
            continue
        turns = build_turns(
            _normalize_action_queue_events(events_by_session.get(session_id, [])),
            cwd=_normalized_string(_row_value(session_row, "cwd")) or None,
        )
        project = _materialization_project_fields(session_row)
        issues = _extract_turn_issues(
            row=session_row,
            project=project,
            turns=turns,
            verification_successes=_empty_verification_success_markers(),
            apply_verification_clears=False,
        )
        signal_inserts.extend(_materialized_signal_inserts(session_id, issues))
        success_inserts.extend(_materialized_verification_success_inserts(session_id, turns, session_row))

    if signal_inserts:
        connection.executemany(
            """
            INSERT INTO action_queue_signals (
                session_id,
                turn_number,
                issue_kind,
                signature,
                timestamp,
                severity,
                noise_penalty,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            signal_inserts,
        )

    if success_inserts:
        connection.executemany(
            """
            INSERT INTO action_queue_verification_successes (
                session_id,
                turn_number,
                timestamp,
                labels_json,
                files_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            success_inserts,
        )

    connection.executemany(
        "UPDATE sessions SET action_queue_rollup_version = ? WHERE id = ?",
        [(ACTION_QUEUE_ROLLUP_VERSION, session_id) for session_id in session_ids],
    )
    return len(session_ids)


def build_homepage_action_queue(
    connection: sqlite3.Connection,
    rows: list[sqlite3.Row],
    repo_groups: list[object],
    *,
    owner_scope: str | None = None,
    limit: int = 6,
    candidate_session_limit: int = MAX_ACTION_QUEUE_CANDIDATE_SESSIONS,
    turns_per_session: int = MAX_ACTION_QUEUE_TURNS_PER_SESSION,
) -> list[dict[str, object]]:
    candidate_rows = [row for row in rows[: max(candidate_session_limit, 0)] if str(row["id"] or "").strip()]
    if not candidate_rows:
        return []

    row_by_session = {str(row["id"]): row for row in candidate_rows}
    session_ids = list(row_by_session)
    signal_rows = _fetch_recent_materialized_signal_rows(
        connection,
        session_ids,
        turns_per_session=turns_per_session,
    )
    if not signal_rows:
        return []

    project_href_by_key = {str(group.key): group.detail_href for group in repo_groups}
    verification_successes = _collect_materialized_verification_successes(
        connection,
        candidate_rows,
    )

    issue_candidates: list[dict[str, object]] = []
    for signal_row in signal_rows:
        session_id = str(signal_row["session_id"] or "").strip()
        row = row_by_session.get(session_id)
        if row is None:
            continue
        project = effective_project_fields(row)
        issue = _hydrate_materialized_issue(
            signal_row=signal_row,
            row=row,
            project=project,
            project_href=project_href_by_key.get(str(project["effective_group_key"]), "/"),
        )
        if _issue_cleared_by_later_verification(
            issue,
            repo_key=str(project["effective_group_key"] or ""),
            host=str(project["source_host"] or ""),
            issue_timestamp=parse_timestamp(str(issue.get("timestamp") or "")),
            verification_successes=verification_successes,
        ):
            continue
        issue_candidates.append(issue)

    ranked_items = _dedupe_and_rank_issue_candidates(issue_candidates, limit=max(limit * 4, limit))
    if owner_scope:
        state_by_fingerprint = fetch_action_queue_states(
            connection,
            owner_scope,
            [str(item.get("fingerprint") or "") for item in ranked_items],
        )
        ranked_items = filter_action_queue_items(ranked_items, state_by_fingerprint)
    return ranked_items[:limit]


def _row_value(row: sqlite3.Row | dict[str, Any] | object, key: str) -> Any:
    if isinstance(row, sqlite3.Row):
        try:
            return row[key]
        except (IndexError, KeyError):
            return None
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


def _normalized_string(value: object) -> str:
    return str(value or "").strip()


def _empty_verification_success_markers() -> VerificationSuccessMarkers:
    return VerificationSuccessMarkers(by_repo_host={}, by_label={}, by_file={})


def _normalize_action_queue_events(
    events: Sequence[sqlite3.Row | dict[str, Any] | object],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for event in events:
        normalized.append(
            {
                "session_id": _row_value(event, "session_id"),
                "event_index": _row_value(event, "event_index"),
                "timestamp": _row_value(event, "timestamp"),
                "record_type": _row_value(event, "record_type"),
                "payload_type": _row_value(event, "payload_type"),
                "kind": _row_value(event, "kind"),
                "role": _row_value(event, "role"),
                "title": _row_value(event, "title"),
                "display_text": _row_value(event, "display_text"),
                "detail_text": _row_value(event, "detail_text"),
                "tool_name": _row_value(event, "tool_name"),
                "call_id": _row_value(event, "call_id"),
                "command_text": _row_value(event, "command_text"),
                "exit_code": _row_value(event, "exit_code"),
                "record_json": _row_value(event, "record_json"),
            }
        )
    return normalized


def _fetch_action_queue_session_rows(
    connection: sqlite3.Connection,
    session_ids: list[str],
) -> dict[str, sqlite3.Row]:
    if not session_ids:
        return {}

    placeholders = ", ".join("?" for _ in session_ids)
    rows = connection.execute(
        f"""
        SELECT
            id,
            session_timestamp,
            started_at,
            imported_at,
            last_turn_timestamp,
            cwd,
            cwd_name,
            source_host,
            inferred_project_key,
            inferred_project_label
        FROM sessions
        WHERE id IN ({placeholders})
        ORDER BY id ASC
        """,
        session_ids,
    ).fetchall()
    return {str(row["id"]): row for row in rows}


def _fetch_action_queue_events(
    connection: sqlite3.Connection,
    session_ids: list[str],
) -> dict[str, list[sqlite3.Row]]:
    if not session_ids:
        return {}

    placeholders = ", ".join("?" for _ in session_ids)
    rows = connection.execute(
        f"""
        SELECT
            {EVENT_VIEW_COLUMNS}
        FROM events AS e
        WHERE session_id IN ({placeholders})
        ORDER BY session_id ASC, event_index ASC
        """,
        session_ids,
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["session_id"]), []).append(row)
    return grouped


def _delete_action_queue_rollups(connection: sqlite3.Connection, session_ids: list[str]) -> None:
    normalized_session_ids = [str(session_id or "").strip() for session_id in session_ids if str(session_id or "").strip()]
    if not normalized_session_ids:
        return
    placeholders = ", ".join("?" for _ in normalized_session_ids)
    connection.execute(
        f"DELETE FROM action_queue_signals WHERE session_id IN ({placeholders})",
        normalized_session_ids,
    )
    connection.execute(
        f"DELETE FROM action_queue_verification_successes WHERE session_id IN ({placeholders})",
        normalized_session_ids,
    )


def _materialization_project_fields(row: sqlite3.Row | dict[str, Any] | object) -> dict[str, str]:
    session_id = _normalized_string(_row_value(row, "id"))
    source_host = _normalized_string(_row_value(row, "source_host")) or "unknown-host"
    cwd = _normalized_string(_row_value(row, "cwd"))
    cwd_name = _normalized_string(_row_value(row, "cwd_name"))
    inferred_key = _normalized_string(_row_value(row, "inferred_project_key"))
    inferred_label = _normalized_string(_row_value(row, "inferred_project_label"))
    project_key = inferred_key or f"directory:{source_host}:{cwd or session_id}"
    project_label = inferred_label or cwd_name or cwd or "Session"
    return {
        "effective_group_key": project_key,
        "display_label": project_label,
        "source_host": source_host,
    }


def _serialize_issue_payload(issue: dict[str, object]) -> str:
    payload = {
        "status_tone": str(issue.get("status_tone") or ""),
        "status_label": str(issue.get("status_label") or ""),
        "title": str(issue.get("title") or ""),
        "status_title": str(issue.get("status_title") or ""),
        "next_action": str(issue.get("next_action") or ""),
        "signal_badges": issue.get("signal_badges") if isinstance(issue.get("signal_badges"), list) else [],
        "files": issue.get("files") if isinstance(issue.get("files"), list) else [],
        "verification_labels": issue.get("verification_labels")
        if isinstance(issue.get("verification_labels"), list)
        else [],
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _materialized_signal_inserts(
    session_id: str,
    issues: list[dict[str, object]],
) -> list[tuple[Any, ...]]:
    inserts: list[tuple[Any, ...]] = []
    for issue in issues:
        inserts.append(
            (
                session_id,
                int(issue.get("turn_number") or 0),
                str(issue.get("issue_kind") or ""),
                str(issue.get("signature") or ""),
                str(issue.get("timestamp") or ""),
                int(issue.get("severity") or 0),
                int(issue.get("noise_penalty") or 0),
                _serialize_issue_payload(issue),
            )
        )
    return inserts


def _materialized_verification_success_inserts(
    session_id: str,
    turns: list[dict[str, object]],
    row: sqlite3.Row | dict[str, Any] | object,
) -> list[tuple[Any, ...]]:
    inserts: list[tuple[Any, ...]] = []
    for turn in turns:
        verdict = turn.get("audit_verification_verdict", {})
        if str(verdict.get("status") or "") not in {"passed", "passed_with_warnings"}:
            continue
        timestamp_row = row if isinstance(row, (sqlite3.Row, dict)) else None
        timestamp = _issue_timestamp_value(turn, timestamp_row)
        if not timestamp:
            continue
        inserts.append(
            (
                session_id,
                int(turn.get("number") or 0),
                timestamp,
                json.dumps(_verification_labels(turn), ensure_ascii=False, sort_keys=True),
                json.dumps(_turn_files_touched(turn), ensure_ascii=False, sort_keys=True),
            )
        )
    return inserts


def _fetch_recent_materialized_signal_rows(
    connection: sqlite3.Connection,
    session_ids: list[str],
    *,
    turns_per_session: int,
) -> list[sqlite3.Row]:
    if not session_ids:
        return []

    placeholders = ", ".join("?" for _ in session_ids)
    rows = connection.execute(
        f"""
        WITH ranked_turns AS (
            SELECT
                session_id,
                turn_number,
                DENSE_RANK() OVER (
                    PARTITION BY session_id
                    ORDER BY turn_number DESC
                ) AS rn
            FROM (
                SELECT DISTINCT session_id, turn_number
                FROM action_queue_signals
                WHERE session_id IN ({placeholders})
            )
        )
        SELECT
            s.session_id,
            s.turn_number,
            s.issue_kind,
            s.signature,
            s.timestamp,
            s.severity,
            s.noise_penalty,
            s.payload_json
        FROM action_queue_signals AS s
        JOIN ranked_turns AS r
          ON r.session_id = s.session_id
         AND r.turn_number = s.turn_number
        WHERE r.rn <= ?
        ORDER BY s.timestamp DESC, s.session_id ASC, s.turn_number DESC, s.issue_kind ASC
        """,
        (*session_ids, max(int(turns_per_session or 1), 1)),
    ).fetchall()
    return list(rows)


def _fetch_materialized_verification_success_rows(
    connection: sqlite3.Connection,
    session_ids: list[str],
) -> list[sqlite3.Row]:
    if not session_ids:
        return []

    placeholders = ", ".join("?" for _ in session_ids)
    rows = connection.execute(
        f"""
        SELECT
            session_id,
            turn_number,
            timestamp,
            labels_json,
            files_json
        FROM action_queue_verification_successes
        WHERE session_id IN ({placeholders})
        ORDER BY timestamp DESC, session_id ASC, turn_number DESC
        """,
        session_ids,
    ).fetchall()
    return list(rows)


def _parse_json_object(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def _json_badges(value: object) -> list[dict[str, str]]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
    else:
        parsed = value
    if not isinstance(parsed, list):
        return []
    badges: list[dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "").strip()
        tone = str(item.get("tone") or "").strip()
        if not label:
            continue
        badges.append({"label": label, "tone": tone})
    return badges


def _hydrate_materialized_issue(
    *,
    signal_row: sqlite3.Row,
    row: sqlite3.Row,
    project: dict[str, object],
    project_href: str,
) -> dict[str, object]:
    payload = _parse_json_object(signal_row["payload_json"])
    session_id = str(row["id"])
    turn_number = int(signal_row["turn_number"] or 0)
    issue_kind = str(signal_row["issue_kind"] or "")
    signature = str(signal_row["signature"] or "")
    return {
        "issue_kind": issue_kind,
        "signature": signature,
        "fingerprint": action_queue_fingerprint(
            project_key=str(project["effective_group_key"] or ""),
            host=str(project["source_host"] or ""),
            issue_kind=issue_kind,
            signature=signature,
        ),
        "severity": int(signal_row["severity"] or 0),
        "noise_penalty": int(signal_row["noise_penalty"] or 0),
        "session_id": session_id,
        "turn_number": turn_number,
        "project_key": str(project["effective_group_key"] or ""),
        "project_label": str(project["display_label"] or "Session"),
        "project_href": project_href,
        "host": str(project["source_host"] or ""),
        "timestamp": str(signal_row["timestamp"] or ""),
        "status_tone": str(payload.get("status_tone") or ""),
        "status_label": str(payload.get("status_label") or ""),
        "title": str(payload.get("title") or ""),
        "status_title": str(payload.get("status_title") or ""),
        "next_action": str(payload.get("next_action") or ""),
        "signal_badges": _json_badges(payload.get("signal_badges")),
        "files": _json_string_list(payload.get("files")),
        "verification_labels": _json_string_list(payload.get("verification_labels")),
        "session_href": (
            f"/sessions/{quote(session_id, safe='')}?view=audit&turn={turn_number}&focus=1"
            if turn_number > 0
            else f"/sessions/{quote(session_id, safe='')}"
        ),
    }


def _collect_materialized_verification_successes(
    connection: sqlite3.Connection,
    rows: list[sqlite3.Row],
) -> VerificationSuccessMarkers:
    session_ids = [str(row["id"] or "").strip() for row in rows if str(row["id"] or "").strip()]
    if not session_ids:
        return _empty_verification_success_markers()

    row_by_session = {str(row["id"]): row for row in rows}
    by_repo_host: dict[tuple[str, str], datetime] = {}
    by_label: dict[tuple[str, str, str], datetime] = {}
    by_file: dict[tuple[str, str, str], datetime] = {}
    for success_row in _fetch_materialized_verification_success_rows(connection, session_ids):
        session_id = str(success_row["session_id"] or "").strip()
        session_row = row_by_session.get(session_id)
        if session_row is None:
            continue
        project = effective_project_fields(session_row)
        repo_key = str(project["effective_group_key"] or "")
        host = str(project["source_host"] or "")
        timestamp = parse_timestamp(str(success_row["timestamp"] or ""))
        if timestamp is None:
            continue
        repo_marker = (repo_key, host)
        previous_repo_success = by_repo_host.get(repo_marker)
        if previous_repo_success is None or timestamp > previous_repo_success:
            by_repo_host[repo_marker] = timestamp
        for label in _json_string_list(success_row["labels_json"]):
            marker = (repo_key, host, label)
            previous = by_label.get(marker)
            if previous is None or timestamp > previous:
                by_label[marker] = timestamp
        for path in _json_string_list(success_row["files_json"]):
            marker = (repo_key, host, path)
            previous = by_file.get(marker)
            if previous is None or timestamp > previous:
                by_file[marker] = timestamp
    return VerificationSuccessMarkers(
        by_repo_host=by_repo_host,
        by_label=by_label,
        by_file=by_file,
    )


def _extract_turn_issues(
    *,
    row: sqlite3.Row | dict[str, Any],
    project: dict[str, object],
    turns: list[dict[str, object]],
    verification_successes: VerificationSuccessMarkers,
    apply_verification_clears: bool = True,
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    repo_key = str(project["effective_group_key"] or "")
    host = str(project["source_host"] or "")

    for turn in turns:
        turn_items: list[dict[str, object]] = []
        issue_timestamp = _issue_timestamp(turn)

        verification_issue = _build_verification_failed_issue(row=row, project=project, turn=turn)
        if not apply_verification_clears or not _issue_cleared_by_later_verification(
            verification_issue,
            repo_key=repo_key,
            host=host,
            issue_timestamp=issue_timestamp,
            verification_successes=verification_successes,
        ):
            if verification_issue is not None:
                turn_items.append(verification_issue)

        claim_issue = _build_claim_mismatch_issue(row=row, project=project, turn=turn)
        if claim_issue is not None:
            turn_items.append(claim_issue)

        risky_issue = _build_risky_unverified_issue(row=row, project=project, turn=turn)
        if not apply_verification_clears or not _issue_cleared_by_later_verification(
            risky_issue,
            repo_key=repo_key,
            host=host,
            issue_timestamp=issue_timestamp,
            verification_successes=verification_successes,
        ):
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
        if not apply_verification_clears or not _issue_cleared_by_later_verification(
            aborted_issue,
            repo_key=repo_key,
            host=host,
            issue_timestamp=issue_timestamp,
            verification_successes=verification_successes,
        ):
            if aborted_issue is not None:
                turn_items.append(aborted_issue)

        items.extend(turn_items)

    return items


def _issue_cleared_by_later_verification(
    issue: dict[str, object] | None,
    *,
    repo_key: str,
    host: str,
    issue_timestamp: datetime | None,
    verification_successes: VerificationSuccessMarkers,
) -> bool:
    if issue is None or issue_timestamp is None:
        return False

    issue_kind = str(issue.get("issue_kind") or "").strip()
    latest_success: datetime | None = None

    if issue_kind == "verification_failed":
        for label in issue.get("verification_labels") or []:
            label_text = str(label or "").strip()
            if not label_text:
                continue
            latest_success = _latest_timestamp(
                latest_success,
                verification_successes.by_label.get((repo_key, host, label_text)),
            )
        for path in issue.get("files") or []:
            path_text = str(path or "").strip()
            if not path_text:
                continue
            latest_success = _latest_timestamp(
                latest_success,
                verification_successes.by_file.get((repo_key, host, path_text)),
            )
        if latest_success is None and not list(issue.get("verification_labels") or []):
            latest_success = verification_successes.by_repo_host.get((repo_key, host))
    elif issue_kind in {"risky_changes_unverified", "aborted_after_changes"}:
        latest_success = verification_successes.by_repo_host.get((repo_key, host))

    return latest_success is not None and latest_success > issue_timestamp


def _latest_timestamp(
    current: datetime | None,
    candidate: datetime | None,
) -> datetime | None:
    if candidate is None:
        return current
    if current is None or candidate > current:
        return candidate
    return current


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
    normalized = path.strip().lower()
    if normalized.startswith("./"):
        normalized = normalized[2:]
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
        "fingerprint": action_queue_fingerprint(
            project_key=str(project["effective_group_key"] or ""),
            host=str(project["source_host"] or ""),
            issue_kind=issue_kind,
            signature=signature,
        ),
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


def _issue_timestamp_value(turn: dict[str, object], row: sqlite3.Row | dict[str, Any] | None) -> str:
    for candidate in (
        turn.get("latest_event_timestamp"),
        turn.get("response_timestamp"),
        turn.get("prompt_timestamp"),
        _row_value(row, "last_turn_timestamp") if row is not None else None,
        _row_value(row, "session_timestamp") if row is not None else None,
        _row_value(row, "started_at") if row is not None else None,
        _row_value(row, "imported_at") if row is not None else None,
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
