from __future__ import annotations

import json
import logging
import signal
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .agents import fetch_remote_agent_health, fetch_remote_agent_status, remote_health_issues, trimmed
from .config import Settings
from .db import connect, write_transaction
from .server_settings import apply_server_settings


ALERT_RETRY_DELAYS_SECONDS = (30, 60, 300, 900, 1800)
DEFAULT_ALERT_WORKER_INTERVAL = 60


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _delivery_available(settings: Settings) -> bool:
    return bool(
        settings.alerts_enabled
        and settings.alerts_provider == "webhook"
        and settings.alerts_webhook_url
    )


def _alert_key(source_host: str, issue_kind: str) -> str:
    return f"{source_host}:{issue_kind}"


def _issue_payload(issue: dict[str, object]) -> str:
    return json.dumps(issue, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _issue_title(issue: dict[str, object]) -> str:
    return str(issue.get("label") or str(issue.get("kind") or "Alert"))


def _issue_detail(issue: dict[str, object]) -> str:
    return str(issue.get("detail") or "")


def _issue_severity(issue: dict[str, object]) -> str:
    return str(issue.get("severity") or "warning")


def _issue_fingerprint(issue: dict[str, object]) -> str:
    return str(issue.get("fingerprint") or "")


def _notification_cooldown_elapsed(
    last_notified_at: str | None,
    cooldown_minutes: int,
    *,
    now: datetime,
) -> bool:
    if not last_notified_at:
        return True
    last_seen = _parse_iso(last_notified_at)
    if last_seen is None:
        return True
    return now - last_seen >= timedelta(minutes=max(1, cooldown_minutes))


def _build_delivery_payload(
    settings: Settings,
    incident: dict[str, object],
    *,
    notification_kind: str,
) -> dict[str, object]:
    source_host = str(incident["source_host"])
    issue_kind = str(incident["issue_kind"])
    audit_path = f"/remotes/{quote(source_host, safe='')}/audit"
    audit_url = None
    if settings.server_base_url:
        audit_url = f"{settings.server_base_url.rstrip('/')}{audit_path}"
    title = str(incident["title"])
    detail = str(incident["detail"])
    severity = str(incident["severity"])
    status = str(incident["status"])
    text = f"[{severity}] {source_host} {title}: {detail}".strip()
    return {
        "source": "codex_session_viewer",
        "notification_kind": notification_kind,
        "status": status,
        "alert_key": str(incident["alert_key"]),
        "source_host": source_host,
        "issue_kind": issue_kind,
        "severity": severity,
        "title": title,
        "detail": detail,
        "text": text,
        "audit_path": audit_path,
        "audit_url": audit_url,
        "opened_at": incident["opened_at"],
        "last_seen_at": incident["last_seen_at"],
        "resolved_at": incident["resolved_at"],
        "detail_json": json.loads(str(incident["detail_json"] or "{}")),
    }


def _queue_alert_delivery(
    connection: sqlite3.Connection,
    settings: Settings,
    incident: dict[str, object],
    *,
    notification_kind: str,
    now: str,
) -> bool:
    if not _delivery_available(settings):
        return False

    payload = _build_delivery_payload(
        settings,
        incident,
        notification_kind=notification_kind,
    )
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    existing = connection.execute(
        """
        SELECT id
        FROM alert_deliveries
        WHERE alert_key = ? AND provider = ? AND status IN ('pending', 'sending')
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (incident["alert_key"], settings.alerts_provider),
    ).fetchone()
    if existing is None:
        connection.execute(
            """
            INSERT INTO alert_deliveries (
                alert_key,
                source_host,
                issue_kind,
                notification_kind,
                provider,
                payload_json,
                status,
                attempt_count,
                next_attempt_at,
                created_at,
                claimed_at,
                sent_at,
                last_error
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, NULL, NULL, NULL)
            """,
            (
                incident["alert_key"],
                incident["source_host"],
                incident["issue_kind"],
                notification_kind,
                settings.alerts_provider,
                payload_json,
                now,
                now,
            ),
        )
    else:
        connection.execute(
            """
            UPDATE alert_deliveries
            SET
                source_host = ?,
                issue_kind = ?,
                notification_kind = ?,
                payload_json = ?,
                status = 'pending',
                attempt_count = 0,
                next_attempt_at = ?,
                claimed_at = NULL,
                sent_at = NULL,
                last_error = NULL
            WHERE id = ?
            """,
            (
                incident["source_host"],
                incident["issue_kind"],
                notification_kind,
                payload_json,
                now,
                int(existing["id"]),
            ),
        )

    connection.execute(
        """
        UPDATE alert_incidents
        SET
            last_notified_at = ?,
            last_notification_kind = ?,
            last_notification_fingerprint = ?,
            updated_at = ?
        WHERE alert_key = ?
        """,
        (
            now,
            notification_kind,
            incident["fingerprint"],
            now,
            incident["alert_key"],
        ),
    )
    return True


def _incident_from_row(row: sqlite3.Row) -> dict[str, object]:
    return {
        "alert_key": str(row["alert_key"]),
        "source_host": str(row["source_host"]),
        "issue_kind": str(row["issue_kind"]),
        "status": str(row["status"]),
        "severity": str(row["severity"]),
        "title": str(row["title"]),
        "detail": str(row["detail"]),
        "fingerprint": str(row["fingerprint"]),
        "opened_at": str(row["opened_at"]),
        "last_seen_at": str(row["last_seen_at"]),
        "resolved_at": trimmed(row["resolved_at"]),
        "last_notified_at": trimmed(row["last_notified_at"]),
        "last_notification_kind": trimmed(row["last_notification_kind"]),
        "last_notification_fingerprint": trimmed(row["last_notification_fingerprint"]),
        "detail_json": str(row["detail_json"] or "{}"),
        "updated_at": str(row["updated_at"]),
    }


def _active_alertable_issues(remote: dict[str, Any]) -> dict[str, dict[str, object]]:
    return {
        str(issue["kind"]): issue
        for issue in remote_health_issues(remote)
        if bool(issue.get("alertable"))
    }


def reconcile_remote_alerts(
    connection: sqlite3.Connection,
    settings: Settings,
    remote: dict[str, Any],
) -> dict[str, int]:
    now = utc_now_iso()
    now_dt = _parse_iso(now) or datetime.now(tz=UTC)
    source_host = str(remote.get("source_host") or "").strip()
    if not source_host:
        return {"opened": 0, "updated": 0, "resolved": 0, "queued": 0}

    seen_at = trimmed(remote.get("last_seen_at")) or now
    active_issues = _active_alertable_issues(remote)
    existing_rows = connection.execute(
        """
        SELECT *
        FROM alert_incidents
        WHERE source_host = ?
        """,
        (source_host,),
    ).fetchall()
    existing = {str(row["issue_kind"]): row for row in existing_rows}
    stats = {"opened": 0, "updated": 0, "resolved": 0, "queued": 0}

    for issue_kind, issue in active_issues.items():
        alert_key = _alert_key(source_host, issue_kind)
        title = _issue_title(issue)
        detail = _issue_detail(issue)
        severity = _issue_severity(issue)
        fingerprint = _issue_fingerprint(issue)
        detail_json = _issue_payload(issue)
        previous = existing.get(issue_kind)
        notification_kind: str | None = None

        if previous is None:
            connection.execute(
                """
                INSERT INTO alert_incidents (
                    alert_key,
                    source_host,
                    issue_kind,
                    status,
                    severity,
                    title,
                    detail,
                    fingerprint,
                    opened_at,
                    last_seen_at,
                    resolved_at,
                    last_notified_at,
                    last_notification_kind,
                    last_notification_fingerprint,
                    detail_json,
                    updated_at
                ) VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?)
                """,
                (
                    alert_key,
                    source_host,
                    issue_kind,
                    severity,
                    title,
                    detail,
                    fingerprint,
                    seen_at,
                    seen_at,
                    detail_json,
                    now,
                ),
            )
            stats["opened"] += 1
            notification_kind = "open"
            incident = {
                "alert_key": alert_key,
                "source_host": source_host,
                "issue_kind": issue_kind,
                "status": "open",
                "severity": severity,
                "title": title,
                "detail": detail,
                "fingerprint": fingerprint,
                "opened_at": seen_at,
                "last_seen_at": seen_at,
                "resolved_at": None,
                "detail_json": detail_json,
            }
        else:
            previous_status = str(previous["status"])
            last_notified_at = trimmed(previous["last_notified_at"])
            last_notification_fingerprint = trimmed(previous["last_notification_fingerprint"]) or ""
            updates = (
                severity != str(previous["severity"])
                or title != str(previous["title"])
                or detail != str(previous["detail"])
                or fingerprint != str(previous["fingerprint"])
                or detail_json != str(previous["detail_json"])
            )
            if previous_status != "open":
                if not _delivery_available(settings):
                    last_notified_at = None
                    last_notification_fingerprint = ""
                connection.execute(
                    """
                    UPDATE alert_incidents
                    SET
                        status = 'open',
                        severity = ?,
                        title = ?,
                        detail = ?,
                        fingerprint = ?,
                        opened_at = ?,
                        last_seen_at = ?,
                        resolved_at = NULL,
                        last_notified_at = ?,
                        last_notification_kind = NULL,
                        last_notification_fingerprint = ?,
                        detail_json = ?,
                        updated_at = ?
                    WHERE alert_key = ?
                    """,
                    (
                        severity,
                        title,
                        detail,
                        fingerprint,
                        seen_at,
                        seen_at,
                        last_notified_at,
                        last_notification_fingerprint or None,
                        detail_json,
                        now,
                        alert_key,
                    ),
                )
                stats["opened"] += 1
                notification_kind = "open"
                incident = {
                    "alert_key": alert_key,
                    "source_host": source_host,
                    "issue_kind": issue_kind,
                    "status": "open",
                    "severity": severity,
                    "title": title,
                    "detail": detail,
                    "fingerprint": fingerprint,
                    "opened_at": seen_at,
                    "last_seen_at": seen_at,
                    "resolved_at": None,
                    "detail_json": detail_json,
                }
            else:
                connection.execute(
                    """
                    UPDATE alert_incidents
                    SET
                        severity = ?,
                        title = ?,
                        detail = ?,
                        fingerprint = ?,
                        last_seen_at = ?,
                        detail_json = ?,
                        updated_at = ?
                    WHERE alert_key = ?
                    """,
                    (
                        severity,
                        title,
                        detail,
                        fingerprint,
                        seen_at,
                        detail_json,
                        now,
                        alert_key,
                    ),
                )
                if updates:
                    stats["updated"] += 1
                incident = {
                    "alert_key": alert_key,
                    "source_host": source_host,
                    "issue_kind": issue_kind,
                    "status": "open",
                    "severity": severity,
                    "title": title,
                    "detail": detail,
                    "fingerprint": fingerprint,
                    "opened_at": str(previous["opened_at"]),
                    "last_seen_at": seen_at,
                    "resolved_at": None,
                    "detail_json": detail_json,
                }
                if not last_notified_at:
                    notification_kind = "open"
                elif last_notification_fingerprint != fingerprint:
                    notification_kind = "update"
                elif _notification_cooldown_elapsed(
                    last_notified_at,
                    settings.alerts_realert_minutes,
                    now=now_dt,
                ):
                    notification_kind = "realert"

        if notification_kind and _queue_alert_delivery(
            connection,
            settings,
            incident,
            notification_kind=notification_kind,
            now=now,
        ):
            stats["queued"] += 1

    for issue_kind, previous in existing.items():
        if issue_kind in active_issues or str(previous["status"]) != "open":
            continue
        connection.execute(
            """
            UPDATE alert_incidents
            SET
                status = 'resolved',
                resolved_at = ?,
                updated_at = ?
            WHERE alert_key = ?
            """,
            (
                now,
                now,
                str(previous["alert_key"]),
            ),
        )
        stats["resolved"] += 1
        if settings.alerts_send_resolutions and trimmed(previous["last_notified_at"]):
            incident = _incident_from_row(previous)
            incident["status"] = "resolved"
            incident["resolved_at"] = now
            if _queue_alert_delivery(
                connection,
                settings,
                incident,
                notification_kind="resolved",
                now=now,
            ):
                stats["queued"] += 1

    return stats


def reconcile_remote_alerts_for_host(
    connection: sqlite3.Connection,
    settings: Settings,
    source_host: str,
) -> dict[str, int]:
    remote = fetch_remote_agent_status(connection, settings, source_host)
    if remote is None:
        return {"opened": 0, "updated": 0, "resolved": 0, "queued": 0}
    return reconcile_remote_alerts(connection, settings, remote)


def reconcile_all_remote_alerts(
    connection: sqlite3.Connection,
    settings: Settings,
) -> dict[str, int]:
    totals = {"opened": 0, "updated": 0, "resolved": 0, "queued": 0}
    for remote in fetch_remote_agent_health(connection, settings):
        stats = reconcile_remote_alerts(connection, settings, remote)
        for key, value in stats.items():
            totals[key] += value
    return totals


def _stale_delivery_reset_before(now: str) -> str:
    current = _parse_iso(now) or datetime.now(tz=UTC)
    return (current - timedelta(minutes=15)).replace(microsecond=0).isoformat()


def claim_due_alert_deliveries(
    connection: sqlite3.Connection,
    *,
    limit: int = 20,
) -> list[dict[str, object]]:
    now = utc_now_iso()
    connection.execute(
        """
        UPDATE alert_deliveries
        SET
            status = 'pending',
            claimed_at = NULL
        WHERE status = 'sending' AND claimed_at IS NOT NULL AND claimed_at <= ?
        """,
        (_stale_delivery_reset_before(now),),
    )
    rows = connection.execute(
        """
        SELECT *
        FROM alert_deliveries
        WHERE status = 'pending' AND next_attempt_at <= ?
        ORDER BY next_attempt_at ASC, created_at ASC
        LIMIT ?
        """,
        (now, limit),
    ).fetchall()
    deliveries: list[dict[str, object]] = []
    for row in rows:
        delivery_id = int(row["id"])
        updated = connection.execute(
            """
            UPDATE alert_deliveries
            SET
                status = 'sending',
                claimed_at = ?
            WHERE id = ? AND status = 'pending'
            """,
            (now, delivery_id),
        )
        if updated.rowcount != 1:
            continue
        deliveries.append(
            {
                "id": delivery_id,
                "alert_key": str(row["alert_key"]),
                "source_host": str(row["source_host"]),
                "issue_kind": str(row["issue_kind"]),
                "notification_kind": str(row["notification_kind"]),
                "provider": str(row["provider"]),
                "payload_json": str(row["payload_json"]),
                "attempt_count": int(row["attempt_count"] or 0),
                "created_at": str(row["created_at"]),
            }
        )
    return deliveries


def _next_retry_at(attempt_count: int, *, now: str) -> str:
    current = _parse_iso(now) or datetime.now(tz=UTC)
    index = min(max(attempt_count, 0), len(ALERT_RETRY_DELAYS_SECONDS) - 1)
    retry_after = ALERT_RETRY_DELAYS_SECONDS[index]
    return (current + timedelta(seconds=retry_after)).replace(microsecond=0).isoformat()


def _send_webhook(settings: Settings, payload: dict[str, object]) -> None:
    if not settings.alerts_webhook_url:
        raise RuntimeError("Webhook delivery is not configured.")
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        settings.alerts_webhook_url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": f"codex-session-viewer/{settings.app_version}",
        },
    )
    try:
        with urlopen(request, timeout=settings.remote_timeout_seconds) as response:
            response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"Webhook delivery failed: {exc.code} {detail}".strip()) from exc
    except URLError as exc:
        raise RuntimeError(f"Webhook delivery failed: {exc.reason}") from exc


def mark_alert_delivery_sent(
    connection: sqlite3.Connection,
    delivery_id: int,
) -> None:
    now = utc_now_iso()
    connection.execute(
        """
        UPDATE alert_deliveries
        SET
            status = 'sent',
            attempt_count = attempt_count + 1,
            claimed_at = NULL,
            sent_at = ?,
            last_error = NULL
        WHERE id = ?
        """,
        (now, delivery_id),
    )


def mark_alert_delivery_failed(
    connection: sqlite3.Connection,
    delivery_id: int,
    *,
    attempt_count: int,
    error: str,
) -> None:
    now = utc_now_iso()
    connection.execute(
        """
        UPDATE alert_deliveries
        SET
            status = 'pending',
            attempt_count = attempt_count + 1,
            next_attempt_at = ?,
            claimed_at = NULL,
            last_error = ?
        WHERE id = ?
        """,
        (
            _next_retry_at(attempt_count, now=now),
            error,
            delivery_id,
        ),
    )


def deliver_pending_alerts(
    settings: Settings,
    *,
    limit: int = 20,
) -> dict[str, int]:
    if not _delivery_available(settings):
        return {"claimed": 0, "sent": 0, "failed": 0}

    with connect(settings.database_path) as connection:
        with write_transaction(connection):
            deliveries = claim_due_alert_deliveries(connection, limit=limit)

    stats = {"claimed": len(deliveries), "sent": 0, "failed": 0}
    if not deliveries:
        return stats

    for delivery in deliveries:
        payload = json.loads(str(delivery["payload_json"]))
        try:
            if str(delivery["provider"]) != "webhook":
                raise RuntimeError(f"Unsupported alert provider: {delivery['provider']}")
            _send_webhook(settings, payload)
        except Exception as exc:
            stats["failed"] += 1
            with connect(settings.database_path) as connection:
                with write_transaction(connection):
                    mark_alert_delivery_failed(
                        connection,
                        int(delivery["id"]),
                        attempt_count=int(delivery["attempt_count"]),
                        error=str(exc),
                    )
            continue
        with connect(settings.database_path) as connection:
            with write_transaction(connection):
                mark_alert_delivery_sent(connection, int(delivery["id"]))
        stats["sent"] += 1
    return stats


def run_alert_worker(
    settings: Settings,
    *,
    interval_seconds: int = DEFAULT_ALERT_WORKER_INTERVAL,
    once: bool = False,
) -> int:
    logger = logging.getLogger("codex_session_viewer.alerts")
    stop_event = threading.Event()
    interval_seconds = max(5, interval_seconds)

    def _request_shutdown(signum: int, _frame: object) -> None:
        logger.info("Received signal %s, shutting down alert worker", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    logger.info("Starting alert worker with interval=%ss", interval_seconds)
    while not stop_event.is_set():
        reconcile_stats = {"opened": 0, "updated": 0, "resolved": 0, "queued": 0}
        delivery_stats = {"claimed": 0, "sent": 0, "failed": 0}
        try:
            with connect(settings.database_path) as connection:
                apply_server_settings(connection, settings)
            with connect(settings.database_path) as connection:
                with write_transaction(connection):
                    reconcile_stats = reconcile_all_remote_alerts(connection, settings)
            delivery_stats = deliver_pending_alerts(settings)
            if settings.alerts_enabled and settings.alerts_provider == "webhook" and not settings.alerts_webhook_url:
                logger.warning("Alerts are enabled, but no webhook URL is configured.")
            logger.info(
                "Alert pass finished: %s",
                json.dumps(
                    {
                        "reconciled": reconcile_stats,
                        "deliveries": delivery_stats,
                    },
                    sort_keys=True,
                ),
            )
        except Exception:
            logger.exception("Alert worker pass crashed unexpectedly")
        if once:
            break
        if stop_event.wait(interval_seconds):
            break

    logger.info("Alert worker stopped")
    return 0
