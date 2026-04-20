from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import sqlite3
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from .config import Settings


MIN_PAGE_SIZE = 6
MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 24


@dataclass(slots=True)
class ServerSettingsSnapshot:
    page_size: int
    expected_agent_version: str
    sync_on_start: bool
    alerts_enabled: bool
    alerts_provider: str
    alerts_webhook_url: str | None
    alerts_realert_minutes: int
    alerts_send_resolutions: bool


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def parse_bool_value(raw: object, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off", ""}:
        return False
    return default


def normalize_page_size(raw: object) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("Home page repo limit must be a whole number.") from exc
    if value < MIN_PAGE_SIZE or value > MAX_PAGE_SIZE:
        raise ValueError(
            f"Home page repo limit must be between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}."
        )
    return value


def normalize_expected_agent_version(raw: object, default_version: str) -> str:
    value = str(raw or "").strip() or default_version
    if len(value) > 64:
        raise ValueError("Expected agent version must be 64 characters or fewer.")
    return value


def normalize_alert_provider(raw: object) -> str:
    value = str(raw or "").strip().lower() or "webhook"
    if value != "webhook":
        raise ValueError("Alert provider must currently be webhook.")
    return value


def normalize_alert_webhook_url(
    raw: object,
    *,
    alerts_enabled: bool,
    alerts_provider: str,
) -> str | None:
    value = str(raw or "").strip().rstrip("/")
    if not value:
        if alerts_enabled and alerts_provider == "webhook":
            raise ValueError("Webhook alerts require a webhook URL.")
        return None
    if len(value) > 2048:
        raise ValueError("Alert webhook URL must be 2048 characters or fewer.")
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Alert webhook URL must be a valid http or https URL.")
    return value


def normalize_alert_realert_minutes(raw: object) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("Alert re-alert window must be a whole number of minutes.") from exc
    if value < 1 or value > 10_080:
        raise ValueError("Alert re-alert window must be between 1 and 10080 minutes.")
    return value


def fetch_server_settings(
    connection: sqlite3.Connection,
    defaults: "Settings",
) -> ServerSettingsSnapshot:
    rows = connection.execute(
        """
        SELECT key, value
        FROM server_settings
        WHERE key IN (
            'page_size',
            'expected_agent_version',
            'sync_on_start',
            'alerts_enabled',
            'alerts_provider',
            'alerts_webhook_url',
            'alerts_realert_minutes',
            'alerts_send_resolutions'
        )
        """
    ).fetchall()
    values = {str(row["key"]): str(row["value"]) for row in rows}

    page_size = defaults.page_size
    if "page_size" in values:
        try:
            page_size = normalize_page_size(values["page_size"])
        except ValueError:
            page_size = defaults.page_size

    expected_agent_version = defaults.expected_agent_version
    if "expected_agent_version" in values:
        try:
            expected_agent_version = normalize_expected_agent_version(
                values["expected_agent_version"],
                defaults.app_version,
            )
        except ValueError:
            expected_agent_version = defaults.expected_agent_version

    sync_on_start = defaults.sync_on_start
    if "sync_on_start" in values:
        sync_on_start = parse_bool_value(values["sync_on_start"], defaults.sync_on_start)

    alerts_enabled = defaults.alerts_enabled
    if "alerts_enabled" in values:
        alerts_enabled = parse_bool_value(values["alerts_enabled"], defaults.alerts_enabled)

    alerts_provider = defaults.alerts_provider
    if "alerts_provider" in values:
        try:
            alerts_provider = normalize_alert_provider(values["alerts_provider"])
        except ValueError:
            alerts_provider = defaults.alerts_provider

    alerts_realert_minutes = defaults.alerts_realert_minutes
    if "alerts_realert_minutes" in values:
        try:
            alerts_realert_minutes = normalize_alert_realert_minutes(values["alerts_realert_minutes"])
        except ValueError:
            alerts_realert_minutes = defaults.alerts_realert_minutes

    alerts_send_resolutions = defaults.alerts_send_resolutions
    if "alerts_send_resolutions" in values:
        alerts_send_resolutions = parse_bool_value(
            values["alerts_send_resolutions"],
            defaults.alerts_send_resolutions,
        )

    alerts_webhook_url = defaults.alerts_webhook_url
    if "alerts_webhook_url" in values:
        try:
            alerts_webhook_url = normalize_alert_webhook_url(
                values["alerts_webhook_url"],
                alerts_enabled=alerts_enabled,
                alerts_provider=alerts_provider,
            )
        except ValueError:
            alerts_webhook_url = defaults.alerts_webhook_url

    return ServerSettingsSnapshot(
        page_size=page_size,
        expected_agent_version=expected_agent_version,
        sync_on_start=sync_on_start,
        alerts_enabled=alerts_enabled,
        alerts_provider=alerts_provider,
        alerts_webhook_url=alerts_webhook_url,
        alerts_realert_minutes=alerts_realert_minutes,
        alerts_send_resolutions=alerts_send_resolutions,
    )


def apply_server_settings(
    connection: sqlite3.Connection,
    settings: "Settings",
    *,
    preserve_sync_on_start: bool = False,
) -> ServerSettingsSnapshot:
    snapshot = fetch_server_settings(connection, settings)
    settings.page_size = snapshot.page_size
    settings.expected_agent_version = snapshot.expected_agent_version
    if not preserve_sync_on_start:
        settings.sync_on_start = snapshot.sync_on_start
    settings.alerts_enabled = snapshot.alerts_enabled
    settings.alerts_provider = snapshot.alerts_provider
    settings.alerts_webhook_url = snapshot.alerts_webhook_url
    settings.alerts_realert_minutes = snapshot.alerts_realert_minutes
    settings.alerts_send_resolutions = snapshot.alerts_send_resolutions
    return snapshot


def update_server_settings(
    connection: sqlite3.Connection,
    *,
    page_size: int,
    expected_agent_version: str,
    sync_on_start: bool,
    alerts_enabled: bool,
    alerts_provider: str,
    alerts_webhook_url: str | None,
    alerts_realert_minutes: int,
    alerts_send_resolutions: bool,
) -> ServerSettingsSnapshot:
    now = utc_now_iso()
    values = {
        "page_size": str(page_size),
        "expected_agent_version": expected_agent_version,
        "sync_on_start": "1" if sync_on_start else "0",
        "alerts_enabled": "1" if alerts_enabled else "0",
        "alerts_provider": alerts_provider,
        "alerts_webhook_url": alerts_webhook_url or "",
        "alerts_realert_minutes": str(alerts_realert_minutes),
        "alerts_send_resolutions": "1" if alerts_send_resolutions else "0",
    }
    for key, value in values.items():
        connection.execute(
            """
            INSERT INTO server_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, now),
        )
    return ServerSettingsSnapshot(
        page_size=page_size,
        expected_agent_version=expected_agent_version,
        sync_on_start=sync_on_start,
        alerts_enabled=alerts_enabled,
        alerts_provider=alerts_provider,
        alerts_webhook_url=alerts_webhook_url,
        alerts_realert_minutes=alerts_realert_minutes,
        alerts_send_resolutions=alerts_send_resolutions,
    )
