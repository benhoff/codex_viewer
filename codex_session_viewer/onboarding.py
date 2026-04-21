from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from .agents import fetch_remote_agent_health
from .api_tokens import active_api_token_count, list_api_tokens
from .config import Settings
from .local_auth import AuthStatus, fetch_auth_status


FAILED_UPDATE_STATES = {"update_failed"}
DEGRADED_UPDATE_STATES = {"protocol_mismatch", "manual_update_required", "updated_restart_required"}


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def trimmed(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def effective_bootstrap_required(settings: Settings, auth_status: AuthStatus) -> bool:
    return settings.auth_enabled() and auth_status.admin_user is None


def ensure_onboarding_state_row(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        "SELECT 1 FROM onboarding_state WHERE singleton = 1"
    ).fetchone()
    if row is None:
        now = utc_now_iso()
        connection.execute(
            """
            INSERT INTO onboarding_state (
                singleton,
                completed_at,
                first_heartbeat_at,
                first_heartbeat_source_host,
                first_session_ingested_at,
                first_session_source_host,
                last_failure_reason,
                created_at,
                updated_at
            ) VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)
            """,
            (now, now),
        )


def fetch_onboarding_state(connection: sqlite3.Connection) -> dict[str, Any]:
    ensure_onboarding_state_row(connection)
    row = connection.execute(
        """
        SELECT
            completed_at,
            first_heartbeat_at,
            first_heartbeat_source_host,
            first_session_ingested_at,
            first_session_source_host,
            last_failure_reason,
            created_at,
            updated_at
        FROM onboarding_state
        WHERE singleton = 1
        """
    ).fetchone()
    return {
        "completed_at": trimmed(row["completed_at"]) if row else None,
        "first_heartbeat_at": trimmed(row["first_heartbeat_at"]) if row else None,
        "first_heartbeat_source_host": trimmed(row["first_heartbeat_source_host"]) if row else None,
        "first_session_ingested_at": trimmed(row["first_session_ingested_at"]) if row else None,
        "first_session_source_host": trimmed(row["first_session_source_host"]) if row else None,
        "last_failure_reason": trimmed(row["last_failure_reason"]) if row else None,
        "created_at": trimmed(row["created_at"]) if row else None,
        "updated_at": trimmed(row["updated_at"]) if row else None,
    }


def _update_onboarding_state(connection: sqlite3.Connection, **fields: str | None) -> None:
    if not fields:
        return
    assignments = ", ".join(f"{key} = ?" for key in fields)
    params = [value for value in fields.values()]
    params.extend([utc_now_iso(), 1])
    connection.execute(
        f"""
        UPDATE onboarding_state
        SET {assignments},
            updated_at = ?
        WHERE singleton = ?
        """,
        params,
    )


def record_first_heartbeat(
    connection: sqlite3.Connection,
    *,
    source_host: str | None,
    seen_at: str | None,
) -> None:
    state = fetch_onboarding_state(connection)
    if state["first_heartbeat_at"]:
        return
    timestamp = trimmed(seen_at) or utc_now_iso()
    _update_onboarding_state(
        connection,
        first_heartbeat_at=timestamp,
        first_heartbeat_source_host=trimmed(source_host),
    )


def record_first_session_ingested(
    connection: sqlite3.Connection,
    *,
    source_host: str | None,
    imported_at: str | None,
) -> None:
    state = fetch_onboarding_state(connection)
    if state["first_session_ingested_at"]:
        return
    timestamp = trimmed(imported_at) or utc_now_iso()
    _update_onboarding_state(
        connection,
        first_session_ingested_at=timestamp,
        first_session_source_host=trimmed(source_host),
    )


def _backfill_onboarding_timestamps(connection: sqlite3.Connection) -> None:
    state = fetch_onboarding_state(connection)
    if not state["first_heartbeat_at"]:
        heartbeat_row = connection.execute(
            """
            SELECT source_host, last_seen_at
            FROM remote_agents
            WHERE COALESCE(last_seen_at, '') <> ''
            ORDER BY last_seen_at ASC
            LIMIT 1
            """
        ).fetchone()
        if heartbeat_row is not None:
            _update_onboarding_state(
                connection,
                first_heartbeat_at=trimmed(heartbeat_row["last_seen_at"]),
                first_heartbeat_source_host=trimmed(heartbeat_row["source_host"]),
            )
            state = fetch_onboarding_state(connection)

    if not state["first_session_ingested_at"]:
        session_row = _fetch_first_session_row(connection, remote_only=True)
        if session_row is not None:
            _update_onboarding_state(
                connection,
                first_session_ingested_at=trimmed(session_row["first_timestamp"]),
                first_session_source_host=trimmed(session_row["source_host"]),
            )


def _fetch_first_session_row(
    connection: sqlite3.Connection,
    *,
    remote_only: bool,
) -> sqlite3.Row | None:
    timestamp_expr = "COALESCE(NULLIF(s.imported_at, ''), NULLIF(s.started_at, ''), NULLIF(s.session_timestamp, ''))"
    remote_filter = (
        """
          AND EXISTS (
            SELECT 1
            FROM remote_agents AS r
            WHERE r.source_host = s.source_host
          )
        """
        if remote_only
        else ""
    )
    return connection.execute(
        f"""
        SELECT
            s.source_host,
            MIN({timestamp_expr}) AS first_timestamp
        FROM sessions AS s
        WHERE {timestamp_expr} IS NOT NULL
          {remote_filter}
        GROUP BY s.source_host
        ORDER BY first_timestamp ASC
        LIMIT 1
        """
    ).fetchone()


def _count_imported_sessions(
    connection: sqlite3.Connection,
    *,
    remote_only: bool,
) -> int:
    remote_filter = (
        """
        WHERE EXISTS (
            SELECT 1
            FROM remote_agents AS r
            WHERE r.source_host = s.source_host
        )
        """
        if remote_only
        else ""
    )
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS session_count
        FROM sessions AS s
        {remote_filter}
        """
    ).fetchone()
    return int(row["session_count"] or 0) if row is not None else 0


def _check_state(
    *,
    label: str,
    state: str,
    detail: str,
) -> dict[str, str]:
    return {
        "label": label,
        "state": state,
        "detail": detail,
    }


def _status_label(status: str) -> str:
    return {
        "not_started": "Not started",
        "in_progress": "In progress",
        "complete": "Complete",
        "blocked": "Blocked",
    }.get(status, status.replace("_", " ").title())


def reconcile_onboarding_state(connection: sqlite3.Connection, settings: Settings) -> dict[str, Any]:
    ensure_onboarding_state_row(connection)
    _backfill_onboarding_timestamps(connection)

    local_mode = settings.sync_mode == "local"
    auth_status = fetch_auth_status(connection)
    bootstrap_required = effective_bootstrap_required(settings, auth_status)
    token_count = active_api_token_count(connection)
    token_rows = [token for token in list_api_tokens(connection) if not token.get("revoked_at")]
    latest_token = token_rows[0] if token_rows else None
    remotes = fetch_remote_agent_health(connection, settings)
    primary_remote = remotes[0] if remotes else None
    imported_session_count = _count_imported_sessions(connection, remote_only=not local_mode)
    session_root_labels = [str(root) for root in settings.session_roots]
    existing_session_root_count = sum(1 for root in settings.session_roots if root.exists())
    state = fetch_onboarding_state(connection)
    if local_mode and not state["first_session_ingested_at"]:
        first_local_session = _fetch_first_session_row(connection, remote_only=False)
        if first_local_session is not None:
            _update_onboarding_state(
                connection,
                first_session_ingested_at=trimmed(first_local_session["first_timestamp"]),
                first_session_source_host=trimmed(first_local_session["source_host"]),
            )
            state = fetch_onboarding_state(connection)

    auth_ready = (not settings.auth_enabled()) or bool(settings.session_secret)
    if local_mode:
        configured = auth_ready and not bootstrap_required
        agent_seen = False
        compatible = False
        sync_healthy = False
        connected_but_empty = False
        data_verified = bool(state["first_session_ingested_at"]) or imported_session_count > 0
        overall_tone = "amber"
        overall_state = "in_progress"
        health_classification = "pending"
        reason = ""
        next_action = ""

        if not auth_ready:
            overall_tone = "rose"
            overall_state = "blocked"
            health_classification = "failed"
            reason = "Browser authentication could not initialize a working session secret."
            next_action = "Review the server auth configuration and restart the viewer."
        elif bootstrap_required:
            overall_tone = "amber"
            overall_state = "in_progress"
            health_classification = "pending"
            if settings.auth_allows_password():
                reason = "Create the first admin account to finish local setup."
                next_action = "Finish the Create Admin step below."
            else:
                reason = "Sign in through the trusted proxy and claim the first admin account."
                next_action = "Use the Create Admin step below after your SSO session is visible here."
        elif data_verified:
            overall_tone = "emerald"
            overall_state = "complete"
            health_classification = "healthy"
            reason = "Local session import is working and the viewer has data to show."
            next_action = "Opening the dashboard."
        else:
            overall_tone = "amber"
            overall_state = "in_progress"
            health_classification = "configured"
            if existing_session_root_count == 0 and session_root_labels:
                reason = "No local sessions have been imported yet because the configured session root was not found."
                next_action = f"Set CODEX_SESSION_ROOTS to a valid directory or create a Codex session under {session_root_labels[0]}."
            else:
                reason = "No local sessions have been imported yet."
                next_action = "Run Codex once on this machine or trigger a manual sync after pointing CODEX_SESSION_ROOTS at your session directory."
    else:
        configured = auth_ready and not bootstrap_required and token_count > 0
        agent_seen = bool(primary_remote and not primary_remote.get("stale"))
        compatible = bool(primary_remote and not primary_remote.get("api_mismatch") and not primary_remote.get("version_mismatch"))
        update_state = str(primary_remote.get("update_state") or "").strip() if primary_remote else ""
        has_sync_error = bool(
            primary_remote
            and (
                int(primary_remote.get("last_fail_count") or 0) > 0
                or primary_remote.get("last_error")
            )
        )
        sync_healthy = bool(
            primary_remote
            and not primary_remote.get("stale")
            and not has_sync_error
            and update_state not in FAILED_UPDATE_STATES
        )
        data_verified = bool(state["first_session_ingested_at"])
        connected_but_empty = agent_seen and compatible and sync_healthy and not data_verified

        overall_tone = "amber"
        overall_state = "in_progress"
        health_classification = "pending"
        reason = ""
        next_action = ""

        if not auth_ready:
            overall_tone = "rose"
            overall_state = "blocked"
            health_classification = "failed"
            reason = "Browser authentication could not initialize a working session secret."
            next_action = "Review the server auth configuration and restart the viewer."
        elif bootstrap_required:
            overall_tone = "amber"
            overall_state = "in_progress"
            health_classification = "pending"
            if settings.auth_allows_password():
                reason = "Create or claim the first admin account to continue setup."
                next_action = "Finish the Create Admin step below."
            else:
                reason = "Sign in through the trusted proxy and claim the first admin account to continue setup."
                next_action = "Use the Create Admin step below after your SSO session is visible here."
        elif token_count == 0:
            overall_tone = "amber"
            overall_state = "in_progress"
            health_classification = "configured"
            reason = "No active sync API token exists yet."
            next_action = "Create a labeled token, copy the daemon snippet, and start one machine."
        elif primary_remote is None:
            overall_tone = "rose"
            overall_state = "blocked"
            health_classification = "failed"
            reason = "No machine heartbeat has been received yet."
            next_action = "Start the daemon on one machine with the token and server URL shown below."
        elif update_state in FAILED_UPDATE_STATES:
            overall_tone = "rose"
            overall_state = "blocked"
            health_classification = "failed"
            reason = f"{primary_remote['source_host']} reported an update failure during sync."
            next_action = "Open Machines for the recorded update error, fix the install, and restart the daemon."
        elif has_sync_error:
            overall_tone = "rose"
            overall_state = "blocked"
            health_classification = "failed"
            reason = (
                f"{primary_remote['source_host']} reached the server, but the latest sync pass reported "
                f"{int(primary_remote.get('last_fail_count') or 0)} failed upload"
                f"{'' if int(primary_remote.get('last_fail_count') or 0) == 1 else 's'}."
            )
            next_action = "Open Machines to inspect the failing session path and exception, then rerun the daemon."
        elif primary_remote.get("stale"):
            overall_tone = "amber"
            overall_state = "blocked"
            health_classification = "degraded"
            reason = f"{primary_remote['source_host']} checked in before, but its latest heartbeat is now stale."
            next_action = "Restart the daemon and confirm the machine can still reach this server."
        elif primary_remote.get("api_mismatch"):
            overall_tone = "amber"
            overall_state = "blocked"
            health_classification = "degraded"
            reason = (
                f"{primary_remote['source_host']} is using sync API {primary_remote['sync_api_version']}, "
                f"but the server expects {settings.sync_api_version}."
            )
            next_action = "Update the machine's Agent Operations Viewer install so the sync protocol matches."
        elif primary_remote.get("version_mismatch") or update_state in DEGRADED_UPDATE_STATES:
            overall_tone = "amber"
            overall_state = "blocked"
            health_classification = "degraded"
            reason = (
                f"{primary_remote['source_host']} is running version {primary_remote['agent_version']}, "
                f"but the server target is {settings.expected_agent_version}."
            )
            next_action = "Update and restart the daemon so the machine matches the server target version."
        elif connected_but_empty:
            overall_tone = "amber"
            overall_state = "in_progress"
            health_classification = "connected_but_empty"
            reason = f"{primary_remote['source_host']} is connected and healthy, but no session data has been uploaded yet."
            next_action = "Point the daemon at a Codex sessions directory or generate one new session on that machine."
        elif configured and agent_seen and compatible and sync_healthy and data_verified:
            overall_tone = "emerald"
            overall_state = "complete"
            health_classification = "healthy"
            reason = "First machine connection and first session upload are both verified."
            next_action = "Opening the dashboard."

    if overall_state == "complete" and not state["completed_at"]:
        _update_onboarding_state(connection, completed_at=utc_now_iso())
        state = fetch_onboarding_state(connection)
    elif reason and state["last_failure_reason"] != reason:
        _update_onboarding_state(connection, last_failure_reason=reason)
        state = fetch_onboarding_state(connection)

    if local_mode:
        checks = [
            _check_state(
                label="Configured",
                state="complete" if configured else ("blocked" if bootstrap_required else "pending"),
                detail=(
                    "Auth is ready and the viewer can import local sessions."
                    if configured
                    else (
                        "A first admin still needs to be created."
                        if bootstrap_required
                        else "Review the local auth configuration before continuing."
                    )
                ),
            ),
            _check_state(
                label="Session Roots",
                state="complete" if existing_session_root_count > 0 else "warning",
                detail=(
                    ", ".join(session_root_labels)
                    if existing_session_root_count > 0
                    else (
                        "Configured roots are missing: " + ", ".join(session_root_labels)
                        if session_root_labels
                        else "No session roots are configured."
                    )
                ),
            ),
            _check_state(
                label="Data Imported",
                state="complete" if data_verified else "warning",
                detail=(
                    f"{imported_session_count} imported session{'s' if imported_session_count != 1 else ''} available."
                    if data_verified
                    else "Waiting for the first local session import."
                ),
            ),
        ]

        steps = [
            {
                "key": "auth",
                "title": "Auth",
                "status": "complete" if auth_ready else "blocked",
                "status_label": _status_label("complete" if auth_ready else "blocked"),
                "detail": (
                    "Browser auth is disabled for this instance."
                    if not settings.auth_enabled()
                    else (
                        "Password auth is configured for the web UI."
                        if settings.auth_mode == "password"
                        else (
                            "Proxy / SSO auth is configured for the web UI."
                            if settings.auth_mode == "proxy"
                            else "Password and proxy auth are both available."
                        )
                    )
                ),
            },
            {
                "key": "admin",
                "title": "Create Admin",
                "status": (
                    "complete"
                    if not settings.auth_enabled() or not bootstrap_required
                    else "in_progress"
                ),
                "status_label": _status_label(
                    "complete"
                    if not settings.auth_enabled() or not bootstrap_required
                    else "in_progress"
                ),
                "detail": (
                    "Not required for the current auth mode."
                    if not settings.auth_enabled()
                    else (
                        f"Admin {auth_status.admin_user['display_name']} is ready."
                        if auth_status.admin_user
                        else (
                            "Create the first admin account."
                            if settings.auth_allows_password()
                            else "Sign in through the trusted proxy, then claim the first admin account."
                        )
                    )
                ),
            },
            {
                "key": "import_sessions",
                "title": "Import Sessions",
                "status": "complete" if data_verified else ("blocked" if bootstrap_required else "in_progress"),
                "status_label": _status_label("complete" if data_verified else ("blocked" if bootstrap_required else "in_progress")),
                "detail": (
                    f"{imported_session_count} session{'s' if imported_session_count != 1 else ''} imported from local storage."
                    if data_verified
                    else (
                        "Configured roots are missing. Update CODEX_SESSION_ROOTS or create a Codex session."
                        if existing_session_root_count == 0
                        else "The viewer is waiting for the first local session import."
                    )
                ),
            },
            {
                "key": "verify_library",
                "title": "Verify Library",
                "status": "complete" if configured and data_verified else ("in_progress" if configured else "not_started"),
                "status_label": _status_label("complete" if configured and data_verified else ("in_progress" if configured else "not_started")),
                "detail": (
                    "The dashboard is ready with imported local data."
                    if configured and data_verified
                    else reason or "Waiting for the first successful local import."
                ),
            },
        ]
    else:
        checks = [
            _check_state(
                label="Configured",
                state="complete" if configured else ("blocked" if bootstrap_required else "pending"),
                detail=(
                    f"Auth ready and {token_count} active token{'s' if token_count != 1 else ''} available."
                    if configured
                    else (
                        "A first admin still needs to be created."
                        if bootstrap_required
                        else "Create at least one active sync API token."
                    )
                ),
            ),
            _check_state(
                label="Agent Seen",
                state="complete" if agent_seen else ("warning" if primary_remote else "pending"),
                detail=(
                    f"Fresh heartbeat received from {primary_remote['source_host']}."
                    if agent_seen and primary_remote
                    else (
                        f"Latest heartbeat from {primary_remote['source_host']} is stale."
                        if primary_remote
                        else "Waiting for the first machine heartbeat."
                    )
                ),
            ),
            _check_state(
                label="Compatible",
                state=(
                    "complete"
                    if compatible
                    else ("warning" if primary_remote else "pending")
                ),
                detail=(
                    f"Machine version {primary_remote['agent_version']} and sync API {primary_remote['sync_api_version']} match the server."
                    if compatible and primary_remote
                    else (
                        "Waiting for a machine check-in before compatibility can be verified."
                        if not primary_remote
                        else f"Version/API mismatch detected on {primary_remote['source_host']}."
                    )
                ),
            ),
            _check_state(
                label="Sync Healthy",
                state=(
                    "complete"
                    if sync_healthy
                    else ("warning" if primary_remote else "pending")
                ),
                detail=(
                    f"Latest sync pass from {primary_remote['source_host']} reported no upload failures."
                    if sync_healthy and primary_remote
                    else (
                        "Waiting for a machine heartbeat before sync health can be checked."
                        if not primary_remote
                        else "Latest heartbeat reported sync errors, failed uploads, or update problems."
                    )
                ),
            ),
            _check_state(
                label="Data Verified",
                state="complete" if data_verified else ("warning" if connected_but_empty else "pending"),
                detail=(
                    f"First session upload recorded at {state['first_session_ingested_at']}."
                    if data_verified
                    else (
                        "Machine connectivity is verified, but no session data has been uploaded yet."
                        if connected_but_empty
                        else "Waiting for the first successful session upload."
                    )
                ),
            ),
        ]

        steps = [
            {
                "key": "auth",
                "title": "Auth",
                "status": "complete" if auth_ready else "blocked",
                "status_label": _status_label("complete" if auth_ready else "blocked"),
                "detail": (
                    "Browser auth is disabled for this instance."
                    if not settings.auth_enabled()
                    else (
                        "Password auth is configured for the web UI."
                        if settings.auth_mode == "password"
                        else (
                            "Proxy / SSO auth is configured for the web UI."
                            if settings.auth_mode == "proxy"
                            else "Password and proxy auth are both available."
                        )
                    )
                ),
            },
            {
                "key": "admin",
                "title": "Create Admin",
                "status": (
                    "complete"
                    if not settings.auth_allows_password() or not bootstrap_required
                    else "in_progress"
                ),
                "status_label": _status_label(
                    "complete"
                    if not settings.auth_allows_password() or not bootstrap_required
                    else "in_progress"
                ),
                "detail": (
                    "Not required for the current auth mode."
                    if not settings.auth_enabled()
                    else (
                        f"Admin {auth_status.admin_user['display_name']} is ready."
                        if auth_status.admin_user
                        else (
                            "Create the first admin account."
                            if settings.auth_allows_password()
                            else "Sign in through the trusted proxy, then claim the first admin account."
                        )
                    )
                ),
            },
            {
                "key": "token",
                "title": "Create Token",
                "status": (
                    "complete"
                    if token_count > 0
                    else ("in_progress" if not bootstrap_required else "blocked")
                ),
                "status_label": _status_label(
                    "complete"
                    if token_count > 0
                    else ("in_progress" if not bootstrap_required else "blocked")
                ),
                "detail": (
                    f"{latest_token['label']} is available for the first machine."
                    if latest_token
                    else "Create a labeled sync API token."
                ),
            },
            {
                "key": "connect_agent",
                "title": "Connect Agent",
                "status": (
                    "complete"
                    if agent_seen
                    else ("blocked" if token_count > 0 and primary_remote and primary_remote.get("stale") else ("in_progress" if token_count > 0 else "not_started"))
                ),
                "status_label": _status_label(
                    "complete"
                    if agent_seen
                    else ("blocked" if token_count > 0 and primary_remote and primary_remote.get("stale") else ("in_progress" if token_count > 0 else "not_started"))
                ),
                "detail": (
                    f"Heartbeat received from {primary_remote['source_host']}."
                    if agent_seen and primary_remote
                    else (
                        "A machine checked in before, but it is now stale."
                        if primary_remote and primary_remote.get("stale")
                        else "Start one machine with the generated token and server URL."
                    )
                ),
            },
            {
                "key": "verify_health",
                "title": "Verify Health",
                "status": (
                    "complete"
                    if configured and agent_seen and compatible and sync_healthy and data_verified
                    else (
                        "blocked"
                        if token_count > 0 and primary_remote and (not compatible or not sync_healthy)
                        else (
                            "in_progress"
                            if connected_but_empty
                            else "not_started"
                        )
                    )
                ),
                "status_label": _status_label(
                    "complete"
                    if configured and agent_seen and compatible and sync_healthy and data_verified
                    else (
                        "blocked"
                        if token_count > 0 and primary_remote and (not compatible or not sync_healthy)
                        else (
                            "in_progress"
                            if connected_but_empty
                            else "not_started"
                        )
                    )
                ),
                "detail": (
                    "Heartbeat, compatibility, sync health, and session ingestion are all verified."
                    if configured and agent_seen and compatible and sync_healthy and data_verified
                    else reason or "Waiting for the first healthy machine sync."
                ),
            },
        ]

    return {
        "local_mode": local_mode,
        "session_roots": session_root_labels,
        "existing_session_root_count": existing_session_root_count,
        "imported_session_count": imported_session_count,
        "auth_status": auth_status,
        "bootstrap_required": bootstrap_required,
        "token_count": token_count,
        "latest_token": latest_token,
        "primary_remote": primary_remote,
        "remotes": remotes,
        "configured": configured,
        "agent_seen": agent_seen,
        "compatible": compatible,
        "sync_healthy": sync_healthy,
        "data_verified": data_verified,
        "connected_but_empty": connected_but_empty,
        "overall_state": overall_state,
        "overall_tone": overall_tone,
        "health_classification": health_classification,
        "overall_reason": reason,
        "next_action": next_action,
        "checks": checks,
        "steps": steps,
        "completed_at": state["completed_at"],
        "first_heartbeat_at": state["first_heartbeat_at"],
        "first_heartbeat_source_host": state["first_heartbeat_source_host"],
        "first_session_ingested_at": state["first_session_ingested_at"],
        "first_session_source_host": state["first_session_source_host"],
        "last_failure_reason": state["last_failure_reason"],
        "onboarding_required": not bool(state["completed_at"]),
        "health_evidence": {
            "last_heartbeat_at": None if local_mode else (trimmed(primary_remote.get("last_seen_at")) if primary_remote else state["first_heartbeat_at"]),
            "source_host": state["first_session_source_host"] if local_mode else (trimmed(primary_remote.get("source_host")) if primary_remote else state["first_heartbeat_source_host"]),
            "agent_version": None if local_mode else (trimmed(primary_remote.get("agent_version")) if primary_remote else None),
            "sync_api_version": None if local_mode else (trimmed(primary_remote.get("sync_api_version")) if primary_remote else None),
            "last_upload_count": imported_session_count if local_mode else (int(primary_remote.get("last_upload_count") or 0) if primary_remote else 0),
            "last_skip_count": 0 if local_mode else (int(primary_remote.get("last_skip_count") or 0) if primary_remote else 0),
            "last_fail_count": 0 if local_mode else (int(primary_remote.get("last_fail_count") or 0) if primary_remote else 0),
            "last_error": None if local_mode else (trimmed(primary_remote.get("last_error")) if primary_remote else None),
            "session_data_received": data_verified,
        },
    }
