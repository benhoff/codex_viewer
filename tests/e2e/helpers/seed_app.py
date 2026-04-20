from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEPENDENCY_ROOT = PROJECT_ROOT / ".deps"
if DEPENDENCY_ROOT.exists():
    sys.path.insert(0, str(DEPENDENCY_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

from codex_session_viewer.agents import upsert_remote_agent_status  # noqa: E402
from codex_session_viewer.api_tokens import create_api_token  # noqa: E402
from codex_session_viewer.config import Settings  # noqa: E402
from codex_session_viewer.db import connect, init_db, write_transaction  # noqa: E402
from codex_session_viewer.importer import NormalizedEvent, ParsedSession, upsert_parsed_session  # noqa: E402
from codex_session_viewer.local_auth import create_initial_admin, create_local_user, fetch_user_by_username  # noqa: E402
from codex_session_viewer.onboarding import (  # noqa: E402
    record_first_heartbeat,
    record_first_session_ingested,
    reconcile_onboarding_state,
    utc_now_iso,
)
from codex_session_viewer.projects import (  # noqa: E402
    fetch_group_detail,
    sync_project_registry,
    update_project_visibility,
    upsert_project_acl_member,
)
from codex_session_viewer.session_rollups import compute_session_rollups  # noqa: E402


def iso_at(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed Codex Viewer E2E fixtures")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_admin = subparsers.add_parser("create-admin")
    create_admin.add_argument("--username", required=True)
    create_admin.add_argument("--password", required=True)

    create_user = subparsers.add_parser("create-user")
    create_user.add_argument("--username", required=True)
    create_user.add_argument("--password", required=True)
    create_user.add_argument("--role", default="viewer")

    create_token = subparsers.add_parser("create-token")
    create_token.add_argument("--label", default="E2E token")

    heartbeat = subparsers.add_parser("seed-heartbeat")
    heartbeat.add_argument("--source-host", required=True)
    heartbeat.add_argument("--status", choices=["healthy", "degraded", "failed", "stale"], default="healthy")
    heartbeat.add_argument("--upload-count", type=int, default=1)
    heartbeat.add_argument("--skip-count", type=int, default=0)
    heartbeat.add_argument("--fail-count", type=int, default=0)
    heartbeat.add_argument("--last-error", default="")

    session = subparsers.add_parser("seed-session")
    session.add_argument("--source-host", required=True)
    session.add_argument("--project-key", required=True)
    session.add_argument("--project-label", required=True)
    session.add_argument("--github-org", default="")
    session.add_argument("--github-repo", default="")
    session.add_argument("--turns", type=int, default=3)
    session.add_argument("--commands-per-turn", type=int, default=1)
    session.add_argument("--session-index", type=int, default=1)

    visibility = subparsers.add_parser("set-project-visibility")
    visibility.add_argument("--group-key", required=True)
    visibility.add_argument("--visibility", choices=["authenticated", "private"], required=True)

    grant = subparsers.add_parser("grant-project-access")
    grant.add_argument("--group-key", required=True)
    grant.add_argument("--username", required=True)
    grant.add_argument("--role", choices=["viewer", "editor"], default="viewer")

    return parser.parse_args()


def make_message_event(
    *,
    event_index: int,
    timestamp: str,
    role: str,
    record_type: str,
    payload_type: str,
    title: str,
    text: str,
) -> NormalizedEvent:
    return NormalizedEvent(
        event_index=event_index,
        timestamp=timestamp,
        record_type=record_type,
        payload_type=payload_type,
        kind="message",
        role=role,
        title=title,
        display_text=text,
        detail_text=text,
        tool_name=None,
        call_id=None,
        command_text=None,
        exit_code=None,
        record_json=json.dumps(
            {
                "timestamp": timestamp,
                "record_type": record_type,
                "payload_type": payload_type,
                "role": role,
                "text": text,
            },
            sort_keys=True,
        ),
    )


def build_synthetic_session(
    *,
    source_host: str,
    project_key: str,
    project_label: str,
    github_org: str,
    github_repo: str,
    turns: int,
    commands_per_turn: int,
    session_index: int,
) -> ParsedSession:
    repo_name = github_repo or project_key.rsplit("/", 1)[-1]
    org_name = github_org or (project_key.split("/", 1)[0] if "/" in project_key else "")
    github_slug = f"{org_name}/{repo_name}" if org_name and repo_name else None
    start = datetime.now(tz=UTC) - timedelta(minutes=max(turns, 1) * 2 + session_index)
    events: list[NormalizedEvent] = []

    for turn_number in range(1, turns + 1):
        turn_start = start + timedelta(minutes=turn_number * 2)
        user_text = f"Investigate synthetic turn {turn_number} for {project_label}."
        events.append(
            make_message_event(
                event_index=len(events),
                timestamp=iso_at(turn_start),
                role="user",
                record_type="event_msg",
                payload_type="user_message",
                title=f"User prompt {turn_number}",
                text=user_text,
            )
        )
        events.append(
            make_message_event(
                event_index=len(events),
                timestamp=iso_at(turn_start + timedelta(seconds=10)),
                role="assistant",
                record_type="event_msg",
                payload_type="agent_message",
                title=f"Assistant update {turn_number}",
                text=f"Reviewing files for turn {turn_number}.",
            )
        )
        for command_number in range(1, commands_per_turn + 1):
            command_text = f"echo synthetic-turn-{turn_number}-command-{command_number}"
            call_id = f"call-{session_index}-{turn_number}-{command_number}"
            command_time = turn_start + timedelta(seconds=20 + (command_number * 4))
            events.append(
                NormalizedEvent(
                    event_index=len(events),
                    timestamp=iso_at(command_time),
                    record_type="response_item",
                    payload_type="exec_command",
                    kind="command",
                    role=None,
                    title=f"Command {command_number}",
                    display_text=command_text,
                    detail_text=command_text,
                    tool_name="exec_command",
                    call_id=call_id,
                    command_text=command_text,
                    exit_code=0,
                    record_json=json.dumps(
                        {
                            "type": "exec_command",
                            "call_id": call_id,
                            "command": command_text,
                            "exit_code": 0,
                        },
                        sort_keys=True,
                    ),
                )
            )
            events.append(
                NormalizedEvent(
                    event_index=len(events),
                    timestamp=iso_at(command_time + timedelta(seconds=1)),
                    record_type="response_item",
                    payload_type="exec_command_result",
                    kind="tool_result",
                    role=None,
                    title=f"Command result {command_number}",
                    display_text="Command completed",
                    detail_text=f"synthetic output for turn {turn_number} command {command_number}",
                    tool_name="exec_command",
                    call_id=call_id,
                    command_text=None,
                    exit_code=None,
                    record_json=json.dumps(
                        {
                            "type": "exec_command_result",
                            "call_id": call_id,
                            "output": f"synthetic output for turn {turn_number} command {command_number}",
                        },
                        sort_keys=True,
                    ),
                )
            )
        response_text = f"Completed synthetic turn {turn_number} for {project_label}."
        events.append(
            make_message_event(
                event_index=len(events),
                timestamp=iso_at(turn_start + timedelta(seconds=45)),
                role="assistant",
                record_type="response_item",
                payload_type="message",
                title=f"Assistant response {turn_number}",
                text=response_text,
            )
        )
        events.append(
            NormalizedEvent(
                event_index=len(events),
                timestamp=iso_at(turn_start + timedelta(seconds=50)),
                record_type="event_msg",
                payload_type="task_complete",
                kind="system",
                role=None,
                title=f"Task complete {turn_number}",
                display_text="Task complete",
                detail_text="Task complete",
                tool_name=None,
                call_id=None,
                command_text=None,
                exit_code=None,
                record_json=json.dumps({"type": "task_complete", "turn": turn_number}, sort_keys=True),
            )
        )

    rollups = compute_session_rollups(events)
    imported_at = utc_now_iso()
    source_root = Path("/tmp/e2e-sessions") / source_host
    source_path = source_root / project_key.replace("/", "_") / f"session-{session_index}.jsonl"
    record_blob = "\n".join(event.record_json for event in events)
    content_sha256 = hashlib.sha256(record_blob.encode("utf-8")).hexdigest()
    session_id = str(uuid5(NAMESPACE_URL, f"{source_host}:{project_key}:{session_index}:{turns}:{commands_per_turn}"))
    search_text = "\n".join(event.display_text for event in events)
    first_timestamp = events[0].timestamp
    last_timestamp = events[-1].timestamp

    return ParsedSession(
        session_id=session_id,
        source_path=source_path,
        source_root=source_root,
        file_size=len(record_blob.encode("utf-8")),
        file_mtime_ns=0,
        content_sha256=content_sha256,
        session_timestamp=first_timestamp,
        started_at=first_timestamp,
        ended_at=last_timestamp,
        cwd=f"/workspace/{repo_name}",
        cwd_name=repo_name,
        source_host=source_host,
        originator="e2e",
        cli_version="0.1.0",
        source="synthetic",
        model_provider="openai",
        git_branch="main",
        git_commit_hash="deadbeef",
        git_repository_url=f"https://github.com/{github_slug}.git" if github_slug else None,
        github_remote_url=f"https://github.com/{github_slug}.git" if github_slug else None,
        github_org=org_name or None,
        github_repo=repo_name or None,
        github_slug=github_slug,
        inferred_project_kind="github" if github_slug else "directory",
        inferred_project_key=project_key,
        inferred_project_label=project_label,
        summary=f"Synthetic fixture for {project_label}",
        event_count=len(events),
        user_message_count=turns,
        assistant_message_count=turns * 2,
        tool_call_count=turns * commands_per_turn,
        rollup_version=int(rollups["rollup_version"]),
        turn_count=int(rollups["turn_count"]),
        last_user_message=str(rollups["last_user_message"] or ""),
        last_turn_timestamp=rollups["last_turn_timestamp"],
        latest_turn_summary=rollups["latest_turn_summary"],
        command_failure_count=int(rollups["command_failure_count"]),
        aborted_turn_count=int(rollups["aborted_turn_count"]),
        import_warning=None,
        search_text=search_text,
        raw_meta_json=json.dumps({"fixture": "synthetic-e2e", "session_index": session_index}, sort_keys=True),
        imported_at=imported_at,
        updated_at=imported_at,
        events=events,
    )


def main() -> int:
    args = parse_args()
    settings = Settings.from_env(PROJECT_ROOT)
    settings.ensure_directories()
    init_db(settings.database_path)

    with connect(settings.database_path) as connection:
        with write_transaction(connection):
            if args.command == "create-admin":
                user = create_initial_admin(connection, username=args.username, password=args.password)
                reconcile_onboarding_state(connection, settings)
                print(json.dumps({"user": user}))
                return 0

            if args.command == "create-user":
                user = create_local_user(connection, username=args.username, password=args.password, role=args.role)
                reconcile_onboarding_state(connection, settings)
                print(json.dumps({"user": user}))
                return 0

            if args.command == "create-token":
                token = create_api_token(connection, args.label)
                reconcile_onboarding_state(connection, settings)
                print(json.dumps(token))
                return 0

            if args.command == "seed-heartbeat":
                now = datetime.now(tz=UTC)
                seen_at = now
                update_state = "current"
                last_error = args.last_error or None
                fail_count = args.fail_count
                if args.status == "degraded":
                    fail_count = max(fail_count, 1)
                    last_error = last_error or "Upload failures occurred"
                elif args.status == "failed":
                    update_state = "update_failed"
                    fail_count = max(fail_count, 1)
                    last_error = last_error or "Machine update failed"
                elif args.status == "stale":
                    seen_at = now - timedelta(hours=2)

                upsert_remote_agent_status(
                    connection,
                    source_host=args.source_host,
                    agent_version=settings.expected_agent_version,
                    sync_api_version=settings.sync_api_version,
                    sync_mode=settings.sync_mode,
                    update_state=update_state,
                    update_message=last_error if update_state == "update_failed" else None,
                    server_version_seen=settings.app_version,
                    server_api_version_seen=settings.sync_api_version,
                    last_seen_at=iso_at(seen_at),
                    last_sync_at=iso_at(seen_at),
                    last_upload_count=args.upload_count,
                    last_skip_count=args.skip_count,
                    last_fail_count=fail_count,
                    last_error=last_error,
                    last_failed_source_path="/tmp/e2e-sessions/failing.jsonl" if fail_count else None,
                    last_failure_detail="RuntimeError: synthetic upload failure" if fail_count else None,
                )
                record_first_heartbeat(connection, source_host=args.source_host, seen_at=iso_at(seen_at))
                reconcile_onboarding_state(connection, settings)
                print(
                    json.dumps(
                        {
                            "source_host": args.source_host,
                            "status": args.status,
                            "last_seen_at": iso_at(seen_at),
                        }
                    )
                )
                return 0

            if args.command == "seed-session":
                parsed = build_synthetic_session(
                    source_host=args.source_host,
                    project_key=args.project_key,
                    project_label=args.project_label,
                    github_org=args.github_org,
                    github_repo=args.github_repo,
                    turns=args.turns,
                    commands_per_turn=args.commands_per_turn,
                    session_index=args.session_index,
                )
                upsert_parsed_session(connection, parsed)
                sync_project_registry(connection)
                record_first_session_ingested(
                    connection,
                    source_host=args.source_host,
                    imported_at=parsed.imported_at,
                )
                reconcile_onboarding_state(connection, settings)
                print(
                    json.dumps(
                        {
                            "session_id": parsed.session_id,
                            "project_key": parsed.inferred_project_key,
                            "project_label": parsed.inferred_project_label,
                            "source_host": parsed.source_host,
                            "turn_count": parsed.turn_count,
                        }
                    )
                )
                return 0

            if args.command == "set-project-visibility":
                sync_project_registry(connection)
                detail = fetch_group_detail(connection, args.group_key)
                if detail is None or not detail["group"].project_id:
                    raise SystemExit(f"Unknown project group key: {args.group_key}")
                update_project_visibility(connection, str(detail["group"].project_id), args.visibility)
                print(json.dumps({"group_key": args.group_key, "visibility": args.visibility}))
                return 0

            if args.command == "grant-project-access":
                sync_project_registry(connection)
                detail = fetch_group_detail(connection, args.group_key)
                if detail is None or not detail["group"].project_id:
                    raise SystemExit(f"Unknown project group key: {args.group_key}")
                user_row = fetch_user_by_username(connection, args.username)
                if user_row is None:
                    raise SystemExit(f"Unknown user: {args.username}")
                upsert_project_acl_member(
                    connection,
                    project_id=str(detail["group"].project_id),
                    user_id=str(user_row["id"]),
                    role=args.role,
                    granted_by_user_id=None,
                )
                print(json.dumps({"group_key": args.group_key, "username": args.username, "role": args.role}))
                return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
