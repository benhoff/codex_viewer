from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from codex_session_viewer import SYNC_API_VERSION, __version__
from codex_session_viewer.config import Settings
from codex_session_viewer.db import connect, init_db, write_transaction
from codex_session_viewer.importer import parse_session_text, upsert_parsed_session
from codex_session_viewer.projects import (
    project_detail_href_for_route,
    resolve_github_project_detail_href,
    sync_project_registry,
)


def make_test_settings(*, data_dir: Path, session_roots: list[Path]) -> Settings:
    return Settings(
        project_root=Path.cwd(),
        environment_name="test",
        data_dir=data_dir,
        database_path=data_dir / "viewer.sqlite3",
        session_roots=session_roots,
        sync_mode="remote",
        app_version=__version__,
        sync_api_version=SYNC_API_VERSION,
        expected_agent_version=__version__,
        agent_update_command=None,
        daemon_rebuild_on_start=False,
        sync_on_start=False,
        page_size=24,
        alerts_enabled=False,
        alerts_provider="webhook",
        alerts_webhook_url=None,
        alerts_realert_minutes=60,
        alerts_send_resolutions=True,
        server_host="127.0.0.1",
        server_port=8000,
        server_base_url="http://127.0.0.1:8000",
        sync_api_token="test-token",
        sync_interval_seconds=30,
        remote_timeout_seconds=15,
        remote_batch_size=25,
        log_level="info",
        source_host="test-host",
        auth_mode="none",
        session_secret=None,
        auth_proxy_user_header="X-Forwarded-User",
        auth_proxy_name_header="X-Forwarded-Name",
        auth_proxy_email_header="X-Forwarded-Email",
        auth_proxy_login_url=None,
        auth_proxy_logout_url=None,
        auth_cookie_secure=False,
    )


def raw_session_jsonl(
    session_id: str,
    *,
    cwd: str,
    github_org: str,
    github_repo: str,
    inferred_project_key: str,
    inferred_project_label: str,
) -> str:
    records = [
        {
            "type": "session_meta",
            "payload": {
                "id": session_id,
                "timestamp": "2026-04-21T02:00:00Z",
                "cwd": cwd,
                "originator": "tester",
                "cli_version": "1.0.0",
                "source": "cli",
                "model_provider": "openai",
                "git": {
                    "repository_url": f"https://github.com/{github_org}/{github_repo}.git",
                },
                "github_org": github_org,
                "github_repo": github_repo,
                "github_slug": f"{github_org}/{github_repo}".lower(),
            },
        },
        {
            "type": "event_msg",
            "timestamp": "2026-04-21T02:00:01Z",
            "payload": {
                "type": "user_message",
                "text": "Investigate the repo.",
            },
        },
        {
            "type": "event_msg",
            "timestamp": "2026-04-21T02:00:02Z",
            "payload": {
                "type": "turn_complete",
                "last_agent_message": "Done.",
            },
        },
    ]
    records[0]["payload"].update(
        {
            "inferred_project_key": inferred_project_key,
            "inferred_project_label": inferred_project_label,
            "inferred_project_kind": "github",
        }
    )
    return "\n".join(json.dumps(record) for record in records)


class ProjectRouteResolutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        data_dir = Path(self.temp_dir.name)
        self.session_root = data_dir / "sessions"
        self.session_root.mkdir(parents=True, exist_ok=True)
        self.settings = make_test_settings(data_dir=data_dir, session_roots=[self.session_root])
        init_db(self.settings.database_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_resolve_github_project_detail_href_matches_github_slug_for_non_github_group_key(self) -> None:
        raw = raw_session_jsonl(
            "session-project-route",
            cwd="/workspace/codex-viewer",
            github_org="openai",
            github_repo="codex-viewer",
            inferred_project_key="openai/codex-viewer",
            inferred_project_label="openai/codex-viewer",
        )
        parsed = parse_session_text(
            raw,
            self.session_root / "session-project-route.jsonl",
            self.session_root,
            "builder-1",
        )

        with connect(self.settings.database_path) as connection:
            with write_transaction(connection):
                upsert_parsed_session(connection, parsed)
                sync_project_registry(connection)

            detail_href = resolve_github_project_detail_href(connection, "openai", "codex-viewer")

        self.assertEqual(detail_href, "/openai/codex-viewer")

    def test_reserved_owner_slug_falls_back_to_projects_namespace(self) -> None:
        self.assertEqual(
            project_detail_href_for_route("sessions", "example-repo"),
            "/projects/sessions/example-repo",
        )


if __name__ == "__main__":
    unittest.main()
