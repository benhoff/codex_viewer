from __future__ import annotations

from pathlib import Path
import json
import tempfile
from typing import Any
import unittest

from agent_operations_viewer.db import connect, init_db
from agent_operations_viewer.projects import (
    ProjectAccessContext,
    build_turn_search_match_expression,
    search_turn_hits,
)
from agent_operations_viewer.search import search_turn_hits_raw
from agent_operations_viewer.search_query import (
    SEARCH_INTENT_LATEST_NEXT_STEP,
    SEARCH_INTENT_REMAINING_ISSUES,
    SEARCH_INTENT_RESOLVED_ISSUES,
    build_lexical_match_expression,
    plan_search_query,
)
from agent_operations_viewer.turn_index import (
    SEARCH_CHUNK_VERSION,
    TURN_INDEX_VERSION,
    TURN_SEARCH_VERSION,
)


NOW = "2026-08-23T12:00:00+00:00"
HWS_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "search" / "hws-history.json"


def insert_search_turn(
    connection: object,
    *,
    session_id: str,
    project_id: str,
    project_key: str,
    project_label: str,
    visibility: str = "authenticated",
    host: str = "search-host",
    turn_number: int = 1,
    timestamp: str = NOW,
    prompt: str = "",
    response: str = "",
    activity: str = "",
    commands: str = "",
    paths: str = "",
    commit_ids: str = "",
    tool_output: str = "",
    import_warning: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO projects (
            id, current_group_key, display_label, visibility, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (project_id, project_key, project_label, visibility, NOW, NOW),
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO project_sources (
            match_project_key, project_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?)
        """,
        (project_key, project_id, NOW, NOW),
    )
    connection.execute(
        """
        INSERT INTO sessions (
            id,
            source_path,
            source_root,
            file_size,
            file_mtime_ns,
            session_timestamp,
            started_at,
            cwd,
            cwd_name,
            source_host,
            inferred_project_kind,
            inferred_project_key,
            inferred_project_label,
            summary,
            turn_index_version,
            turn_search_version,
            import_warning,
            search_text,
            raw_meta_json,
            imported_at,
            updated_at
        ) VALUES (?, ?, ?, 0, 0, ?, ?, ?, ?, ?, 'project', ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?)
        """,
        (
            session_id,
            f"/tmp/{session_id}.jsonl",
            "/tmp",
            timestamp,
            timestamp,
            f"/workspace/{project_label}",
            project_label,
            host,
            project_key,
            project_label,
            prompt or response or session_id,
            TURN_INDEX_VERSION,
            TURN_SEARCH_VERSION,
            import_warning,
            "x" * 200_000 if import_warning else "",
            NOW,
            NOW,
        ),
    )
    connection.execute(
        """
        INSERT INTO session_turns (
            session_id,
            turn_number,
            start_event_index,
            end_event_index,
            prompt_excerpt,
            prompt_timestamp,
            response_excerpt,
            response_timestamp,
            response_state,
            latest_timestamp
        ) VALUES (?, ?, 0, 1, ?, ?, ?, ?, 'final', ?)
        """,
        (session_id, turn_number, prompt[:280], timestamp, response[:320], timestamp, timestamp),
    )
    connection.execute(
        """
        INSERT INTO session_turn_search (
            project_text,
            prompt_text,
            response_text,
            event_text,
            command_text,
            path_text,
            commit_id_text,
            tool_output_text,
            session_id,
            turn_number
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"{project_key}\n{project_label}",
            prompt,
            response,
            activity,
            commands,
            paths,
            commit_ids,
            tool_output,
            session_id,
            turn_number,
        ),
    )


def load_hws_search_fixture() -> dict[str, Any]:
    return json.loads(HWS_FIXTURE_PATH.read_text(encoding="utf-8"))


def insert_hws_search_fixture(connection: object, fixture: dict[str, Any]) -> None:
    for project in fixture["projects"]:
        for turn in project["turns"]:
            insert_search_turn(
                connection,
                session_id=turn["session_id"],
                project_id=project["id"],
                project_key=project["key"],
                project_label=project["label"],
                visibility=project["visibility"],
                host=project["host"],
                turn_number=turn["turn_number"],
                timestamp=turn["timestamp"],
                prompt=turn["prompt"],
                response=turn["response"],
                activity=turn["activity"],
                import_warning=(
                    "Search text truncated during import"
                    if turn.get("legacy_search_capped")
                    else None
                ),
            )


class SearchServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "viewer.sqlite3"
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_query_expression_preserves_current_all_terms_behavior(self) -> None:
        self.assertEqual(
            build_turn_search_match_expression("HWS authentication authentication API"),
            '"hws"* AND "authentication"* AND "api"*',
        )
        self.assertEqual(
            build_lexical_match_expression("alpha beta", mode="any"),
            '"alpha"* OR "beta"*',
        )
        self.assertEqual(
            build_lexical_match_expression("alpha beta", mode="phrase"),
            '"alpha beta"*',
        )
        self.assertEqual(
            build_lexical_match_expression("alpha beta", mode="exact"),
            '"alpha beta"',
        )

    def test_lexical_modes_and_field_filters_are_deterministic(self) -> None:
        with connect(self.db_path) as connection:
            insert_search_turn(
                connection,
                session_id="lexical-one",
                project_id="lexical-project",
                project_key="acme/lexical",
                project_label="acme/lexical",
                prompt="alpha beta gamma prompt-only-marker",
                commands="python3 verify_hardware.py --register VDONE",
                paths="src/device/register_map.py",
                commit_ids="2d48e17abc123",
                tool_output="VDONE reached value 2073600",
            )
            insert_search_turn(
                connection,
                session_id="lexical-two",
                project_id="lexical-project",
                project_key="acme/lexical",
                project_label="acme/lexical",
                prompt="alpha intervening beta",
                response="response-only-marker",
            )

            all_page = search_turn_hits_raw(connection, "alpha beta", mode="all")
            any_page = search_turn_hits_raw(connection, "gamma missing", mode="any")
            phrase_page = search_turn_hits_raw(connection, "alpha bet", mode="phrase")
            exact_page = search_turn_hits_raw(connection, "alpha bet", mode="exact")
            command_page = search_turn_hits_raw(
                connection,
                "verify_hardware",
                fields=["commands"],
            )
            output_page = search_turn_hits_raw(
                connection,
                "2073600",
                fields=["tool_output"],
            )
            path_page = search_turn_hits_raw(
                connection,
                "register_map.py",
                fields=["paths"],
            )
            commit_page = search_turn_hits_raw(
                connection,
                "2d48e17abc123",
                fields=["commit_ids"],
            )
            excluded_page = search_turn_hits_raw(
                connection,
                "prompt-only-marker",
                fields=["response"],
            )

        self.assertEqual(all_page["total_count"], 2)
        self.assertEqual({item["session_id"] for item in any_page["items"]}, {"lexical-one"})
        self.assertEqual([item["session_id"] for item in phrase_page["items"]], ["lexical-one"])
        self.assertEqual(exact_page["total_count"], 0)
        self.assertEqual(command_page["items"][0]["matched_field"], "commands")
        self.assertEqual(output_page["items"][0]["matched_field"], "tool_output")
        self.assertEqual(path_page["items"][0]["matched_field"], "paths")
        self.assertEqual(commit_page["items"][0]["matched_field"], "commit_ids")
        self.assertEqual(excluded_page["total_count"], 0)

    def test_facets_are_computed_before_pagination(self) -> None:
        with connect(self.db_path) as connection:
            insert_search_turn(
                connection,
                session_id="facet-one",
                project_id="facet-project",
                project_key="acme/facet",
                project_label="acme/facet",
                timestamp="2026-08-20T12:00:00+00:00",
                prompt="facet needle",
            )
            insert_search_turn(
                connection,
                session_id="facet-two",
                project_id="facet-project",
                project_key="acme/facet",
                project_label="acme/facet",
                timestamp="2026-08-21T12:00:00+00:00",
                response="facet needle",
            )
            connection.execute(
                "UPDATE sessions SET git_branch = 'feature/facets' WHERE id = 'facet-one'"
            )
            page = search_turn_hits_raw(
                connection,
                "facet needle",
                page_size=1,
                facets=["project", "session", "date", "branch", "matched_field"],
            )

        self.assertEqual(len(page["items"]), 1)
        self.assertEqual(page["facets"]["project"][0]["count"], 2)
        self.assertEqual(len(page["facets"]["session"]), 2)
        self.assertEqual(len(page["facets"]["date"]), 2)
        self.assertEqual(page["facets"]["branch"][0]["value"], "feature/facets")
        self.assertEqual(
            {item["value"] for item in page["facets"]["matched_field"]},
            {"prompt", "response"},
        )

    def test_query_planner_detects_abstract_intent_and_project_hint(self) -> None:
        latest = plan_search_query(
            "What was the last thing that we were going to do on the HWS project?"
        )
        remaining = plan_search_query(
            "What issues were remaining on the HWS project?"
        )
        resolved = plan_search_query("Which issues were resolved on the HWS project?")

        self.assertEqual(latest.intent, SEARCH_INTENT_LATEST_NEXT_STEP)
        self.assertEqual(latest.time_focus, "latest")
        self.assertEqual(latest.project_hint, "hws")
        self.assertEqual(latest.content_terms, ())
        self.assertIn("next", latest.relaxed_terms)
        self.assertEqual(remaining.intent, SEARCH_INTENT_REMAINING_ISSUES)
        self.assertEqual(remaining.status_focus, "open")
        self.assertEqual(resolved.intent, SEARCH_INTENT_RESOLVED_ISSUES)
        self.assertEqual(resolved.status_focus, "resolved")
        self.assertIsNone(plan_search_query("hws project authentication").project_hint)
        natural_keyword = plan_search_query(
            "How did we implement authentication on the HWS project?"
        )
        self.assertTrue(natural_keyword.natural_language)
        self.assertEqual(natural_keyword.project_hint, "hws")
        self.assertEqual(natural_keyword.content_terms, ("implement", "authentication"))

    def test_raw_service_returns_plain_domain_data_and_html_adapter_preserves_markup(self) -> None:
        with connect(self.db_path) as connection:
            insert_search_turn(
                connection,
                session_id="hws-session",
                project_id="hws-project",
                project_key="acme/hws",
                project_label="acme/hws",
                prompt="Investigate the authentication regression",
                response="The authentication regression is in the API token lookup.",
            )

            raw_page = search_turn_hits_raw(connection, "hws authentication")
            html_page = search_turn_hits(connection, "hws authentication")

        self.assertEqual(raw_page["total_count"], 1)
        self.assertEqual(raw_page["items"][0]["project_id"], "hws-project")
        self.assertEqual(raw_page["items"][0]["matched_field"], "prompt")
        self.assertNotIn("[[", raw_page["items"][0]["snippet"])
        self.assertNotIn("<mark>", raw_page["items"][0]["snippet"])

        self.assertEqual(html_page["total_count"], 1)
        self.assertIn("<mark>", html_page["items"][0]["snippet_html"])
        self.assertEqual(html_page["items"][0]["conversation_href"], "/sessions/hws-session?turn=1")

    def test_project_acl_is_applied_inside_search_query(self) -> None:
        with connect(self.db_path) as connection:
            insert_search_turn(
                connection,
                session_id="public-session",
                project_id="public-project",
                project_key="acme/public-hws",
                project_label="acme/public-hws",
                prompt="shared needle",
                response="public evidence",
            )
            insert_search_turn(
                connection,
                session_id="private-session",
                project_id="private-project",
                project_key="acme/private-hws",
                project_label="acme/private-hws",
                visibility="private",
                prompt="shared needle",
                response="private evidence",
            )
            viewer_access = ProjectAccessContext(
                auth_enabled=True,
                bypass=False,
                user_id="viewer-user",
                project_roles={},
            )
            hidden_page = search_turn_hits_raw(
                connection,
                "shared needle",
                project_access=viewer_access,
            )
            relaxed_hidden_page = search_turn_hits_raw(
                connection,
                "What unresolved needle exists?",
                project_access=viewer_access,
            )
            viewer_access.project_roles["private-project"] = "viewer"
            visible_page = search_turn_hits_raw(
                connection,
                "shared needle",
                project_access=viewer_access,
            )

        self.assertEqual(
            {item["session_id"] for item in hidden_page["items"]},
            {"public-session"},
        )
        self.assertEqual(relaxed_hidden_page["retrieval"]["strategy"], "relaxed")
        self.assertEqual(
            {item["session_id"] for item in relaxed_hidden_page["items"]},
            {"public-session"},
        )
        self.assertEqual(
            {item["session_id"] for item in visible_page["items"]},
            {"public-session", "private-session"},
        )

    def test_api_filters_project_host_and_time(self) -> None:
        with connect(self.db_path) as connection:
            insert_search_turn(
                connection,
                session_id="older-session",
                project_id="hws-project",
                project_key="acme/hws",
                project_label="acme/hws",
                host="host-a",
                timestamp="2026-08-20T12:00:00+00:00",
                prompt="filter needle",
            )
            insert_search_turn(
                connection,
                session_id="newer-session",
                project_id="other-project",
                project_key="acme/other",
                project_label="acme/other",
                host="host-b",
                timestamp="2026-08-23T12:00:00+00:00",
                prompt="filter needle",
            )
            page = search_turn_hits_raw(
                connection,
                "filter needle",
                project_id="hws-project",
                host="host-a",
                from_timestamp="2026-08-19T00:00:00+00:00",
                to_timestamp="2026-08-21T00:00:00+00:00",
            )

        self.assertEqual([item["session_id"] for item in page["items"]], ["older-session"])

    def test_coverage_is_filter_aware_acl_safe_and_reports_unknown_freshness(self) -> None:
        with connect(self.db_path) as connection:
            insert_search_turn(
                connection,
                session_id="coverage-known",
                project_id="coverage-public-project",
                project_key="acme/coverage-public",
                project_label="acme/coverage-public",
                host="coverage-host",
                timestamp="2026-08-20T12:00:00+00:00",
                prompt="indexed public evidence",
            )
            insert_search_turn(
                connection,
                session_id="coverage-unknown",
                project_id="coverage-public-project",
                project_key="acme/coverage-public",
                project_label="acme/coverage-public",
                host="coverage-host",
                timestamp="2026-08-21T12:00:00+00:00",
                prompt="indexed timestamp unknown evidence",
            )
            insert_search_turn(
                connection,
                session_id="coverage-private",
                project_id="coverage-private-project",
                project_key="acme/coverage-private",
                project_label="acme/coverage-private",
                visibility="private",
                host="coverage-host",
                timestamp="2026-08-22T12:00:00+00:00",
                prompt="classified coverage evidence",
            )
            insert_search_turn(
                connection,
                session_id="coverage-other-host",
                project_id="coverage-other-project",
                project_key="acme/coverage-other",
                project_label="acme/coverage-other",
                host="other-host",
                timestamp="2026-08-23T12:00:00+00:00",
                prompt="other host evidence",
            )
            connection.executemany(
                """
                UPDATE sessions
                SET search_chunk_version = ?, search_indexed_at = ?
                WHERE id = ?
                """,
                [
                    (
                        SEARCH_CHUNK_VERSION,
                        "2026-08-20T12:05:00+00:00",
                        "coverage-known",
                    ),
                    (SEARCH_CHUNK_VERSION, None, "coverage-unknown"),
                    (
                        SEARCH_CHUNK_VERSION,
                        "2026-08-22T12:05:00+00:00",
                        "coverage-private",
                    ),
                ],
            )
            viewer_access = ProjectAccessContext(
                auth_enabled=True,
                bypass=False,
                user_id="coverage-viewer",
                project_roles={},
            )
            page = search_turn_hits_raw(
                connection,
                "zzznomatchone zzznomatchtwo",
                host="coverage-host",
                from_timestamp="2026-08-20T00:00:00+00:00",
                to_timestamp="2026-08-21T23:59:59+00:00",
                project_access=viewer_access,
            )
            viewer_access.project_roles["coverage-private-project"] = "viewer"
            granted_page = search_turn_hits_raw(
                connection,
                "zzznomatchone zzznomatchtwo",
                host="coverage-host",
                project_access=viewer_access,
            )
            stale_page = search_turn_hits_raw(
                connection,
                "zzznomatchone zzznomatchtwo",
                host="other-host",
                project_access=viewer_access,
            )

        self.assertEqual(page["total_count"], 0)
        coverage = page["coverage"]
        self.assertEqual(coverage["sessions_total"], 2)
        self.assertEqual(coverage["sessions_indexed"], 2)
        self.assertEqual(coverage["turns_total"], 2)
        self.assertEqual(coverage["turns_indexed"], 2)
        self.assertEqual(coverage["pending_reindex_sessions"], 0)
        self.assertEqual(coverage["first_session_at"], "2026-08-20T12:00:00Z")
        self.assertEqual(coverage["last_session_at"], "2026-08-21T12:00:00Z")
        self.assertEqual(coverage["last_indexed_at"], "2026-08-20T12:05:00Z")
        self.assertEqual(coverage["freshness"]["state"], "timestamp_unknown")
        self.assertEqual(coverage["freshness"]["indexed_at_known_sessions"], 1)
        self.assertEqual(coverage["freshness"]["indexed_at_unknown_sessions"], 1)
        self.assertEqual(
            [project["id"] for project in coverage["projects_searched"]],
            ["coverage-public-project"],
        )

        granted_coverage = granted_page["coverage"]
        self.assertEqual(granted_coverage["sessions_total"], 3)
        self.assertEqual(
            {project["id"] for project in granted_coverage["projects_searched"]},
            {"coverage-public-project", "coverage-private-project"},
        )
        self.assertNotIn("coverage-other-project", str(granted_coverage))

        stale_coverage = stale_page["coverage"]
        self.assertEqual(stale_coverage["sessions_total"], 1)
        self.assertEqual(stale_coverage["sessions_indexed"], 0)
        self.assertEqual(stale_coverage["pending_reindex_sessions"], 1)
        self.assertIsNone(stale_coverage["last_indexed_at"])
        self.assertEqual(stale_coverage["freshness"]["state"], "pending_reindex")

    def test_legacy_session_cap_does_not_limit_turn_fts_search(self) -> None:
        with connect(self.db_path) as connection:
            insert_search_turn(
                connection,
                session_id="long-session",
                project_id="hws-project",
                project_key="acme/hws",
                project_label="acme/hws",
                prompt="ordinary prompt",
                response="late unique evidence zanzibar",
                import_warning="Search text truncated during import",
            )
            page = search_turn_hits_raw(connection, "zanzibar")

        self.assertEqual(page["total_count"], 1)
        self.assertEqual(page["items"][0]["session_id"], "long-session")
        self.assertEqual(page["items"][0]["matched_field"], "response")

    def test_abstract_hws_questions_relax_and_auto_scope_to_exact_project(self) -> None:
        fixture = load_hws_search_fixture()
        with connect(self.db_path) as connection:
            insert_hws_search_fixture(connection, fixture)

            latest_page = search_turn_hits_raw(
                connection,
                "What was the last thing that we were going to do on the HWS project?",
            )
            remaining_page = search_turn_hits_raw(
                connection,
                "What was the list of issues remaining on the HWS project?",
            )
            resolved_page = search_turn_hits_raw(
                connection,
                "Which issues were resolved on the HWS project?",
            )
            implementation_page = search_turn_hits_raw(
                connection,
                "How did we implement the raw search service on the HWS project?",
            )

        self.assertEqual(latest_page["retrieval"]["strategy"], "relaxed")
        self.assertEqual(latest_page["retrieval"]["project"]["resolution"], "matched")
        self.assertEqual(latest_page["retrieval"]["project"]["id"], "hws-project")
        self.assertEqual(
            latest_page["items"][0]["session_id"],
            fixture["expected"]["latest_next_step_source"],
        )

        remaining_ids = {item["session_id"] for item in remaining_page["items"]}
        self.assertTrue(
            set(fixture["expected"]["remaining_issue_sources"]) <= remaining_ids
        )
        self.assertNotIn("hws-archive-1", remaining_ids)
        self.assertNotIn("hws-secret-1", remaining_ids)

        resolved_ids = {item["session_id"] for item in resolved_page["items"]}
        self.assertTrue(
            set(fixture["expected"]["resolved_issue_sources"]) <= resolved_ids
        )
        self.assertNotIn("hws-secret-1", resolved_ids)
        self.assertEqual(implementation_page["retrieval"]["strategy"], "relaxed")
        self.assertEqual(implementation_page["items"][0]["session_id"], "hws-plan-5")

    def test_abstract_project_history_is_the_final_fallback(self) -> None:
        with connect(self.db_path) as connection:
            insert_search_turn(
                connection,
                session_id="atlas-old",
                project_id="atlas-project",
                project_key="acme/atlas",
                project_label="acme/atlas",
                timestamp="2026-08-20T12:00:00+00:00",
                response="We selected the blue implementation.",
            )
            insert_search_turn(
                connection,
                session_id="atlas-new",
                project_id="atlas-project",
                project_key="acme/atlas",
                project_label="acme/atlas",
                timestamp="2026-08-21T12:00:00+00:00",
                response="We selected the green implementation.",
            )
            page = search_turn_hits_raw(
                connection,
                "What was the last thing we were going to do on the atlas project?",
            )
            grouped_page = search_turn_hits_raw(
                connection,
                "What was the last thing we were going to do on the atlas project?",
                sort="time_asc",
                group_by="session",
                max_hits_per_session=1,
            )

        self.assertEqual(page["retrieval"]["strategy"], "project_history")
        self.assertEqual(page["items"][0]["session_id"], "atlas-new")
        self.assertEqual(page["items"][0]["matched_field"], "history")
        self.assertEqual(grouped_page["pagination_unit"], "session")
        self.assertEqual(grouped_page["session_count"], 2)
        self.assertEqual(
            [group["session_id"] for group in grouped_page["groups"]],
            ["atlas-old", "atlas-new"],
        )
        self.assertTrue(
            all(len(group["items"]) == 1 for group in grouped_page["groups"])
        )

    def test_unavailable_project_hint_does_not_broaden_across_projects(self) -> None:
        fixture = load_hws_search_fixture()
        with connect(self.db_path) as connection:
            insert_hws_search_fixture(connection, fixture)
            viewer_access = ProjectAccessContext(
                auth_enabled=True,
                bypass=False,
                user_id="fixture-viewer",
                project_roles={},
            )
            page = search_turn_hits_raw(
                connection,
                "What issues remain on the hws-secret project?",
                project_access=viewer_access,
            )

        self.assertEqual(page["total_count"], 0)
        self.assertEqual(page["retrieval"]["project"]["resolution"], "unmatched")

    def test_hws_evaluation_fixture_captures_history_ambiguity_and_acl_boundaries(self) -> None:
        fixture = load_hws_search_fixture()
        with connect(self.db_path) as connection:
            insert_hws_search_fixture(connection, fixture)

            pagination_page = search_turn_hits_raw(
                connection,
                "pagination remains",
                project_id="hws-project",
            )
            late_marker_page = search_turn_hits_raw(connection, "tailmarkerzanzibar")
            viewer_access = ProjectAccessContext(
                auth_enabled=True,
                bypass=False,
                user_id="fixture-viewer",
                project_roles={},
            )
            secret_page = search_turn_hits_raw(
                connection,
                "launch credentials",
                project_access=viewer_access,
            )

        self.assertEqual(
            {item["session_id"] for item in pagination_page["items"]},
            set(fixture["expected"]["remaining_issue_sources"]),
        )
        self.assertEqual(
            late_marker_page["items"][0]["session_id"],
            fixture["expected"]["late_marker_source"],
        )
        self.assertEqual(secret_page["total_count"], 0)
        self.assertEqual(
            fixture["expected"]["latest_next_step_source"],
            "hws-plan-5",
        )


if __name__ == "__main__":
    unittest.main()
