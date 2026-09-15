from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from agent_operations_viewer.assessment_dashboard import PAGE_SIZE
from agent_operations_viewer import llm_grader as grader
from agent_operations_viewer.db import connect, write_transaction
from agent_operations_viewer.importer import parse_session_text, upsert_parsed_session
from agent_operations_viewer.local_auth import create_initial_admin
from agent_operations_viewer.projects import sync_project_registry
from agent_operations_viewer.task_assessment import DEFAULT_POLICY, task_source
from agent_operations_viewer.web.app import create_app
from tests.test_route_auth_audit import make_test_settings
from tests.test_task_assessment import raw_session


class AssessmentDashboardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = make_test_settings(data_dir=Path(self.tmp.name), port=8765)
        self.settings.auth_mode = "none"
        self.client = TestClient(create_app(self.settings))
        self.seed("alpha", "machine-a", "project-a")
        self.seed("beta", "machine-b", "project-b")

    def tearDown(self):
        self.client.close()
        self.tmp.cleanup()

    def seed(self, session_id, host, project, turns=2):
        raw = raw_session(turns).replace("assess-session", session_id).replace("/workspace/repo", "/workspace/" + project)
        parsed = parse_session_text(raw, Path(self.tmp.name) / (session_id + ".jsonl"), Path(self.tmp.name), host)
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            upsert_parsed_session(connection, parsed)
            sync_project_registry(connection)

    def dashboard(self, **params):
        response = self.client.get("/assessments.json", params={"view": "sessions", **params})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers["cache-control"], "private, no-store")
        return response.json()

    def save(self, session_id="alpha", start=1, end=2, policy=None):
        url = f"/sessions/{session_id}/assessment"
        report = self.client.get(url + ".json", params={"start_turn": start, "end_turn": end}).json()
        response = self.client.post(url, data={"start_turn": start, "end_turn": end,
            "policy": json.dumps(policy or DEFAULT_POLICY), "evidence_digest": report["current_evidence_digest"]}, follow_redirects=False)
        self.assertEqual(response.status_code, 303, response.text)

    def test_all_machines_appear_without_reviews_and_use_whole_session_metrics(self):
        result = self.dashboard()
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["machine_count"], 2)
        self.assertEqual(result["reviewed_count"], 0)
        self.assertEqual({row["source_host"] for row in result["systems"]}, {"machine-a", "machine-b"})
        for item in result["sessions"]:
            self.assertEqual(item["metrics"]["tokens"]["input_tokens"], 200)
            self.assertAlmostEqual(item["metrics"]["resource_wu"], .36)
            self.assertIn("start_turn=1&end_turn=2", item["assessment_href"])
            self.assertNotIn("intervals", item["metrics"])
            self.assertNotIn("record_json", json.dumps(item))
        page = self.client.get("/assessments?view=sessions")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Assessment dashboard", page.text)
        self.assertIn("Assess full session", page.text)

    def test_filters_pagination_and_counts_are_applied_before_trace_reads(self):
        for index in range(PAGE_SIZE):
            self.seed(f"extra-{index}", "machine-c", "project-c", turns=1)
        with patch("agent_operations_viewer.assessment_dashboard.task_source", wraps=task_source) as measure:
            first = self.dashboard()
            self.assertEqual(measure.call_count, PAGE_SIZE)
        second = self.dashboard(page=2)
        self.assertEqual(first["total"], PAGE_SIZE + 2)
        self.assertEqual(len(second["sessions"]), 2)
        self.assertFalse({s["id"] for s in first["sessions"]} & {s["id"] for s in second["sessions"]})
        filtered = self.dashboard(machine="machine-a")
        self.assertEqual([s["id"] for s in filtered["sessions"]], ["alpha"])
        project = filtered["sessions"][0]["project_id"]
        self.assertEqual(self.dashboard(project=project)["total"], 1)
        self.assertEqual(self.dashboard(machine="does-not-exist")["total"], 0)
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE sessions SET last_turn_timestamp = '2000-01-01T00:00:00Z'")
        self.assertEqual(self.dashboard(days=7)["total"], 0)

    def test_latest_personal_review_tracks_its_range_freshness_and_not_revision_count(self):
        self.save(start=2)
        self.save(start=2)
        result = self.dashboard(review="reviewed")
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["reviewed_count"], 1)
        item = result["sessions"][0]
        self.assertEqual(item["review"]["start_turn"], 2)
        self.assertFalse(item["review"]["stale"])
        self.assertEqual(item["metrics"]["tokens"]["input_tokens"], 200)
        self.assertEqual(self.dashboard(review="unreviewed")["total"], 1)
        self.seed("alpha", "machine-a", "project-a", turns=3)
        self.assertFalse(self.dashboard(review="reviewed")["sessions"][0]["review"]["stale"])
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE events SET display_text = 'changed' WHERE session_id = 'alpha' AND event_index = 9")
        self.assertTrue(self.dashboard(review="reviewed")["sessions"][0]["review"]["stale"])

    def test_unknown_unsupported_and_oversized_sessions_do_not_become_zero_cost(self):
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE sessions SET turn_count = 51 WHERE id = 'alpha'")
            connection.execute("UPDATE sessions SET source = 'claude', model_provider = 'anthropic' WHERE id = 'beta'")
        items = {item["id"]: item for item in self.dashboard()["sessions"]}
        self.assertIsNone(items["alpha"]["metrics"])
        self.assertEqual(items["alpha"]["coverage"], "range_required")
        self.assertIn("view=conversation#chunk-picker", items["alpha"]["assessment_href"])
        self.assertIsNone(items["beta"]["metrics"]["resource_wu"])
        self.assertEqual(items["beta"]["coverage"], "unknown")
        with patch("agent_operations_viewer.task_assessment.MAX_EVENTS", 2):
            items = {item["id"]: item for item in self.dashboard()["sessions"]}
        self.assertIsNone(items["beta"]["metrics"])
        self.assertIn("exceeds", items["beta"]["limitation"])

    def test_common_policy_and_saved_review_access_after_turn_removal(self):
        policy = {**DEFAULT_POLICY, "version": "personal", "resource": {"uncached_input": 10, "cached_input": 1, "output": 40}}
        self.save(policy=policy)
        item = self.dashboard(review="reviewed")["sessions"][0]
        self.assertAlmostEqual(item["metrics"]["resource_wu"], .36)
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("DELETE FROM session_turns WHERE session_id = 'alpha' AND turn_number = 2")
        item = self.dashboard(review="reviewed")["sessions"][0]
        self.assertEqual(item["coverage"], "not_indexed")
        self.assertTrue(item["review"]["stale"])
        self.assertIn("revision=", item["review"]["href"])
        self.assertEqual(self.client.get(item["review"]["href"]).status_code, 200)

    def test_project_acl_filters_counts_options_export_and_personal_reviews(self):
        self.client.close()
        self.settings.auth_mode = "proxy"
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            create_initial_admin(connection, username="admin", password="Password123!")
        self.client = TestClient(create_app(self.settings))
        self.client.headers["X-Forwarded-User"] = "alice"
        self.save()
        self.assertEqual(self.dashboard()["reviewed_count"], 1)
        self.client.headers["X-Forwarded-User"] = "bob"
        self.assertEqual(self.dashboard()["reviewed_count"], 0)
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE projects SET visibility = 'private' WHERE id IN "
                               "(SELECT ps.project_id FROM project_sources ps JOIN sessions s "
                               "ON s.inferred_project_key = ps.match_project_key WHERE s.id = 'alpha')")
        result = self.dashboard()
        self.assertEqual(result["total"], 1)
        self.assertNotIn("machine-a", json.dumps(result))
        self.assertNotIn("project-a", json.dumps(result))
        self.assertEqual(self.dashboard(machine="machine-a")["total"], 0)
        self.assertNotIn("machine-a", self.client.get("/assessments").text)

    def test_invalid_filters_and_escaped_session_text(self):
        for params in ({"page": 0}, {"days": -1}, {"review": "pass"}, {"machine": "a" * 201}):
            self.assertEqual(self.client.get("/assessments.json", params=params).status_code, 422)
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE sessions SET summary = '<script>alert(1)</script>' WHERE id = 'alpha'")
        page = self.client.get("/assessments?view=sessions")
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", page.text)
        self.assertNotIn("<script>alert(1)</script>", page.text)

    def seed_run(self, session_id="alpha", start=1, end=1, status="failed", owner="__global__"):
        from tests.test_llm_grader import fake_call
        data = self.client.get(f"/sessions/{session_id}/assessment.json?start_turn={start}&end_turn={end}").json()
        def response(kind):
            return json.loads(fake_call({}, None, '', {'events': data['report']['evidence']}, kind)['response']['choices'][0]['message']['content'])
        result = {"prompt_version": grader.PROMPT_VERSION, "calls": [], "acceptance_criteria": "",
                  "batches": [{"number": 1, "start_turn": start, "end_turn": end, "status": "completed"}],
                  "demand": response(grader.DemandGrade), "configuration": response(grader.ConfigGrade)}
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            return connection.execute("INSERT INTO task_grader_runs (owner_scope, session_id, start_turn, end_turn, "
                "created_at, status, evidence_digest, config_json, snapshot_json, result_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (owner, session_id, start, end, "2026-09-12T08:00:00Z", status, data["current_evidence_digest"],
                 json.dumps(grader.DEFAULT_CONFIG), json.dumps({"report": data["report"]}), json.dumps(result))).lastrowid

    def test_run_dashboard_tracks_ranges_latest_attempt_and_interrupted_workers_without_trace_reads(self):
        self.seed_run(status="completed")
        newest = self.seed_run(status="failed")
        running = self.seed_run(start=2, end=2, status="running")
        self.seed_run(session_id="beta", owner="someone-else", status="completed")
        with patch("agent_operations_viewer.assessment_dashboard.task_source", side_effect=AssertionError("No fleet trace reads")):
            data = self.dashboard(view="runs")
        self.assertEqual(data["total"], 2)
        self.assertEqual({r["id"] for r in data["runs"]}, {newest, running})
        self.assertEqual(data["counts"], {"failed": 1, "interrupted": 1})
        failed = next(r for r in data["runs"] if r["id"] == newest)
        self.assertIsNone(failed["outcome"])  # A partial demand result is not a completed grade.
        self.assertEqual(failed["completed_batches"], 1)
        with grader.ACTIVE_RUNS_LOCK:
            grader.ACTIVE_RUNS.add((str(self.settings.database_path), running))
        try:
            data = self.dashboard(view="runs", status="running")
            self.assertEqual([r["id"] for r in data["runs"]], [running])
        finally:
            with grader.ACTIVE_RUNS_LOCK:
                grader.ACTIVE_RUNS.discard((str(self.settings.database_path), running))
        self.assertNotIn("snapshot", json.dumps(data))
        self.assertEqual(self.client.get("/assessments").status_code, 200)
        self.assertIn("Review and resume", self.client.get("/assessments").text)

    def test_run_filters_and_pagination_apply_to_ranges(self):
        self.seed("many", "machine-c", "project-c", turns=PAGE_SIZE + 1)
        for turn in range(1, PAGE_SIZE + 2):
            self.seed_run(session_id="many", start=turn, end=turn, status="completed")
        self.seed_run()
        first = self.dashboard(view="runs", status="completed")
        second = self.dashboard(view="runs", status="completed", page=2)
        self.assertEqual(first["total"], PAGE_SIZE + 1)
        self.assertEqual(len(first["runs"]), PAGE_SIZE)
        self.assertEqual(len(second["runs"]), 1)
        self.assertFalse({r["id"] for r in first["runs"]} & {r["id"] for r in second["runs"]})
        self.assertIn("view=runs", first["next_href"])
        self.assertEqual(self.dashboard(view="runs", machine="machine-a")["total"], 1)
        self.assertEqual(self.dashboard(view="runs", project=first["runs"][0]["project_id"])["total"], PAGE_SIZE + 1)
        with connect(self.settings.database_path) as c, write_transaction(c):
            c.execute("UPDATE task_grader_runs SET created_at='2000-01-01T00:00:00Z'")
        self.assertEqual(self.dashboard(view="runs", days=7)["total"], 0)

    def test_saved_run_page_uses_frozen_evidence_after_changes_and_turn_removal(self):
        run_id = self.seed_run(status="completed")
        url = f"/sessions/alpha/assessment/grader/{run_id}"
        with connect(self.settings.database_path) as c, write_transaction(c):
            c.execute("UPDATE events SET display_text='CHANGED LIVE SOURCE' WHERE session_id='alpha'")
        current = self.client.get('/sessions/alpha/assessment?start_turn=1&end_turn=1')
        self.assertIn(f'href="{url}#event-', current.text)
        with connect(self.settings.database_path) as c, write_transaction(c):
            c.execute("DELETE FROM session_turns WHERE session_id='alpha'")
        with patch.object(grader, "call_grader") as provider:
            page = self.client.get(url)
        provider.assert_not_called()
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("Estimated outcome", page.text)
        self.assertIn("Evidence has changed", page.text)
        self.assertNotIn("CHANGED LIVE SOURCE", page.text)
        self.assertNotIn('id="submit-grading"', page.text)
        self.assertEqual(page.headers["cache-control"], "private, no-store")
        self.assertEqual(self.client.get(f"/sessions/beta/assessment/grader/{run_id}").status_code, 404)

    def test_run_owner_and_acl_apply_to_dashboard_history_and_export(self):
        private_run = self.seed_run()
        other_run = self.seed_run(session_id="beta", owner="someone-else")
        self.assertEqual(self.client.get(f"/sessions/beta/assessment/grader/{other_run}").status_code, 404)
        self.client.close()
        self.settings.auth_mode = "proxy"
        with connect(self.settings.database_path) as c, write_transaction(c):
            create_initial_admin(c, username="admin", password="Password123!")
        self.client = TestClient(create_app(self.settings))
        self.client.headers["X-Forwarded-User"] = "alice"
        self.client.get("/assessments")  # Provision the proxy identity.
        with connect(self.settings.database_path) as c, write_transaction(c):
            owner = c.execute("SELECT id FROM users WHERE username='alice'").fetchone()[0]
            c.execute("UPDATE task_grader_runs SET owner_scope=? WHERE id=?", (owner, private_run))
        self.assertEqual(self.dashboard(view="runs")["total"], 1)
        self.client.headers["X-Forwarded-User"] = "bob"
        self.assertEqual(self.dashboard(view="runs")["total"], 0)
        self.assertEqual(self.client.get(f"/sessions/alpha/assessment/grader/{private_run}").status_code, 404)
        self.client.headers["X-Forwarded-User"] = "alice"
        with connect(self.settings.database_path) as c, write_transaction(c):
            c.execute("UPDATE projects SET visibility='private' WHERE id IN (SELECT ps.project_id FROM project_sources ps "
                      "JOIN sessions s ON s.inferred_project_key=ps.match_project_key WHERE s.id='alpha')")
        data = self.dashboard(view="runs")
        self.assertEqual(data["total"], 0)
        self.assertNotIn("machine-a", json.dumps(data))
        self.assertNotIn("project-a", json.dumps(data))
        for suffix in ('', '.json'):
            self.assertEqual(self.client.get(f"/sessions/alpha/assessment/grader/{private_run}{suffix}").status_code, 404)


if __name__ == "__main__":
    unittest.main()
