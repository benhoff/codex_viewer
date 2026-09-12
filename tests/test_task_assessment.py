from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from fastapi.testclient import TestClient

from agent_operations_viewer.db import connect, write_transaction
from agent_operations_viewer.importer import parse_session_text, upsert_parsed_session
from agent_operations_viewer.local_auth import create_initial_admin
from agent_operations_viewer.projects import sync_project_registry
from agent_operations_viewer.task_assessment import (
    DEFAULT_POLICY, NORMALIZATION_VERSION, measure_task, validate_policy, validate_review,
)
from agent_operations_viewer.web.app import create_app
from tests.test_route_auth_audit import make_test_settings


def event(index, record_type="event_msg", *, kind="telemetry", role=None, **data):
    return {"event_index": index, "timestamp": f"2026-09-09T12:00:{index:02d}Z",
            "record_type": record_type, "payload_type": data.get("type"), "kind": kind,
            "role": role, "display_text": data.get("text", ""), "record_json": json.dumps({"type": record_type, "payload": data})}


def usage(index, amount, *, cache=0, output=None, reasoning=None, origin=False):
    counts = {"input_tokens": amount, "output_tokens": output if output is not None else amount // 5,
              "cached_input_tokens": cache, "reasoning_output_tokens": reasoning if reasoning is not None else amount // 10}
    info = {"total_token_usage": counts}
    if origin:
        info["last_token_usage"] = counts.copy()
    return event(index, type="token_count", info=info)


class AccountingTests(unittest.TestCase):
    def measure(self, events, **kwargs):
        return measure_task(events, **{"baseline": None, "prior_context": None, "origin_allowed": True,
                                      "supported": True, "policy": validate_policy(DEFAULT_POLICY), **kwargs})

    def test_cumulative_deltas_duplicates_and_reasoning_subset(self):
        result = self.measure([usage(2, 160, cache=30, output=40, reasoning=25), usage(3, 160, cache=30, output=40, reasoning=25)],
                              baseline=usage(1, 100, cache=10, output=20, reasoning=10))
        self.assertEqual(result["tokens"], {"input_tokens": 60, "cached_input_tokens": 20, "output_tokens": 20, "reasoning_output_tokens": 15})
        self.assertAlmostEqual(result["resource_wu"], .122)
        self.assertEqual(result["duplicate_checkpoints"], 1)
        self.assertEqual(result["usage_coverage"], "recorded")

    def test_origin_needs_matching_last_usage_and_unforked_origin(self):
        self.assertEqual(self.measure([usage(1, 100, origin=True)])["tokens"]["input_tokens"], 100)
        for kwargs in ({}, {"origin_allowed": False}):
            row = usage(1, 100, origin=bool(kwargs))
            result = self.measure([row, usage(2, 160)], **kwargs)
            self.assertEqual(result["tokens"]["input_tokens"], 60)
            self.assertEqual(result["usage_coverage"], "partial")

    def test_missing_fields_reset_and_unsupported_stay_unknown(self):
        partial = usage(1, 100, origin=True)
        record = json.loads(partial["record_json"])
        del record["payload"]["info"]["total_token_usage"]["cached_input_tokens"]
        partial["record_json"] = json.dumps(record)
        result = self.measure([partial])
        self.assertIsNone(result["resource_wu"])
        result = self.measure([usage(1, 100, origin=True), usage(2, 20), usage(3, 50)])
        self.assertEqual(result["tokens"]["input_tokens"], 130)
        self.assertEqual(result["usage_coverage"], "partial")
        result = self.measure([usage(1, 100, origin=True)], supported=False)
        self.assertIsNone(result["resource_wu"])
        self.assertIsNone(result["tokens"]["input_tokens"])

    def test_model_weighting_and_mixed_configuration(self):
        policy = deepcopy(DEFAULT_POLICY)
        policy["models"] = {"model-a": {"weights": {"uncached_input": 2, "cached_input": .2, "output": 8}, "basis": "Test convention"}}
        config = event(1, "turn_context", kind="context", model="model-a", effort="high")
        measured = self.measure([config, usage(2, 100, origin=True)], policy=validate_policy(policy))
        self.assertAlmostEqual(measured["model_weighted_wu"], measured["resource_wu"] * 2)
        mixed = self.measure([config, event(2, "response_item", kind="message", role="assistant", type="message", text="Working"),
                              event(3, "turn_context", kind="context", model="model-b", effort="low"), usage(4, 100, origin=True)],
                             policy=validate_policy(policy))
        self.assertEqual(len(mixed["configurations"]), 2)
        self.assertIsNone(mixed["model_weighted_wu"])
        self.assertIsNotNone(mixed["resource_wu"])
        self.assertIsNone(mixed["actual_serving_model"])

    def test_character_counts_do_not_double_event_mirrors(self):
        events = [event(1, "response_item", kind="message", role="assistant", type="message", phase="commentary", text="Working"),
                  event(2, type="agent_message", text="Working"),
                  event(3, "response_item", kind="message", role="assistant", type="message", phase="final_answer", text="Done")]
        result = self.measure(events)
        self.assertEqual(result["commentary_characters"], 7)
        self.assertEqual(result["final_characters"], 4)
        self.assertIsNone(result["resource_wu"])

    def test_context_changes_without_activity_leave_only_ambiguous_intervals_unpriced(self):
        policy = deepcopy(DEFAULT_POLICY)
        for model in ("model-a", "model-b"):
            policy["models"][model] = {"weights": policy["resource"], "basis": "Test"}
        config_a = event(1, "turn_context", kind="context", model="model-a")
        config_b = event(3, "turn_context", kind="context", model="model-b")
        for configurations in ([config_b], [config_b, {**config_a, "event_index": 4}]):
            with self.subTest(configurations=configurations):
                result = self.measure([config_a, usage(2, 100, origin=True), *configurations,
                                       usage(5, 160), usage(6, 200)], policy=validate_policy(policy))
                self.assertEqual(result["tokens"]["input_tokens"], 200)
                self.assertAlmostEqual(result["resource_wu"], .36)
                self.assertIsNone(result["intervals"][1]["model_weighted_wu"])
                self.assertIsNotNone(result["intervals"][2]["model_weighted_wu"])
                self.assertEqual(result["priced_intervals"], 2)
                self.assertEqual(result["model_coverage"], "partial")

    def test_context_changes_from_baseline_and_before_first_checkpoint_are_ambiguous(self):
        policy = deepcopy(DEFAULT_POLICY)
        for model in ("model-a", "model-b"):
            policy["models"][model] = {"weights": policy["resource"], "basis": "Test"}
        config_a = event(1, "turn_context", kind="context", model="model-a", reasoning_effort="high")
        config_b = event(3, "turn_context", kind="context", model="model-b")
        for events, kwargs in (([config_b, usage(4, 160)], {"baseline": usage(2, 100), "prior_context": config_a}),
                               ([config_a, config_b, usage(4, 160, origin=True)], {})):
            with self.subTest(kwargs=kwargs):
                result = self.measure(events, policy=validate_policy(policy), **kwargs)
                self.assertIsNone(result["model_weighted_wu"])
                self.assertIsNotNone(result["resource_wu"])
                self.assertEqual(result["configurations"][0]["effort"], "high")

    def test_preceding_reroute_does_not_use_its_model_as_supported_configuration(self):
        policy = deepcopy(DEFAULT_POLICY)
        policy["models"]["model-a"] = {"weights": policy["resource"], "basis": "Test"}
        result = self.measure([usage(3, 160)], baseline=usage(1, 100),
                              prior_context=event(2, type="model_reroute", model="model-a"),
                              policy=validate_policy(policy))
        self.assertIsNone(result["model_weighted_wu"])
        self.assertEqual(result["usage_coverage"], "recorded")

    def test_missing_reasoning_detail_does_not_hide_resource_cost_or_claim_complete_reasoning(self):
        partial = usage(2, 160)
        data = json.loads(partial["record_json"])
        del data["payload"]["info"]["total_token_usage"]["reasoning_output_tokens"]
        partial["record_json"] = json.dumps(data)
        result = self.measure([usage(1, 100, origin=True), partial])
        self.assertEqual(result["usage_coverage"], "recorded")
        self.assertEqual(result["token_coverage"]["reasoning_output_tokens"], "partial")
        self.assertEqual(result["tokens"]["reasoning_output_tokens"], 10)
        self.assertEqual(result["tokens"]["input_tokens"], 160)

    def test_command_lifecycle_is_deduplicated_by_call_id(self):
        call = {**event(1, kind="tool_call"), "call_id": "shell-1", "tool_name": "exec_command"}
        result = {**event(2, kind="command"), "call_id": "shell-1"}
        measured = self.measure([call, result])
        self.assertEqual(measured["tool_calls"], 1)
        self.assertEqual(measured["shell_invocations"], 1)

    def test_reroute_stops_pricing_against_previous_model(self):
        policy = deepcopy(DEFAULT_POLICY)
        policy["models"]["model-a"] = {"weights": policy["resource"], "basis": "Test"}
        measured = self.measure([
            event(1, "turn_context", kind="context", model="model-a", effort="high"),
            event(2, type="model_reroute", to_model="model-b"),
            usage(3, 100, origin=True),
        ], policy=validate_policy(policy))
        self.assertIsNone(measured["model_weighted_wu"])
        self.assertEqual(measured["usage_coverage"], "recorded")

    def test_policies_reject_invalid_numbers_and_require_basis(self):
        for value in (float("nan"), float("inf"), -1, True, "2"):
            policy = deepcopy(DEFAULT_POLICY)
            policy["resource"]["output"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_policy(policy)
        policy = deepcopy(DEFAULT_POLICY)
        policy["models"]["a"] = {"weights": policy["resource"]}
        with self.assertRaises(ValueError):
            validate_policy(policy)

    def test_review_requires_evidence_and_never_accepts_validated_claims(self):
        for fields in ({"model_fit": "validated"}, {"outcome": "pass"}, {"evidence_events": "999"}, {"complexity": "6"}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                validate_review(fields, {1})
        result = validate_review({"model_fit": "candidate_for_comparison", "findings": "Narrow task", "evidence_events": "1"}, {1})
        self.assertEqual(result["evidence_level"], "human_trace_review")


def raw_session(turns=2):
    records = [{"type": "session_meta", "payload": {"id": "assess-session", "timestamp": "2026-09-09T12:00:00Z", "cwd": "/workspace/repo", "model_provider": "openai"}}]
    for turn in range(1, turns + 1):
        records.extend([
            {"type": "event_msg", "payload": {"type": "task_started", "turn_id": str(turn)}},
            {"type": "turn_context", "payload": {"turn_id": str(turn), "model": "model-a", "effort": "medium"}},
            {"type": "event_msg", "payload": {"type": "user_message", "message": f"Do task {turn}"}},
            {"type": "response_item", "payload": {"type": "message", "role": "assistant", "phase": "final_answer", "content": [{"type": "output_text", "text": f"Done {turn}"}]}},
            json.loads(usage(1, 100 * turn, origin=True)["record_json"]),
            {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": str(turn), "last_agent_message": f"Done {turn}"}},
        ])
    return "\n".join(json.dumps({**record, "timestamp": f"2026-09-09T12:00:{index:02d}Z"}) for index, record in enumerate(records))


class AssessmentRouteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = make_test_settings(data_dir=Path(self.tmp.name), port=8765)
        self.settings.auth_mode = "none"
        self.app = create_app(self.settings)
        self.client = TestClient(self.app)
        self.import_session()
        self.url = "/sessions/assess-session/assessment"

    def tearDown(self):
        self.client.close()
        self.tmp.cleanup()

    def import_session(self, turns=2):
        parsed = parse_session_text(raw_session(turns), Path(self.tmp.name) / "session.jsonl", Path(self.tmp.name), "test-host")
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            upsert_parsed_session(connection, parsed)
            sync_project_registry(connection)

    def report(self, **params):
        response = self.client.get(self.url + ".json", params={"start_turn": 2, "end_turn": 2, **params})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def save(self, **fields):
        data = self.report()
        form = {"start_turn": "2", "end_turn": "2", "evidence_digest": data["current_evidence_digest"],
                "policy": json.dumps(DEFAULT_POLICY), **fields}
        return self.client.post(self.url, data=form, follow_redirects=False)

    def test_page_export_and_cumulative_task_boundary(self):
        result = self.report()
        self.assertEqual(result["report"]["metrics"]["tokens"]["input_tokens"], 100)
        self.assertAlmostEqual(result["report"]["metrics"]["resource_wu"], .18)
        self.assertEqual(result["review"]["outcome"], "unknown")
        page = self.client.get(self.url, params={"start_turn": 2})
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("Resource Work Units", page.text)
        self.assertIn("Save review revision", page.text)
        self.assertEqual(page.headers["cache-control"], "private, no-store")
        session = self.client.get("/sessions/assess-session")
        self.assertEqual(session.status_code, 200, session.text)
        self.assertIn("Grade turn", session.text)
        self.assertIn('id="chunk-picker"', session.text)

    def test_revisions_snapshot_freshness_and_stale_save(self):
        first = self.report()
        index = first["report"]["evidence"][0]["event_index"]
        response = self.save(model_fit="candidate_for_comparison", findings="Try a smaller model", evidence_events=str(index))
        self.assertEqual(response.status_code, 303, response.text)
        revision = self.report()["saved"]["id"]
        self.assertEqual(self.save().status_code, 303)
        self.assertEqual(len(self.report()["history"]), 2)
        self.import_session(3)
        self.assertFalse(self.report()["stale"], "An unrelated appended turn must not invalidate the review")
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE events SET display_text = 'changed evidence' WHERE session_id = ? AND event_index = ?", ("assess-session", index))
        current = self.report()
        self.assertTrue(current["stale"])
        snapshot = self.report(revision=revision)
        self.assertNotEqual(snapshot["report"]["evidence_digest"], current["current_evidence_digest"])
        self.assertEqual(snapshot["review"]["model_fit"], "candidate_for_comparison")
        self.assertEqual(self.save(evidence_digest=first["current_evidence_digest"]).status_code, 409)
        page = self.client.get(self.url, params={"start_turn": 2, "revision": revision})
        self.assertIn("Saved snapshot", page.text)
        self.assertNotIn("Save review revision", page.text)

    def test_invalid_ranges_reviews_and_cross_origin(self):
        for params, status in (({"start_turn": 1, "end_turn": 51}, 400), ({"start_turn": 2, "end_turn": 1}, 400), ({"start_turn": 50}, 404)):
            self.assertEqual(self.client.get(self.url, params=params).status_code, status)
        self.assertEqual(self.save(outcome="pass").status_code, 400)
        self.assertEqual(self.save(evidence_events="999999").status_code, 400)
        self.assertEqual(self.client.post(self.url, headers={"Origin": "https://elsewhere.example"}).status_code, 403)

    def test_policy_changes_recalculate_without_mutating_usage(self):
        old = self.report()
        policy = deepcopy(DEFAULT_POLICY)
        policy["version"] = "my-policy-v2"
        policy["resource"]["output"] = 8
        self.assertEqual(self.save(policy=json.dumps(policy)).status_code, 303)
        new = self.report()
        self.assertEqual(old["report"]["metrics"]["tokens"], new["report"]["metrics"]["tokens"])
        self.assertAlmostEqual(new["report"]["metrics"]["resource_wu"], .26)

    def test_preceding_reroute_is_retained_and_changes_freshness(self):
        policy = deepcopy(DEFAULT_POLICY)
        policy["models"]["model-a"] = {"weights": policy["resource"], "basis": "Test"}
        self.assertEqual(self.save(policy=json.dumps(policy)).status_code, 303)
        before = self.report()
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            # Simulate a producer with no new context for the selected turn.
            connection.execute("DELETE FROM events WHERE session_id = ? AND event_index = 8", ("assess-session",))
            connection.execute(
                "UPDATE events SET payload_type = 'model_reroute', record_json = ? "
                "WHERE session_id = ? AND event_index = 6",
                (json.dumps({"type": "event_msg", "payload": {"type": "model_reroute", "to_model": "model-b"}}), "assess-session"))
        after = self.report()
        self.assertTrue(after["stale"])
        self.assertEqual(after["report"]["prior_context"]["event_index"], 6)
        self.assertIsNone(after["report"]["metrics"]["model_weighted_wu"])
        self.assertEqual(before["report"]["metrics"]["resource_wu"], after["report"]["metrics"]["resource_wu"])
        self.assertEqual(self.save(policy=json.dumps(policy)).status_code, 303)
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE events SET record_json = ? WHERE session_id = ? AND event_index = 6",
                               (json.dumps({"type": "event_msg", "payload": {"type": "model_reroute", "to_model": "model-c"}}), "assess-session"))
        self.assertTrue(self.report()["stale"], "The accounting context must participate in the evidence digest")

    def test_snapshot_renders_saved_demand_policy_and_normalization_version(self):
        self.assertEqual(self.save(complexity="4", findings="<script>alert(1)</script>").status_code, 303)
        current = self.report()
        revision = current["saved"]["id"]
        snapshot = self.report(revision=revision)
        self.assertEqual(snapshot["report"]["normalization_version"], NORMALIZATION_VERSION)
        self.assertEqual(snapshot["review"]["demand"]["complexity"], 4)
        page = self.client.get(self.url, params={"start_turn": 2, "revision": revision})
        self.assertIn("Task demand", page.text)
        self.assertIn("Complexity</dt><dd>4</dd>", page.text)
        self.assertIn("Saved Work Unit policy", page.text)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", page.text)
        self.assertNotIn("<script>alert(1)</script>", page.text)

    def test_historical_snapshot_survives_removal_of_indexed_turns(self):
        self.assertEqual(self.save().status_code, 303)
        revision = self.report()["saved"]["id"]
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("DELETE FROM session_turns WHERE session_id = ? AND turn_number = 2", ("assess-session",))
        saved = self.report(revision=revision)
        self.assertTrue(saved["stale"])
        self.assertIsNone(saved["current_evidence_digest"])
        self.assertEqual(saved["report"]["start_turn"], 2)

    def test_private_project_and_personal_revision_authorization(self):
        self.client.close()
        self.settings.auth_mode = "proxy"
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            create_initial_admin(connection, username="admin", password="Password123!")
        self.client = TestClient(create_app(self.settings))
        self.client.headers["X-Forwarded-User"] = "alice"
        self.assertEqual(self.save(findings="Alice's personal notes").status_code, 303)
        revision = self.report()["saved"]["id"]
        self.client.headers["X-Forwarded-User"] = "bob"
        self.assertIsNone(self.report()["saved"])
        self.assertEqual(self.client.get(self.url + ".json", params={"start_turn": 2, "revision": revision}).status_code, 404)
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE projects SET visibility = 'private'")
        for suffix in ("", ".json"):
            self.assertEqual(self.client.get(self.url + suffix, params={"start_turn": 2}).status_code, 404)
        self.assertEqual(self.client.post(self.url, data={"start_turn": 2, "end_turn": 2, "policy": json.dumps(DEFAULT_POLICY)}).status_code, 404)


if __name__ == "__main__":
    unittest.main()
