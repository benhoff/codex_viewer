from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from agent_operations_viewer import llm_grader as grader
from agent_operations_viewer.db import connect, write_transaction
from agent_operations_viewer.importer import parse_session_text, upsert_parsed_session
from agent_operations_viewer.local_auth import create_initial_admin
from agent_operations_viewer.projects import sync_project_registry
from agent_operations_viewer.web.app import create_app
from tests.test_route_auth_audit import make_test_settings
from tests.test_task_assessment import raw_session


def fake_call(config, api_key, prompt, data, result_type):
    if result_type is grader.EvidenceNotes:
        indexes = ([data['events'][0]['event_index']] if data.get('events') else
                   data['evidence_notes'][0]['findings'][0]['event_indexes'])
        output = {"findings": [{"text": "Observed bounded work", "event_indexes": indexes}],
                  "limitations": "Only the supplied evidence was examined."}
    elif result_type is grader.DemandGrade:
        output = {"required_level": 2, "required_low": 1, "required_high": 3, "confidence": "medium", "outcome": "unknown",
                  "acceptance_criteria": "Complete the requested task", "verification_notes": "No independent check recorded",
                  "findings": [{"text": "The request is bounded", "event_indexes": [data["events"][0]["event_index"]]}],
                  "recommended_experiment": "Repeat with a lighter configuration and independent checks."}
    else:
        output = {"configured_level": 4, "confidence": "low", "basis": "Illustrative test estimate, not a benchmark."}
    return {"elapsed_seconds": .1, "response": {"model": "test-grader", "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120},
            "choices": [{"finish_reason": "stop", "message": {"content": json.dumps(output)}}]}}


class SynthesisTests(unittest.TestCase):
    def test_business_json_is_not_mistaken_for_transport(self):
        for value in ([1, 2, 3], [{'expected': 1}, {'actual': 2}], {'name': 'Product', 'image_url': 'https://example.com/image.png'}):
            text = json.dumps(value)
            self.assertEqual(grader.evidence_text(text), text)

    def test_portable_extraction_schema_keeps_strict_fields_and_local_bounds(self):
        schema = grader.request_schema(grader.EvidenceNotes)
        self.assertFalse(schema['additionalProperties'])
        self.assertEqual(set(schema['required']), {'findings', 'limitations'})
        self.assertNotIn('$ref', grader.canonical_json(schema))
        self.assertFalse(schema['properties']['findings']['items']['additionalProperties'])
        for output in ({'findings': [], 'limitations': 'No evidence'},
                       {'findings': [{'text': '', 'event_indexes': [1]}], 'limitations': 'No evidence'},
                       {'findings': [{'text': 'Claim', 'event_indexes': ['1']}], 'limitations': 'No evidence'}):
            with self.subTest(output=output), self.assertRaises(ValueError):
                grader.EvidenceNotes.model_validate(output)

    def report(self):
        return {'evidence': [
            {'event_index': 1, 'kind': 'message', 'role': 'user', 'display_text': 'Assess the cluttered project UI.'},
            {'event_index': 2, 'kind': 'tool_result', 'display_text': 'Measured mobile prompt width: 3.5px.\n' * 700},
            {'event_index': 3, 'kind': 'message', 'role': 'assistant', 'display_text': 'The repeated navigation crowds out the turn prompt.'},
            {'event_index': 4, 'kind': 'message', 'role': 'user', 'display_text': 'Prioritize the turn timeline and session drill-down.'},
            {'event_index': 5, 'kind': 'message', 'role': 'assistant', 'display_text': 'Use a full-width timeline with linked turn titles.'},
        ], 'turns': [{'turn_number': 1, 'start_event_index': 1}, {'turn_number': 2, 'start_event_index': 4}],
            'metrics': {'configurations': [{'model': 'example', 'effort': 'high'}]}}

    def test_mixed_media_and_nested_transport_never_become_binary_text_batches(self):
        report = self.report()
        report['evidence'][1]['display_text'] = json.dumps([
            {'type': 'text', 'text': json.dumps({'status': 'fulfilled', 'value': {'output': 'Width: 3.5px', 'exit_code': 0, 'chunk_id': 'transport-id'}})},
            {'image_url': 'data:image/png;base64,' + 'A' * 1200000},
            {'type': 'image', 'data': 'B' * 120000, 'mimeType': 'image/png'}])
        original = report['evidence'][1]['display_text']
        inputs = grader.grader_input(report, '', grader.DEFAULT_CONFIG)
        self.assertIn('demand', inputs)
        serialized = grader.canonical_json(inputs)
        self.assertNotIn('base64', serialized)
        self.assertNotIn('transport-id', serialized)
        self.assertNotIn('BBBBBBBB', serialized)
        self.assertIn('Width: 3.5px', serialized)
        self.assertIn('Media omitted', serialized)
        self.assertEqual(report['evidence'][1]['display_text'], original)

    def test_synthesis_receives_complete_dialogue_and_can_establish_pass(self):
        inputs = grader.grader_input(self.report(), '', grader.DEFAULT_CONFIG)
        result = {'calls': []}
        def response(*args):
            payload = fake_call(*args)
            if args[-1] is grader.DemandGrade:
                self.assertEqual([e['event_index'] for e in args[3]['events'] if e.get('role') in {'user', 'assistant'}], [1, 3, 4, 5])
                self.assertIn('evidence_notes', args[3])
                self.assertEqual(args[0]['max_output_tokens'], 1024)
                output = json.loads(payload['response']['choices'][0]['message']['content'])
                output.update(outcome='pass', verification_notes='The critique responds to the clarified workflow and measured layout issue.')
                payload['response']['choices'][0]['message']['content'] = json.dumps(output)
            return payload
        with patch.object(grader, 'call_grader', side_effect=response):
            grader.grade(inputs, grader.DEFAULT_CONFIG, None, result)
        self.assertEqual(result['demand']['outcome'], 'pass')
        self.assertTrue(all('evidence' in b and 'demand' not in b for b in result['batches']))
        self.assertEqual(result['calls'][-2]['stage'], 'synthesis')
        self.assertIn('input', result['calls'][-2])
        self.assertFalse(result['coverage']['conversation_excerpted'])
        self.assertIn('Only the supplied evidence was examined.', result['coverage']['note_limitations'])

    def test_incomplete_synthesis_retries_only_synthesis_and_configuration(self):
        inputs = grader.grader_input(self.report(), '', grader.DEFAULT_CONFIG)
        result = {'calls': []}
        def incomplete(*args):
            payload = fake_call(*args)
            if args[-1] is grader.DemandGrade:
                payload['response']['choices'][0]['finish_reason'] = 'length'
            return payload
        with patch.object(grader, 'call_grader', side_effect=incomplete), self.assertRaisesRegex(grader.GraderError, 'incomplete'):
            grader.grade(inputs, grader.DEFAULT_CONFIG, None, result)
        self.assertNotIn('demand', result)
        self.assertTrue(all(b['status'] == 'completed' for b in result['batches']))
        with patch.object(grader, 'call_grader', side_effect=fake_call) as calls:
            grader.grade(inputs, grader.DEFAULT_CONFIG, None, result)
        self.assertEqual(calls.call_count, 2)

    def test_reduction_is_bounded_and_preserves_original_citations(self):
        inputs = {'synthesis': {'events': [{'event_index': 1, 'role': 'user', 'text': 'Assess the UI'}]},
                  'demand_batches': []}
        result = {'calls': [], 'batches': [{'evidence': {'findings': [{'text': 'Observed issue. ' * 35, 'event_indexes': [i]}],
                                                        'limitations': 'Sample data'}} for i in range(2, 22)]}
        config = {**grader.DEFAULT_CONFIG, 'max_input_chars': 4000}
        def compact(*args):
            self.assertLessEqual(grader.json_size(args[3]), 4000)
            payload = fake_call(*args)
            if args[-1] is grader.EvidenceNotes:
                output = {'findings': [{'text': 'Observed layout issues', 'event_indexes': sorted({i for n in args[3]['evidence_notes'] for f in n['findings'] for i in f['event_indexes']})}],
                          'limitations': 'Sample data'}
                payload['response']['choices'][0]['message']['content'] = json.dumps(output)
            return payload
        with patch.object(grader, 'call_grader', side_effect=compact):
            grader.synthesize(inputs, config, None, result, lambda: None)
        self.assertTrue(result['reductions'])
        final = result['calls'][-1]['input']
        self.assertEqual({i for n in final['evidence_notes'] for f in n['findings'] for i in f['event_indexes']}, set(range(2, 22)))

    def test_synthesis_rejects_invented_citations(self):
        data = {'events': [{'event_index': 1}], 'evidence_notes': [{'findings': [{'text': 'Measurement', 'event_indexes': [2]}], 'limitations': 'Sample data'}]}
        def invalid(*args):
            payload = fake_call(*args)
            output = json.loads(payload['response']['choices'][0]['message']['content'])
            output['findings'][0]['event_indexes'] = [999]
            payload['response']['choices'][0]['message']['content'] = json.dumps(output)
            return payload
        with patch.object(grader, 'call_grader', side_effect=invalid), self.assertRaisesRegex(grader.GraderError, 'outside this task batch'):
            grader.grade_stage('synthesis', grader.SYNTHESIS_PROMPT, grader.DemandGrade, data, grader.DEFAULT_CONFIG, None, {'calls': []}, lambda: None)


class GraderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = make_test_settings(data_dir=Path(self.tmp.name), port=8765)
        self.settings.auth_mode = "none"
        self.client = TestClient(create_app(self.settings))
        parsed = parse_session_text(raw_session(), Path(self.tmp.name) / "session.jsonl", Path(self.tmp.name), "test-host")
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            upsert_parsed_session(connection, parsed)
            sync_project_registry(connection)
            grader.save_api_key(connection, self.settings.data_dir, "secret-test-key")
        self.url = "/sessions/assess-session/assessment"

    def tearDown(self):
        self.client.close()
        self.tmp.cleanup()

    def configure(self, **fields):
        config = {**grader.DEFAULT_CONFIG, "enabled": True, "model": "test-grader", **fields}
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            grader.save_config(connection, config)

    def report(self):
        return self.client.get(self.url + ".json").json()

    def run_grade(self, **fields):
        return self.client.post(self.url + "/grade", data={"start_turn": 1, "end_turn": 1,
            "evidence_digest": self.report()["current_evidence_digest"], "acceptance_criteria": "Implement task", **fields}, follow_redirects=False)

    def test_disabled_by_default_and_reading_or_saving_never_calls_provider(self):
        with patch.object(grader, "call_grader") as call:
            self.assertEqual(self.run_grade().status_code, 409)
            self.assertEqual(self.client.get(self.url).status_code, 200)
            call.assert_not_called()
        self.assertFalse(self.report()["grader_config"]["enabled"])

    def test_two_blinded_stages_chart_export_and_human_review_separation(self):
        self.configure()
        before = self.report()
        with patch.object(grader, "call_grader", side_effect=fake_call) as call:
            self.assertEqual(self.run_grade().status_code, 303)
        self.assertEqual(call.call_count, 2)
        self.assertEqual(call.call_args_list[0].args[1], "secret-test-key")
        demand_input = call.call_args_list[0].args[3]
        self.assertNotIn("model-a", json.dumps(demand_input))
        self.assertNotIn("input_tokens", json.dumps(demand_input))
        self.assertNotIn("turn_context", json.dumps(demand_input))
        self.assertEqual(set(call.call_args_list[1].args[3]), {"observations"})
        after = self.report()
        self.assertEqual(before["review"], after["review"])
        self.assertEqual(before["report"]["metrics"], after["report"]["metrics"])
        run = after["grade_run"]
        self.assertEqual(run["status"], "completed")
        self.assertEqual(run["result"]["evidence_level"], "llm_trace_estimate")
        self.assertFalse(run["stale"])
        exported = self.client.get(self.url + f"/grader/{run['id']}.json")
        self.assertEqual(exported.status_code, 200)
        self.assertEqual(exported.headers["cache-control"], "private, no-store")
        self.assertEqual(exported.json()["snapshot"]["inputs"]["demand"], demand_input)
        self.assertEqual(run["result"]["schemas"]["demand"], grader.DemandGrade.model_json_schema())
        self.assertNotIn("secret-test-key", exported.text)
        page = self.client.get(self.url).text
        self.assertIn("Configured intelligence", page)
        self.assertIn("Required intelligence", page)
        self.assertIn("4 / 5", page)
        self.assertIn("Plausible required range: 1–3", page)

    def test_mixed_or_missing_configuration_cannot_establish_capability(self):
        inputs = grader.grader_input(self.report()["report"], "Complete the task", grader.DEFAULT_CONFIG)
        for observations in ([], [{"model": "model-a", "effort": None}],
                             [{"model": "model-a", "effort": "high"}, {"model": "model-b", "effort": "low"}]):
            inputs["configuration"]["observations"] = observations
            with self.subTest(observations=observations), patch.object(grader, "call_grader", side_effect=fake_call):
                result = {"calls": []}
                with self.assertRaises(grader.GraderError):
                    grader.grade(inputs, grader.DEFAULT_CONFIG, None, result)
                self.assertNotIn("configuration", result)
                self.assertEqual(result["calls"][1]["usage"]["total_tokens"], 120)

    def test_second_stage_failure_preserves_attempt_and_evaluator_usage(self):
        self.configure()
        def fail_second(*args):
            if args[-1] is grader.ConfigGrade:
                raise grader.GraderError("Grader connection failed or timed out.")
            return fake_call(*args)
        with patch.object(grader, "call_grader", side_effect=fail_second):
            self.assertEqual(self.run_grade().status_code, 303)
        run = self.report()["grade_run"]
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["result"]["calls"][0]["usage"]["total_tokens"], 120)
        self.assertIsNone(run["result"]["calls"][1]["usage"])
        self.assertEqual(run["result"]["calls"][1]["status"], "failed")
        self.assertNotIn("4 / 5", self.client.get(self.url).text)

    def test_invalid_output_and_out_of_range_citations_are_not_saved_as_estimates(self):
        self.configure()
        for invalid, code in (({"required_level": True}, "schema_validation"), ({"required_low": 5}, "inconsistent_uncertainty"),
                              ({"confidence": "unknown"}, "inconsistent_uncertainty"),
                              ({"findings": [{"text": "Fabricated", "event_indexes": [999]}]}, "invalid_citations")):
            def malformed(*args):
                response = fake_call(*args)
                choice = response["response"]["choices"][0]
                output = json.loads(choice["message"]["content"])
                output.update(invalid)
                choice["message"]["content"] = json.dumps(output)
                return response
            with self.subTest(invalid=invalid), patch.object(grader, "call_grader", side_effect=malformed):
                self.assertEqual(self.run_grade().status_code, 303)
                run = self.report()["grade_run"]
                self.assertEqual(run["status"], "failed")
                self.assertEqual(len(run["result"]["calls"]), 1)
                self.assertEqual(run["result"]["calls"][0]["usage"]["total_tokens"], 120)
                self.assertEqual(run["result"]["calls"][0]["validation_error"]["code"], code)
                self.assertEqual(run["result"]["calls"][0]["finish_reason"], "stop")
                self.assertNotIn("demand", run["result"])

    def test_schema_failure_diagnostics_do_not_echo_provider_values_or_extra_keys(self):
        self.configure()
        def malformed(*args):
            response = fake_call(*args)
            choice = response['response']['choices'][0]
            output = json.loads(choice['message']['content'])
            output['required_level'] = 'private-provider-value'
            output['private-provider-field'] = 'private-provider-value'
            choice['message']['content'] = json.dumps(output)
            return response
        with patch.object(grader, 'call_grader', side_effect=malformed):
            self.run_grade()
        run = self.report()['grade_run']
        diagnostics = run['result']['calls'][0]['validation_error']
        self.assertEqual(diagnostics['code'], 'schema_validation')
        self.assertEqual({entry['field'] for entry in diagnostics['fields']}, {'required_level', '<extra>'})
        exported = self.client.get(self.url + f"/grader/{run['id']}.json").text
        self.assertNotIn('private-provider-value', exported)
        self.assertNotIn('private-provider-field', exported)

    def test_request_schema_clarifies_rating_confidence_and_is_saved_per_call(self):
        self.configure()
        with patch.object(grader, 'call_grader', side_effect=fake_call):
            self.run_grade()
        run = self.report()['grade_run']
        call = run['result']['calls'][0]
        self.assertEqual(call['request_schema'], grader.request_schema(grader.DemandGrade))
        self.assertIn('NOT in the outcome', call['request_schema']['properties']['confidence']['description'])
        self.assertIn('all three required levels MUST be null', call['request_schema']['properties']['confidence']['description'])
        # Unknown outcome does not invalidate a supported numeric demand rating.
        self.assertEqual(run['status'], 'completed')
        self.assertEqual(run['result']['demand']['outcome'], 'unknown')
        self.assertEqual(run['result']['demand']['required_level'], 2)

    def test_stale_submission_and_oversize_input_send_nothing(self):
        self.configure(max_input_chars=1000)
        with patch.object(grader, "call_grader") as call:
            self.assertEqual(self.run_grade(evidence_digest="stale").status_code, 409)
            self.assertEqual(self.run_grade(acceptance_criteria="x" * 2000).status_code, 400)
            call.assert_not_called()
        self.assertIsNone(self.report()["grade_run"])

    def test_malformed_json_and_truncated_valid_json_fail_without_retry(self):
        self.configure()
        for mode in ("malformed", "length"):
            def bad(*args):
                response = fake_call(*args)
                choice = response["response"]["choices"][0]
                if mode == "malformed":
                    choice["message"]["content"] = '{"required_level":'
                else:
                    choice["finish_reason"] = "length"
                return response
            with self.subTest(mode=mode), patch.object(grader, "call_grader", side_effect=bad) as calls:
                self.run_grade()
                run = self.report()["grade_run"]
                self.assertEqual(run["status"], "failed")
                self.assertNotIn("demand", run["result"])
                self.assertEqual(calls.call_count, 1)
                self.assertEqual(run['result']['calls'][0]['validation_error']['code'],
                                 'malformed_json' if mode == 'malformed' else 'output_limit')

    def test_cancel_aborts_http_and_next_run_can_start_immediately(self):
        received, disconnected = threading.Event(), threading.Event()
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                received.set()
                self.connection.settimeout(5)
                if self.connection.recv(1) == b'':
                    disconnected.set()
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.configure(processing='local', base_url=f'http://127.0.0.1:{server.server_port}/v1')
        try:
            response = self.client.post(self.url + '/grade', headers={'Accept': 'application/json'}, data={
                'start_turn': 1, 'end_turn': 1, 'evidence_digest': self.report()['current_evidence_digest']})
            self.assertEqual(response.status_code, 202)
            status_url = response.json()['status_url']
            cancel_url = status_url.replace('/status', '/cancel')
            self.assertTrue(received.wait(2))
            self.assertEqual(self.client.post(cancel_url, headers={'Origin': 'https://elsewhere.example'}).status_code, 403)
            self.assertFalse(disconnected.is_set())
            self.assertEqual(self.client.post(cancel_url).status_code, 200)
            self.assertTrue(disconnected.wait(2))
            deadline = time.monotonic() + 3
            while self.client.get(status_url).json()['status'] == 'running' and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertEqual(self.client.get(status_url).json()['status'], 'cancelled')
            with patch.object(grader, 'call_grader', side_effect=fake_call):
                self.assertEqual(self.run_grade().status_code, 303)
            self.assertEqual(self.report()['grade_run']['status'], 'completed')
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_large_evidence_batches_checkpoint_and_retry_only_unfinished_work(self):
        self.configure(max_input_chars=1800)
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE events SET display_text = ? WHERE session_id = 'assess-session' AND payload_type = 'user_message'",
                               ('Long evidence with quotes " and unicode 漢字. ' * 80,))
        planned = grader.grader_input(self.report()["report"], "Implement task", {**grader.DEFAULT_CONFIG, "max_input_chars": 1800})
        self.assertGreater(len(planned["demand_batches"]), 1)
        seen = []
        def fail_second(config, key, prompt, data, result_type):
            seen.append(data)
            self.assertLessEqual(len(grader.canonical_json(data)), config["max_input_chars"])
            if len(seen) == 2:
                with connect(self.settings.database_path) as connection:
                    saved = json.loads(connection.execute("SELECT result_json FROM task_grader_runs ORDER BY id DESC LIMIT 1").fetchone()[0])
                    self.assertEqual(saved["batches"][0]["status"], "completed")
                raise grader.GraderError("Temporary provider failure")
            return fake_call(config, key, prompt, data, result_type)
        with patch.object(grader, "call_grader", side_effect=fail_second):
            self.assertEqual(self.run_grade().status_code, 303)
        failed = self.report()["grade_run"]
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["result"]["batches"][0]["status"], "completed")
        self.assertEqual(failed["result"]["batches"][1]["status"], "failed")
        self.assertIn("Retry unfinished batches", self.client.get(self.url).text)
        with patch.object(grader, "call_grader", side_effect=fake_call) as calls:
            self.assertEqual(self.run_grade(retry_run_id=str(failed["id"])).status_code, 303)
        finished = self.report()["grade_run"]
        self.assertEqual(finished["id"], failed["id"])
        self.assertEqual(finished["status"], "completed")
        self.assertGreaterEqual(calls.call_count, len(planned["demand_batches"]) + 1)  # Remaining evidence, synthesis, configuration.
        self.assertEqual(calls.call_args_list[0].args[3], planned["demand_batches"][1])
        self.assertEqual(finished["result"]["demand"]["outcome"], "unknown")
        self.assertEqual(finished["result"]["demand"]["required_level"], 2)
        self.assertIn('synthesis', [c['stage'] for c in finished['result']['calls']])
        self.assertEqual(finished["result"]["calls"][1]["status"], "failed")
        self.assertIn("Batch results", self.client.get(self.url).text)
        export = self.client.get(self.url + f"/grader/{failed['id']}.json").json()
        self.assertEqual(export["snapshot"]["inputs"]["demand_batches"], planned["demand_batches"])

    def test_retry_rejects_changed_criteria_and_preserves_original_attempt(self):
        self.configure()
        with patch.object(grader, "call_grader", side_effect=grader.GraderError("Unavailable")):
            self.run_grade()
        failed = self.report()["grade_run"]
        with patch.object(grader, "call_grader") as calls:
            response = self.run_grade(retry_run_id=str(failed["id"]), acceptance_criteria="Changed criteria")
            self.assertEqual(response.status_code, 409)
            calls.assert_not_called()
        self.assertEqual(self.report()["grade_run"], failed)

    def test_changed_limits_show_old_run_settings_and_require_new_submission(self):
        self.configure(timeout_seconds=120, max_input_chars=100000, max_output_tokens=2048)
        with patch.object(grader, 'call_grader', side_effect=grader.GraderError('Grader timed out.')):
            self.run_grade()
        self.configure(timeout_seconds=600, max_input_chars=20000, max_output_tokens=512)
        page = self.client.get(self.url).text
        self.assertIn('Limits used for this run: 120 seconds per call', page)
        self.assertIn('Settings have changed since this run', page)
        self.assertIn('600-second timeout', page)
        self.assertNotIn('name="retry_run_id"', page)
        self.assertIn('Submit chunk for AI grading', page)

    def test_old_context_runs_remain_visible_but_cannot_resume_with_new_batches(self):
        self.configure()
        with patch.object(grader, 'call_grader', side_effect=grader.GraderError('Stopped')):
            self.run_grade()
        run = self.report()['grade_run']
        run['result']['prompt_version'] = 'capability-grader-v3-clean-bounded'
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute('UPDATE task_grader_runs SET result_json=? WHERE id=?', (json.dumps(run['result']), run['id']))
        page = self.client.get(self.url).text
        self.assertIn('Batch context has been updated', page)
        self.assertNotIn('name="retry_run_id"', page)
        with patch.object(grader, 'call_grader') as calls:
            self.assertEqual(self.run_grade(retry_run_id=str(run['id'])).status_code, 409)
            calls.assert_not_called()
        self.assertEqual(self.client.get(self.url + f"/grader/{run['id']}.json").status_code, 200)

    def test_interrupted_run_can_resume_checkpointed_evidence_stage(self):
        self.configure()
        def fail_configuration(*args):
            if args[-1] is grader.ConfigGrade:
                raise grader.GraderError("Interrupted")
            return fake_call(*args)
        with patch.object(grader, "call_grader", side_effect=fail_configuration):
            self.run_grade()
        run = self.report()["grade_run"]
        run["result"]["calls"][-1]["status"] = "started"
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE task_grader_runs SET status = 'running', completed_at = NULL, result_json = ? WHERE id = ?",
                               (json.dumps(run["result"]), run["id"]))
        status_url = self.url + f"/grader/{run['id']}/status"
        self.assertEqual(self.client.get(status_url).json()["status"], "interrupted")
        with patch.object(grader, "call_grader", side_effect=fake_call) as calls:
            self.assertEqual(self.run_grade(retry_run_id=str(run["id"])).status_code, 303)
        self.assertEqual(calls.call_count, 1)
        self.assertIs(calls.call_args.args[-1], grader.ConfigGrade)
        self.assertEqual(self.report()["grade_run"]["result"]["calls"][1]["status"], "interrupted")

    def test_async_submission_reports_progress_and_rejects_duplicate_runs(self):
        self.configure()
        started, release = threading.Event(), threading.Event()
        def delayed(*args):
            started.set()
            release.wait(5)
            return fake_call(*args)
        with patch.object(grader, "call_grader", side_effect=delayed):
            response = self.client.post(self.url + "/grade", headers={"Accept": "application/json"},
                data={"start_turn": 1, "end_turn": 1, "evidence_digest": self.report()["current_evidence_digest"]})
            try:
                self.assertEqual(response.status_code, 202)
                self.assertTrue(started.wait(2))
                progress = self.client.get(response.json()["status_url"])
                self.assertEqual(progress.status_code, 200)
                self.assertEqual(progress.json()["status"], "running")
                self.assertEqual(progress.json()["completed_batches"], 0)
                self.assertEqual(self.run_grade().status_code, 429)
            finally:
                release.set()
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    with grader.ACTIVE_RUNS_LOCK:
                        if not grader.ACTIVE_RUNS:
                            break
                    time.sleep(.02)
        self.assertEqual(self.client.get(response.json()["status_url"]).json()["status"], "completed")
        self.assertEqual(self.report()["grade_run"]["status"], "completed")

    def test_source_change_during_call_marks_frozen_result_stale_and_holds_no_write_lock(self):
        self.configure()
        def change_evidence(*args):
            with connect(self.settings.database_path) as connection, write_transaction(connection):
                connection.execute("UPDATE events SET display_text = 'changed' WHERE session_id = 'assess-session' AND event_index = 3")
            return fake_call(*args)
        with patch.object(grader, "call_grader", side_effect=change_evidence):
            self.assertEqual(self.run_grade().status_code, 303)
        self.assertTrue(self.report()["grade_run"]["stale"])

    def test_limits_key_and_cross_origin_guard(self):
        self.configure()
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            grader.save_api_key(connection, self.settings.data_dir, "")
        self.assertEqual(self.run_grade().status_code, 409)
        self.configure(processing="local", base_url="http://127.0.0.1:1234/v1")
        grader.GRADER_LOCK.acquire()
        try:
            self.assertEqual(self.run_grade().status_code, 429)
        finally:
            grader.GRADER_LOCK.release()
        self.assertEqual(self.client.post(self.url + "/grade", headers={"Origin": "https://elsewhere.example"}).status_code, 403)

    def test_admin_configuration_and_personal_project_access(self):
        self.configure()
        self.client.close()
        self.settings.auth_mode = "proxy"
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            create_initial_admin(connection, username="admin", password="Password123!")
        self.client = TestClient(create_app(self.settings))
        self.client.headers["X-Forwarded-User"] = "alice"
        self.assertNotIn('id="settings-llm"', self.client.get("/settings").text)
        self.assertEqual(self.client.get("/settings/grader").status_code, 403)
        self.assertEqual(self.client.post("/settings/grader", data={}).status_code, 403)
        with patch.object(grader, "call_grader", side_effect=fake_call):
            self.assertEqual(self.run_grade().status_code, 303)
        run_id = self.report()["grade_run"]["id"]
        self.client.headers["X-Forwarded-User"] = "bob"
        self.assertIsNone(self.report()["grade_run"])
        self.assertEqual(self.client.get(self.url + f"/grader/{run_id}.json").status_code, 404)
        self.assertEqual(self.client.get(self.url + f"/grader/{run_id}/status").status_code, 404)
        self.assertEqual(self.client.post(self.url + f"/grader/{run_id}/cancel").status_code, 404)
        self.assertEqual(self.run_grade(retry_run_id=str(run_id)).status_code, 404)
        with connect(self.settings.database_path) as connection, write_transaction(connection):
            connection.execute("UPDATE projects SET visibility = 'private'")
        with patch.object(grader, "call_grader") as call:
            self.assertEqual(self.client.post(self.url + "/grade", data={"start_turn": 1, "end_turn": 1}).status_code, 404)
            call.assert_not_called()
        self.assertEqual(self.client.get(self.url + f"/grader/{run_id}.json").status_code, 404)
        self.assertEqual(self.client.get(self.url + f"/grader/{run_id}/status").status_code, 404)

    def test_configuration_validation_and_key_not_in_pages_or_repr(self):
        migrated = grader.validate_config({**grader.DEFAULT_CONFIG, 'response_format': 'json_object'})
        self.assertEqual(migrated['response_format'], 'json_schema')
        self.assertNotIn("secret-test-key", repr(self.settings))
        legacy = self.client.get("/settings/grader", follow_redirects=False)
        self.assertEqual(legacy.status_code, 303)
        self.assertEqual(legacy.headers["location"], "/settings#settings-llm")
        settings_page = self.client.get("/settings")
        self.assertEqual(settings_page.status_code, 200)
        self.assertIn('id="settings-server-policy"', settings_page.text)
        self.assertIn('id="settings-llm"', settings_page.text)
        self.assertIn('name="base_url"', settings_page.text)
        self.assertNotIn("secret-test-key", settings_page.text)
        self.assertEqual(settings_page.headers["cache-control"], "private, no-store")
        self.assertNotIn("secret-test-key", self.client.get("/settings/grader").text)
        for fields in ({"base_url": "http://external.example/v1"}, {"base_url": "https://user:secret@example.com/v1"},
                       {"processing": "local", "base_url": "https://example.com/v1"}, {"max_output_tokens": True},
                       {"max_input_chars": 0}, {"base_url": "https://example.com/v1?key=secret"}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                grader.validate_config({**grader.DEFAULT_CONFIG, **fields})
        config = {**grader.DEFAULT_CONFIG, "enabled": "on", "model": "test-grader"}
        response = self.client.post("/settings/grader", data=config, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/settings#settings-llm")
        self.assertTrue(self.report()["grader_config"]["enabled"])

    def test_settings_key_lifecycle_is_encrypted_persistent_and_applies_immediately(self):
        config = {**grader.DEFAULT_CONFIG, "enabled": "on", "model": "test-grader"}
        def save(**fields):
            return self.client.post("/settings/grader", data={**config, **fields}, follow_redirects=False)
        self.assertEqual(save(api_key="replacement-key").status_code, 303)
        with connect(self.settings.database_path) as connection:
            self.assertEqual(grader.load_api_key(connection, self.settings.data_dir), "replacement-key")
            stored = connection.execute("SELECT value FROM server_settings WHERE key = 'llm_grader_api_key'").fetchone()["value"]
            self.assertNotIn("replacement-key", stored)
        self.assertEqual((self.settings.data_dir / ".grader-encryption-key").stat().st_mode & 0o777, 0o600)
        self.assertEqual(save(api_key="").status_code, 303)
        self.client.close()
        self.client = TestClient(create_app(self.settings))
        with patch.object(grader, "call_grader", side_effect=fake_call) as call:
            self.assertEqual(self.run_grade().status_code, 303)
            self.assertEqual(call.call_args_list[0].args[1], "replacement-key")
        page = self.client.get("/settings/grader").text
        self.assertIn("API key: Configured", page)
        self.assertNotIn("replacement-key", page)
        self.assertNotIn("replacement-key", json.dumps(self.report()))
        self.assertEqual(save(remove_api_key="on").status_code, 303)
        self.assertIn("API key: Not configured", self.client.get("/settings/grader").text)
        with patch.object(grader, "call_grader") as call:
            self.assertEqual(self.run_grade().status_code, 409)
            call.assert_not_called()

    def test_invalid_or_cross_origin_settings_do_not_change_saved_key(self):
        config = {**grader.DEFAULT_CONFIG, "enabled": "on", "model": "test-grader"}
        for fields in ({"api_key": "replacement", "max_input_chars": 0},
                       {"api_key": "replacement", "remove_api_key": "on"}, {"api_key": "bad\nkey"}):
            response = self.client.post("/settings/grader", data={**config, **fields})
            self.assertEqual(response.status_code, 400)
            self.assertIn('id="settings-server-policy"', response.text)
            self.assertIn('id="settings-llm"', response.text)
            self.assertIn('role="alert"', response.text)
            self.assertNotIn('value="replacement"', response.text)
        self.assertEqual(self.client.post("/settings/grader", data={**config, "api_key": "replacement"},
                         headers={"Origin": "https://elsewhere.example"}).status_code, 403)
        with connect(self.settings.database_path) as connection:
            self.assertEqual(grader.load_api_key(connection, self.settings.data_dir), "secret-test-key")

    def test_missing_encryption_key_blocks_grading_without_leaking_credentials(self):
        self.configure()
        (self.settings.data_dir / ".grader-encryption-key").unlink()
        with patch.object(grader, "call_grader") as call:
            response = self.run_grade()
            self.assertEqual(response.status_code, 400)
            self.assertIn("encryption key is unavailable", response.text)
            self.assertNotIn("secret-test-key", response.text)
            call.assert_not_called()


class BatchPlanningTests(unittest.TestCase):
    def context_report(self):
        return {'evidence': [
            {'event_index': 1, 'kind': 'message', 'role': 'user', 'display_text': '<environment_context>metadata</environment_context>'},
            {'event_index': 2, 'kind': 'message', 'role': 'user', 'display_text': 'Find the provider settings and explain how to open the UI.'},
            {'event_index': 3, 'kind': 'tool_call', 'tool_name': 'exec', 'display_text': 'rg settings routes.py'},
            {'event_index': 4, 'kind': 'tool_result', 'display_text': 'routes.py:20: settings route\n' * 1000},
            {'event_index': 5, 'kind': 'message', 'role': 'user', 'display_text': 'Correction: use the Settings page, not the old standalone page.'},
            {'event_index': 6, 'kind': 'tool_result', 'display_text': '漢字 correction verification\n' * 600},
            {'event_index': 7, 'kind': 'message', 'role': 'user', 'display_text': 'Future unrelated request'},
        ], 'turns': [{'turn_number': 1, 'start_event_index': 1}, {'turn_number': 2, 'start_event_index': 5},
                     {'turn_number': 3, 'start_event_index': 7}], 'metrics': {'configurations': []}}

    def test_batches_carry_requests_corrections_and_action_without_future_context(self):
        config = {**grader.DEFAULT_CONFIG, 'max_input_chars': 2000}
        inputs = grader.grader_input(self.context_report(), 'Explain navigation and admin prerequisites.', config)
        self.assertGreater(len(inputs['demand_batches']), 3)
        for batch in inputs['demand_batches']:
            self.assertLessEqual(grader.json_size(batch), 2000)
            self.assertEqual(batch['acceptance_criteria'], 'Explain navigation and admin prerequisites.')
            context = batch['task_context']
            refs = [request['event_index'] for request in context['requests']]
            self.assertIn(2, refs)
            self.assertNotIn(1, refs)  # Environment wrapper is not a task request.
            self.assertNotIn('metadata', json.dumps(context))
            if context['current_turn'] == 1:
                self.assertEqual(refs, [2])
            if context['current_turn'] == 2:
                self.assertEqual(refs, [2, 5])
                self.assertNotIn('Future unrelated', json.dumps(context))
            if batch['events'][0]['event_index'] == 4:
                self.assertEqual(context['preceding_action']['event_index'], 3)
                self.assertIn('rg settings', context['preceding_action']['text'])
        parts = [event for b in inputs['demand_batches'] for event in b['events'] if event['event_index'] == 4]
        self.assertTrue(all(event['kind'] == 'tool_result' for event in parts))
        restored = json.loads(''.join(event['event_json_fragment'] for event in parts))
        self.assertEqual(restored['text'], self.context_report()['evidence'][3]['display_text'])

    def test_long_unicode_context_is_marked_and_does_not_truncate_original_evidence(self):
        report = self.context_report()
        report['evidence'][1]['display_text'] = '請修復這個問題。' * 400
        inputs = grader.grader_input(report, '', {**grader.DEFAULT_CONFIG, 'max_input_chars': 3000})
        for batch in inputs['demand_batches']:
            self.assertLessEqual(grader.json_size(batch), 3000)
            original = batch['task_context']['requests'][0]
            self.assertTrue(original['truncated'])
            self.assertTrue(report['evidence'][1]['display_text'].startswith(original['text']))
        pieces = [e for batch in inputs['demand_batches'] for e in batch['events'] if e['event_index'] == 2]
        self.assertEqual(json.loads(''.join(e['event_json_fragment'] for e in pieces))['text'], report['evidence'][1]['display_text'])

    def test_citations_accept_supplied_context_but_reject_unseen_events(self):
        data = {'events': [{'event_index': 10}], 'task_context': {'requests': [{'event_index': 2, 'text': 'Request'}],
                                                               'preceding_action': {'event_index': 9, 'text': 'Command'}}}
        for citation, valid in ((2, True), (9, True), (3, False)):
            def cited(*args):
                response = fake_call(*args)
                choice = response['response']['choices'][0]
                output = json.loads(choice['message']['content'])
                output['findings'][0]['event_indexes'] = [citation]
                choice['message']['content'] = json.dumps(output)
                return response
            with self.subTest(citation=citation), patch.object(grader, 'call_grader', side_effect=cited):
                if valid:
                    grader.grade_stage('demand', grader.DEMAND_PROMPT, grader.DemandGrade, data, grader.DEFAULT_CONFIG, None, {'calls': []}, lambda: None)
                else:
                    with self.assertRaisesRegex(grader.GraderError, 'outside this task batch'):
                        grader.grade_stage('demand', grader.DEMAND_PROMPT, grader.DemandGrade, data, grader.DEFAULT_CONFIG, None, {'calls': []}, lambda: None)

    def test_excess_context_requests_are_explicit_and_keep_initial_and_current(self):
        brief = [{'event_index': n, 'turn_number': n, 'text': f'Correction {n}'} for n in range(1, 7)]
        complete = grader.task_context(brief, 5, 3000)
        self.assertEqual([e['event_index'] for e in complete['requests']], [1, 2, 3, 4, 5])
        self.assertEqual(complete['omitted_requests'], 0)
        requests = [{'event_index': n, 'turn_number': n, 'text': 'Request ' + str(n) + 'x' * 1000} for n in range(1, 15)]
        context = grader.task_context(requests, 10, 400)
        self.assertEqual(context['requests'][0]['event_index'], 1)
        self.assertEqual(context['requests'][-1]['event_index'], 10)
        self.assertEqual(context['omitted_requests'], 10 - len(context['requests']))
        self.assertLessEqual(grader.json_size(context), 400)

    def test_cleanup_keeps_instructions_patch_checks_and_legitimate_repetition(self):
        patch_text = '*** Begin Patch\n+return a + b\n*** End Patch'
        events = [
            {'event_index': 1, 'kind': 'system', 'role': 'developer', 'payload_type': 'message', 'display_text': 'Run tests'},
            {'event_index': 2, 'kind': 'system', 'role': 'developer', 'payload_type': 'message', 'display_text': 'Run tests'},
            {'event_index': 3, 'kind': 'message', 'role': 'user', 'display_text': 'Fix it', 'detail_text': 'Fix it'},
            {'event_index': 4, 'kind': 'tool_call', 'display_text': patch_text, 'detail_text': json.dumps(patch_text)},
            {'event_index': 5, 'kind': 'system', 'payload_type': 'item_completed', 'detail_text': json.dumps({'type': 'FileChange', 'diff': patch_text})},
            {'event_index': 6, 'kind': 'tool_result', 'detail_text': json.dumps([{'type': 'input_text', 'text': json.dumps({
                'chunk_id': 'transport-only', 'output': '1 passed', 'exit_code': 0, 'wall_time_seconds': 1})}])},
            {'event_index': 7, 'kind': 'message', 'role': 'assistant', 'display_text': 'Done'},
            {'event_index': 8, 'kind': 'message', 'role': 'user', 'display_text': 'Fix it'},
        ]
        cleaned = grader.clean_events(events + [events[-1]])
        self.assertEqual([e['event_index'] for e in cleaned], [1, 3, 4, 6, 7, 8])
        self.assertEqual(cleaned[2]['text'], patch_text)
        self.assertEqual(cleaned[3]['text'], '1 passed\nExit code: 0')
        self.assertNotIn('transport-only', json.dumps(cleaned))

    def test_cleanup_keeps_standalone_patch_results_and_completion_evidence(self):
        events = [
            {'event_index': 1, 'kind': 'system', 'payload_type': 'patch_apply_end', 'display_text': 'Status: failed', 'detail_text': 'Could not apply patch'},
            {'event_index': 2, 'kind': 'system', 'payload_type': 'item_completed', 'detail_text': json.dumps({'type': 'FileChange', 'diff': '+ standalone patch'})},
            {'event_index': 3, 'kind': 'system', 'payload_type': 'task_complete', 'display_text': 'Work finished'},
            {'event_index': 4, 'kind': 'system', 'role': 'developer', 'display_text': 'Verify all changes'},
            {'event_index': 5, 'kind': 'system', 'payload_type': 'item_completed', 'detail_text': json.dumps({
                'type': 'CommandExecution', 'command': ['pytest', '-q'], 'aggregated_output': '2 failed',
                'formatted_output': '2 failed', 'stdout': '2 failed', 'exit_code': 1})},
        ]
        cleaned = grader.clean_events(events)
        self.assertEqual([e['event_index'] for e in cleaned], [1, 2, 3, 4, 5])
        self.assertIn('Could not apply patch', cleaned[0]['text'])
        self.assertEqual(cleaned[4]['text'].count('2 failed'), 1)
        self.assertEqual(cleaned[4]['exit_code'], 1)

    def test_unicode_batches_fit_combined_context_even_with_large_character_limit(self):
        config = {**grader.DEFAULT_CONFIG, 'max_input_chars': 200000, 'max_output_tokens': 16000}
        report = {'evidence': [{'event_index': 1, 'kind': 'message', 'display_text': '漢😀' * 6000}],
                  'metrics': {'configurations': []}, 'turns': [{'turn_number': 1, 'start_event_index': 1}]}
        inputs = grader.grader_input(report, '', config)
        limit = grader.evidence_budget(config, grader.DEMAND_PROMPT, grader.DemandGrade)
        self.assertGreater(len(inputs['demand_batches']), 1)
        for batch in inputs['demand_batches']:
            self.assertLessEqual(grader.json_size(batch), limit)
        with patch.object(grader.http.client.HTTPConnection, 'connect') as connect_mock:
            with self.assertRaisesRegex(grader.GraderError, '32K context'):
                grader.call_grader(config, None, grader.DEMAND_PROMPT, {'events': report['evidence']}, grader.DemandGrade)
            connect_mock.assert_not_called()

    def test_citations_cannot_reference_evidence_in_another_batch(self):
        data = {"events": [{"event_index": number, "display_text": "x" * 500} for number in (1, 2)],
                "acceptance_criteria": "", "limitations": "Text only"}
        batches = grader.batch_evidence(data, [{"turn_number": number, "start_event_index": number} for number in (1, 2)], 1000)
        self.assertEqual(len(batches), 2)
        def wrong_batch(*args):
            response = fake_call(*args)
            output = json.loads(response["response"]["choices"][0]["message"]["content"])
            output["findings"][0]["event_indexes"] = [2]
            response["response"]["choices"][0]["message"]["content"] = json.dumps(output)
            return response
        result = {"calls": []}
        with patch.object(grader, "call_grader", side_effect=wrong_batch), self.assertRaises(grader.GraderError):
            grader.grade({"demand_batches": batches, "configuration": {"observations": []}}, grader.DEFAULT_CONFIG, None, result)
        self.assertEqual(result["batches"][0]["status"], "failed")
        self.assertNotIn("demand", result["batches"][0])
        self.assertEqual(result["batches"][1]["status"], "pending")

    def test_fragments_reconstruct_unicode_and_escaped_evidence_exactly(self):
        source = {"event_index": 1, "display_text": '漢字 "quoted" \\ newline\n' * 300, "detail_text": "tail"}
        data = {"acceptance_criteria": "Review the full workflow", "events": [source], "limitations": "Text only"}
        batches = grader.batch_evidence(data, [{"turn_number": 1, "start_event_index": 1}], 1000)
        fragments = [event for batch in batches for event in batch["events"]]
        self.assertGreater(len(fragments), 1)
        reconstructed = ""
        for part in fragments:
            self.assertEqual(part["fragment"]["start"], len(reconstructed))
            reconstructed += part["event_json_fragment"]
            self.assertEqual(part["fragment"]["end"], len(reconstructed))
        self.assertEqual(json.loads(reconstructed), {**source, "turn_number": 1})
        self.assertTrue(all(len(grader.canonical_json(batch)) <= 1000 for batch in batches))
        self.assertEqual(batches, grader.batch_evidence(data, [{"turn_number": 1, "start_event_index": 1}], 1000))

    def test_turn_boundaries_and_batch_limit(self):
        events = [{"event_index": number, "display_text": "x" * 250} for number in range(1, 7)]
        turns = [{"turn_number": number, "start_event_index": 2 * number - 1} for number in range(1, 4)]
        data = {"events": events, "acceptance_criteria": "", "limitations": "Text only"}
        batches = grader.batch_evidence(data, turns, 1000)
        self.assertEqual([[event["event_index"] for event in batch["events"]] for batch in batches], [[1, 2], [3, 4], [5, 6]])
        with patch.object(grader, "MAX_BATCHES", 2), self.assertRaisesRegex(ValueError, "more than 2 batches"):
            grader.batch_evidence(data, turns, 1000)

    def test_configuration_is_bounded_independently_from_demand(self):
        report = {"evidence": [{"event_index": 1, "display_text": "x" * 200}],
                  "metrics": {"configurations": [{"model": "model-a", "effort": "high", "event_index": 1}] * 100},
                  "turns": [{"turn_number": 1, "start_event_index": 1}]}
        inputs = grader.grader_input(report, "", {**grader.DEFAULT_CONFIG, "max_input_chars": 1000})
        self.assertIn("demand", inputs)
        self.assertLessEqual(len(grader.canonical_json(inputs["configuration"])), 1000)
        self.assertEqual(inputs["configuration"]["observations"], [{"model": "model-a", "effort": "high"}])


class GraderTransportTests(unittest.TestCase):
    def test_timeout_and_cancel_close_socket_before_headers_and_during_body(self):
        for headers_sent, reason in ((False, 'cancelled'), (True, 'cancelled'), (False, 'timed out'), (True, 'timed out')):
            with self.subTest(headers_sent=headers_sent, reason=reason):
                received, disconnected = threading.Event(), threading.Event()
                class Handler(BaseHTTPRequestHandler):
                    def do_POST(self):
                        self.rfile.read(int(self.headers['Content-Length']))
                        if headers_sent:
                            self.send_response(200)
                            self.send_header('Content-Length', '1000')
                            self.end_headers()
                            self.wfile.write(b'{')
                            self.wfile.flush()
                        received.set()
                        self.connection.settimeout(3)
                        if self.connection.recv(1) == b'':
                            disconnected.set()
                    def log_message(self, *args):
                        pass
                server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                control = grader.RunControl()
                grader.WORKER_STATE.control = control
                def cancel_after_request():
                    if received.wait(2):
                        control.cancel(reason)
                canceller = threading.Thread(target=cancel_after_request)
                canceller.start()
                try:
                    config = {**grader.DEFAULT_CONFIG, 'base_url': f'http://127.0.0.1:{server.server_port}/v1'}
                    started = time.monotonic()
                    with self.assertRaisesRegex(grader.GraderError, reason):
                        grader.call_grader(config, None, grader.CONFIG_PROMPT, {'observations': []}, grader.ConfigGrade)
                    self.assertLess(time.monotonic() - started, 2)
                    self.assertTrue(disconnected.wait(1))
                finally:
                    del grader.WORKER_STATE.control
                    canceller.join()
                    server.shutdown()
                    server.server_close()
                    thread.join()

    def test_absolute_timeout_fires_without_retry(self):
        disconnected = threading.Event()
        requests = []
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                requests.append(self.path)
                self.connection.settimeout(3)
                if self.connection.recv(1) == b'':
                    disconnected.set()
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            config = {**grader.DEFAULT_CONFIG, 'base_url': f'http://127.0.0.1:{server.server_port}/v1', 'timeout_seconds': .1}
            with self.assertRaisesRegex(grader.GraderError, 'timed out'):
                grader.call_grader(config, None, grader.CONFIG_PROMPT, {'observations': []}, grader.ConfigGrade)
            self.assertTrue(disconnected.wait(1))
            self.assertEqual(len(requests), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_http_contract_and_redirects_do_not_forward_trace_or_key(self):
        requests = []
        redirect = False
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append((self.path, self.headers.get("Authorization"), body))
                if redirect:
                    self.send_response(302)
                    self.send_header("Location", "/leaked")
                    self.end_headers()
                    return
                payload = fake_call({}, None, "", {"observations": []}, grader.ConfigGrade)["response"]
                raw = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            config = {**grader.DEFAULT_CONFIG, "model": "test-model", "base_url": f"http://127.0.0.1:{server.server_port}/v1"}
            response = grader.call_grader(config, "test-secret", "prompt", {"observations": []}, grader.ConfigGrade)
            self.assertEqual(response["response"]["model"], "test-grader")
            self.assertEqual(requests[0][0], "/v1/chat/completions")
            self.assertEqual(requests[0][1], "Bearer test-secret")
            self.assertFalse(requests[0][2]["store"])
            self.assertTrue(requests[0][2]["response_format"]["json_schema"]["strict"])
            self.assertEqual(requests[0][2]["max_completion_tokens"], 512)
            self.assertEqual(requests[0][2]["chat_template_kwargs"], {"enable_thinking": False})
            redirect = True
            with self.assertRaises(grader.GraderError) as error:
                grader.call_grader(config, "test-secret", "private trace", {}, grader.ConfigGrade)
            self.assertNotIn("test-secret", str(error.exception))
            self.assertEqual(len(requests), 2)
            self.assertTrue(all(path == "/v1/chat/completions" for path, _, _ in requests))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
