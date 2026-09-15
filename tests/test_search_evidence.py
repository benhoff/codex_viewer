import json
import unittest

from agent_operations_viewer.search_evidence import annotate_turn, output_record


def row(index, kind, payload_type, call_id=None, payload=None, role=None):
    return dict(event_index=index, kind=kind, payload_type=payload_type, call_id=call_id,
                role=role, record_json=json.dumps({'payload': payload or {}}))


class SearchEvidenceTests(unittest.TestCase):
    def test_availability_does_not_infer_completeness_from_output(self):
        cases = [({}, 'unknown', None), ({'output': None}, 'unknown', None),
                 ({'output': ''}, 'captured_empty', ''), ({'output': ' '}, 'captured', ' '),
                 ({'output': 'truncated output'}, 'captured', 'truncated output'),
                 ({'output': 'partial', 'output_truncated': True}, 'truncated', 'partial'),
                 ({'output_available': False}, 'unavailable', None)]
        for payload, state, text in cases:
            with self.subTest(payload=payload):
                result = output_record(payload, 'function_call_output')
                self.assertEqual(result['availability'], state)
                self.assertEqual(result['decoded_text'], text)
                if state != 'truncated':
                    self.assertEqual(result['completeness'], 'unknown')
        self.assertEqual(output_record({'output': '', 'output_complete': True}, 'function_call_output')['completeness'], 'complete')

    def test_decoding_preserves_representation_and_whitespace(self):
        value = json.dumps({'content': [{'type': 'text', 'text': 'one\n'}, {'type': 'text', 'text': 'two'}]})
        result = output_record({'output': value}, 'function_call_output')
        self.assertEqual(result['representation'], value)
        self.assertEqual(result['decoded_text'], 'one\n\ntwo')
        self.assertEqual(result['decoding'], 'json/text_blocks')
        mixed = [{'type': 'text', 'text': 'partial'}, {'type': 'image', 'data': 'abc'}]
        result = output_record({'output': mixed}, 'function_call_output')
        self.assertIsNone(result['decoded_text'])
        self.assertEqual(result['representation'], mixed)
        result = output_record({'output': {'text': '', 'image': 'captured-image'}}, 'function_call_output')
        self.assertEqual(result['availability'], 'captured')

    def test_linkage_empty_missing_reused_and_user_provenance(self):
        rows = [row(0, 'message', 'message', role='user'),
                row(1, 'tool_call', 'function_call', 'one'),
                row(2, 'tool_result', 'function_call_output', 'one', {'output': ''}),
                row(3, 'tool_call', 'function_call', 'missing'),
                row(4, 'tool_call', 'function_call', 'reused'),
                row(5, 'tool_call', 'function_call', 'reused'),
                row(6, 'tool_result', 'function_call_output', 'reused', {'output': 'done'})]
        turn = {'commands': [{'event_index': i, 'output': 'legacy'} for i in (1, 3, 4)],
                'activity': [{'event_index': 1}], 'patches': [], 'prompt': {}, 'response': {}}
        annotate_turn(turn, rows, 'session/a')
        empty, missing, ambiguous = turn['commands']
        self.assertEqual(empty['output'], '')
        self.assertEqual(empty['output_availability'], 'captured_empty')
        self.assertEqual(empty['result_event_ids'], ['session:session%2Fa:event:2'])
        self.assertEqual(turn['source_events'][2]['command_event_id'], empty['event_id'])
        self.assertEqual(missing['output_availability'], 'missing')
        self.assertEqual(ambiguous['output_availability'], 'unknown')
        self.assertEqual(ambiguous['result_event_ids'], [])
        self.assertEqual(turn['source_events'][0]['provenance'], 'user_message')

    def test_result_before_call_is_not_linked(self):
        turn = {'commands': [{'event_index': 2}], 'activity': [], 'patches': [], 'prompt': {}, 'response': {}}
        annotate_turn(turn, [row(1, 'tool_result', 'function_call_output', 'x', {'output': 'old'}),
                             row(2, 'tool_call', 'function_call', 'x')], 's')
        self.assertEqual(turn['commands'][0]['linkage'], 'unknown')

    def test_unanswered_shell_call_remains_in_commands(self):
        from agent_operations_viewer.web.routes.search_api import _serialize_turn
        call = {'event_index': 1, 'kind': 'tool_call', 'tool_name': 'exec_command',
                'call_id': 'pending', 'command_text': 'sleep 10'}
        turn = _serialize_turn({'number': 1, 'merged_detail_events': [call]},
                               target_turn_number=1, include_activity=True)
        annotate_turn(turn, [row(1, 'tool_call', 'function_call', 'pending')], 's')
        self.assertEqual(turn['commands'][0]['command'], 'sleep 10')
        self.assertEqual(turn['commands'][0]['output_availability'], 'missing')

    def test_only_structured_image_reference_is_exposed(self):
        turn = {'commands': [], 'activity': [], 'patches': [], 'prompt': {}, 'response': {}}
        annotate_turn(turn, [row(1, 'tool_result', 'image_generation_end', payload={'saved_path': '/unread/file.png'}),
                             row(2, 'message', 'message', payload={'saved_path': '/not/an/artifact'}, role='user')], 's')
        sources = turn['source_events']
        self.assertEqual(sources[0]['artifact_references'][0]['content_captured'], 'unknown')
        self.assertNotIn('artifact_references', sources[1])
