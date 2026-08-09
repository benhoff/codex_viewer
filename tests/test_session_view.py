from __future__ import annotations

import unittest

from agent_operations_viewer.session_view import (
    build_trust_signals,
    build_turn_review_focus,
    merge_compound_tool_events,
    response_evidence,
)


class SessionViewTests(unittest.TestCase):
    def test_build_turn_review_focus_for_claim_mismatch(self) -> None:
        turn = {
            "number": 7,
            "response_excerpt": "Updated the styling and tests passed.",
            "audit_response_evidence": {
                "warnings": [
                    "Response says tests passed, but no verification command was recorded.",
                    "Response says files were updated, but only one patch was recorded.",
                ]
            },
            "audit_summary": {
                "command_count": 0,
                "patch_count": 1,
                "verification_count": 0,
                "files_touched_count": 2,
            },
        }

        review = build_turn_review_focus(turn, "claim_evidence_mismatch")

        self.assertIsNotNone(review)
        assert review is not None
        self.assertEqual(review["title"], "Response may overstate completed work")
        self.assertEqual(review["turn_number"], 7)
        self.assertEqual(review["warning_count"], 2)
        self.assertEqual(review["warning_count_label"], "2 mismatches detected")
        self.assertEqual(review["claim_excerpt"], "Updated the styling and tests passed.")
        self.assertEqual(review["actions"][0]["href"], "")
        self.assertEqual(review["actions"][1]["href"], "#turn-7-patches")
        self.assertEqual(review["actions"][2]["href"], "")
        self.assertEqual(review["actions"][3]["href"], "#turn-7-files")

    def test_build_turn_review_focus_returns_none_for_unsupported_review_kind(self) -> None:
        turn = {
            "number": 1,
            "audit_response_evidence": {
                "warnings": ["Response says tests passed, but no verification command was recorded."]
            },
            "audit_summary": {},
        }

        self.assertIsNone(build_turn_review_focus(turn, "verification_failed"))

    def test_claim_evidence_signal_links_to_evidence_block(self) -> None:
        signals = build_trust_signals(
            turn_number=8,
            verification_verdict_data={"status": "none"},
            response_evidence_data={"warning_count": 1},
            risky_files=[],
            command_events=[],
            merged_detail_events=[],
            mcp_events=[],
            context_shift_events=[],
            response_state="final",
            abort_reason=None,
            patch_count=0,
            files_touched_count=0,
        )

        mismatch_signal = next(signal for signal in signals if signal["key"] == "claim_evidence_mismatch")
        self.assertEqual(mismatch_signal["href"], "#turn-8-response-evidence")

    def test_response_evidence_skips_pure_discussion(self) -> None:
        evidence = response_evidence(
            turn_number=15,
            response_text=(
                "The best no-clone view is an activity index over paths agents changed.\n\n"
                "Route idea: /projects/<project>/files\n\n"
                "Evidence chips: patch, commands, verification passed\n\n"
                "The view should answer which files are becoming hot spots."
            ),
            command_events=[],
            patch_events=[],
            research_events=[],
            verification_verdict_data={"status": "none", "command_count": 0},
            file_manifest=[],
        )

        self.assertEqual(evidence["warnings"], [])
        self.assertEqual(evidence["warning_count"], 0)

    def test_response_evidence_still_flags_completion_claims_without_evidence(self) -> None:
        evidence = response_evidence(
            turn_number=7,
            response_text="Updated the styling and tests passed.",
            command_events=[],
            patch_events=[],
            research_events=[],
            verification_verdict_data={"status": "none", "command_count": 0},
            file_manifest=[],
        )

        self.assertEqual(evidence["warning_count"], 2)

    def test_exec_command_apply_patch_merges_as_patch_evidence(self) -> None:
        events = [
            {
                "kind": "tool_call",
                "tool_name": "exec_command",
                "call_id": "call-patch",
                "display_text": "apply_patch <<'PATCH'\n*** Begin Patch\n*** Update File: src/app.py\n@@\n-old\n+new\n*** End Patch",
                "command_text": "apply_patch <<'PATCH'",
                "detail_text": "",
            },
            {
                "kind": "system",
                "record_type": "event_msg",
                "payload_type": "patch_apply_end",
                "call_id": "call-patch",
                "display_text": "Status: completed (success)",
                "detail_text": (
                    '{"src/app.py":{"move_path":null,"type":"update",'
                    '"unified_diff":"@@ -1 +1 @@\\n-old\\n+new\\n"}}'
                ),
                "record_json": '{"payload":{"success":true,"status":"completed"}}',
            },
            {
                "kind": "tool_result",
                "call_id": "call-patch",
                "display_text": "Success. Updated the following files:\nM src/app.py",
                "detail_text": "Success. Updated the following files:\nM src/app.py",
            },
        ]

        merged = merge_compound_tool_events(events)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["group_key"], "patch")
        self.assertEqual(merged[0]["patch_file_count"], 1)
        self.assertEqual(merged[0]["patch_files"][0]["path"], "src/app.py")


if __name__ == "__main__":
    unittest.main()
