from __future__ import annotations

import unittest

from agent_operations_viewer.session_view import build_turn_review_focus


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


if __name__ == "__main__":
    unittest.main()
