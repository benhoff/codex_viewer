from __future__ import annotations

from datetime import UTC, datetime
import unittest

from codex_session_viewer.action_queue_state import filter_action_queue_items


class ActionQueueStateTests(unittest.TestCase):
    def test_ignore_hides_item(self) -> None:
        items = [{"fingerprint": "fingerprint-1", "timestamp": "2026-04-20T03:19:19Z"}]
        state_by_fingerprint = {
            "fingerprint-1": {
                "status": "ignored",
                "updated_at": "2026-04-20T03:20:00Z",
            }
        }

        visible = filter_action_queue_items(items, state_by_fingerprint)

        self.assertEqual(visible, [])

    def test_snooze_hides_item_until_expiry(self) -> None:
        items = [{"fingerprint": "fingerprint-1", "timestamp": "2026-04-20T03:19:19Z"}]
        state_by_fingerprint = {
            "fingerprint-1": {
                "status": "snoozed",
                "snoozed_until": "2026-04-21T00:00:00+00:00",
                "updated_at": "2026-04-20T03:20:00Z",
            }
        }

        hidden = filter_action_queue_items(
            items,
            state_by_fingerprint,
            now=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
        )
        visible = filter_action_queue_items(
            items,
            state_by_fingerprint,
            now=datetime(2026, 4, 21, 12, 0, tzinfo=UTC),
        )

        self.assertEqual(hidden, [])
        self.assertEqual(visible, items)

    def test_resolved_hides_only_older_occurrence(self) -> None:
        items = [
            {"fingerprint": "fingerprint-1", "timestamp": "2026-04-20T03:19:19Z"},
            {"fingerprint": "fingerprint-1", "timestamp": "2026-04-21T03:19:19Z"},
        ]
        state_by_fingerprint = {
            "fingerprint-1": {
                "status": "resolved",
                "updated_at": "2026-04-20T12:00:00+00:00",
            }
        }

        visible = filter_action_queue_items(items, state_by_fingerprint)

        self.assertEqual(visible, [items[1]])


if __name__ == "__main__":
    unittest.main()
