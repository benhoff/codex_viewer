from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from agent_operations_viewer.db import connect, init_db, write_transaction
from agent_operations_viewer.local_auth import create_initial_admin, create_local_user, set_user_disabled
from agent_operations_viewer.search_api_tokens import (
    create_search_api_token,
    delete_search_api_token,
    find_active_search_api_token,
    list_search_api_tokens,
    revoke_search_api_token,
    touch_search_api_token_usage,
)


class SearchApiTokenTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "viewer.sqlite3"
        init_db(self.db_path)
        with connect(self.db_path) as connection:
            with write_transaction(connection):
                self.admin = create_initial_admin(
                    connection,
                    username="admin",
                    password="Password123!",
                )
                self.viewer = create_local_user(
                    connection,
                    username="viewer",
                    password="Password123!",
                    role="viewer",
                )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_token_is_user_linked_scoped_and_only_returned_raw_once(self) -> None:
        with connect(self.db_path) as connection:
            with write_transaction(connection):
                created = create_search_api_token(
                    connection,
                    owner_user_id=str(self.viewer["id"]),
                    label="Reporting",
                )
            found = find_active_search_api_token(connection, str(created["token"]))
            listed = list_search_api_tokens(connection, str(self.viewer["id"]))

        self.assertIsNotNone(found)
        self.assertEqual(str(found["owner_user_id"]), str(self.viewer["id"]))
        self.assertEqual(created["scopes"], ["search:read"])
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["scopes"], ["search:read"])
        self.assertNotIn("token", listed[0])

    def test_revoke_and_delete_are_owner_scoped(self) -> None:
        with connect(self.db_path) as connection:
            with write_transaction(connection):
                created = create_search_api_token(
                    connection,
                    owner_user_id=str(self.viewer["id"]),
                    label="Viewer token",
                )
                self.assertFalse(
                    revoke_search_api_token(
                        connection,
                        owner_user_id=str(self.admin["id"]),
                        token_id=str(created["id"]),
                    )
                )
                self.assertTrue(
                    revoke_search_api_token(
                        connection,
                        owner_user_id=str(self.viewer["id"]),
                        token_id=str(created["id"]),
                    )
                )
            self.assertIsNone(find_active_search_api_token(connection, str(created["token"])))
            with write_transaction(connection):
                self.assertFalse(
                    delete_search_api_token(
                        connection,
                        owner_user_id=str(self.admin["id"]),
                        token_id=str(created["id"]),
                    )
                )
                self.assertTrue(
                    delete_search_api_token(
                        connection,
                        owner_user_id=str(self.viewer["id"]),
                        token_id=str(created["id"]),
                    )
                )

    def test_disabled_owner_cannot_authenticate(self) -> None:
        with connect(self.db_path) as connection:
            with write_transaction(connection):
                created = create_search_api_token(
                    connection,
                    owner_user_id=str(self.viewer["id"]),
                    label="Viewer token",
                )
                touch_search_api_token_usage(connection, str(created["id"]))
                set_user_disabled(connection, str(self.viewer["id"]), True)
            found = find_active_search_api_token(connection, str(created["token"]))
            listed = list_search_api_tokens(connection, str(self.viewer["id"]))

        self.assertIsNone(found)
        self.assertIsNotNone(listed[0]["last_used_at"])


if __name__ == "__main__":
    unittest.main()
