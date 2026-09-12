from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from agent_operations_viewer.db import connect, connection_scope, init_db


class ConnectionScopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.path = Path(self.temp_dir.name) / "viewer.sqlite3"
        with connection_scope(self.path) as connection:
            connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")

    def assert_closed(self, connection: sqlite3.Connection) -> None:
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")

    def test_success_commits_and_closes_even_with_retained_references(self) -> None:
        retained = []
        for item in range(40):
            with connection_scope(self.path) as connection:
                connection.execute("INSERT INTO items VALUES (?)", (item,))
                retained.append(connection)
            self.assert_closed(connection)
        with connection_scope(self.path) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM items").fetchone()[0], 40)

    def test_body_failure_rolls_back_and_closes(self) -> None:
        with self.assertRaisesRegex(ValueError, "failed work"):
            with connection_scope(self.path) as connection:
                connection.execute("INSERT INTO items VALUES (1)")
                raise ValueError("failed work")
        self.assert_closed(connection)
        with connection_scope(self.path) as reader:
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM items").fetchone()[0], 0)

    def test_commit_failure_rolls_back_and_closes(self) -> None:
        with connection_scope(self.path) as connection:
            connection.execute(
                "CREATE TABLE children (parent_id INTEGER REFERENCES items(id) "
                "DEFERRABLE INITIALLY DEFERRED)"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            with connection_scope(self.path) as connection:
                connection.execute("INSERT INTO children VALUES (99)")
        self.assert_closed(connection)
        with connection_scope(self.path) as reader:
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM children").fetchone()[0], 0)

    def test_inner_transaction_does_not_close_outer_scope(self) -> None:
        with connection_scope(self.path) as connection:
            with connection:
                connection.execute("INSERT INTO items VALUES (1)")
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM items").fetchone()[0], 1)
            connection.execute("INSERT INTO items VALUES (2)")
        self.assert_closed(connection)
        with connection_scope(self.path) as reader:
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM items").fetchone()[0], 2)

    def test_failed_configuration_closes_connection(self) -> None:
        class FailingConnection(sqlite3.Connection):
            def execute(self, sql, *args, **kwargs):
                if sql == "PRAGMA synchronous = NORMAL":
                    raise sqlite3.OperationalError("configuration failed")
                return super().execute(sql, *args, **kwargs)

        connection = sqlite3.connect(self.path, factory=FailingConnection)
        self.addCleanup(connection.close)
        with mock.patch("agent_operations_viewer.db.sqlite3.connect", return_value=connection):
            with self.assertRaisesRegex(sqlite3.OperationalError, "configuration failed"):
                connect(self.path)
        self.assert_closed(connection)

    def test_startup_closes_schema_and_backfill_connections(self) -> None:
        retained = []

        def track_connect(path):
            connection = connect(path)
            retained.append(connection)
            return connection

        with mock.patch("agent_operations_viewer.db.connect", side_effect=track_connect):
            init_db(self.path)
        self.assertGreaterEqual(len(retained), 2)
        for connection in retained:
            self.assert_closed(connection)


if __name__ == "__main__":
    unittest.main()
