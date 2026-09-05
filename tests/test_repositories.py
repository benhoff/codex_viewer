from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from agent_operations_viewer.db import connect, init_db, write_transaction
from agent_operations_viewer.repositories import (
    list_repository_projects,
    normalize_repository_root,
    repository_registry_needs_sync,
    resolve_repository_id,
    sync_repository_registry,
)
from tests.test_search import insert_search_turn


class RepositoryRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "viewer.sqlite3"
        init_db(self.database_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _insert(
        self,
        connection: object,
        *,
        session_id: str,
        project_id: str,
        project_key: str,
        host: str,
        root: str,
        remote: str | None,
    ) -> None:
        insert_search_turn(
            connection,
            session_id=session_id,
            project_id=project_id,
            project_key=project_key,
            project_label=project_key,
            host=host,
            prompt="repository identity evidence",
        )
        connection.execute(
            "UPDATE sessions SET cwd = ?, git_repository_url = ? WHERE id = ?",
            (root, remote, session_id),
        )

    def test_ssh_and_https_histories_share_a_canonical_repository(self) -> None:
        with connect(self.database_path) as connection:
            with write_transaction(connection):
                self._insert(
                    connection,
                    session_id="laptop-session",
                    project_id="laptop-project",
                    project_key="project:laptop:/work/hws",
                    host="my-laptop",
                    root="/work/hws",
                    remote="git@github.com:benhoff/hws.git",
                )
                self._insert(
                    connection,
                    session_id="server-session",
                    project_id="server-project",
                    project_key="github:benhoff/hws",
                    host="server",
                    root="/srv/hws",
                    remote="https://github.com/benhoff/hws/",
                )
                sync_repository_registry(connection)

            session_rows = connection.execute(
                "SELECT id, repository_id FROM sessions ORDER BY id"
            ).fetchall()
            project_rows = connection.execute(
                "SELECT id, repository_id FROM projects ORDER BY id"
            ).fetchall()
            aliases = connection.execute(
                """
                SELECT alias_type, alias_value
                FROM repository_aliases
                ORDER BY alias_type, alias_value
                """
            ).fetchall()
            needs_sync = repository_registry_needs_sync(connection)

        repository_ids = {str(row["repository_id"]) for row in session_rows}
        self.assertEqual(len(repository_ids), 1)
        self.assertEqual(
            {str(row["repository_id"]) for row in project_rows},
            repository_ids,
        )
        self.assertIn(
            ("remote", "github.com/benhoff/hws"),
            [(str(row["alias_type"]), str(row["alias_value"])) for row in aliases],
        )
        self.assertFalse(needs_sync)

    def test_same_basename_does_not_merge_distinct_remotes(self) -> None:
        with connect(self.database_path) as connection:
            with write_transaction(connection):
                self._insert(
                    connection,
                    session_id="first",
                    project_id="first-project",
                    project_key="git:github.com/alpha/service",
                    host="builder",
                    root="/work/alpha/service",
                    remote="https://github.com/alpha/service.git",
                )
                self._insert(
                    connection,
                    session_id="second",
                    project_id="second-project",
                    project_key="git:gitlab.example.com/beta/service",
                    host="builder",
                    root="/work/beta/service",
                    remote="ssh://git@gitlab.example.com/beta/service.git",
                )
                sync_repository_registry(connection)
            rows = connection.execute(
                "SELECT repository_id FROM sessions ORDER BY id"
            ).fetchall()

        self.assertEqual(len({str(row["repository_id"]) for row in rows}), 2)

    def test_local_fallback_is_scoped_by_host_and_normalized_root(self) -> None:
        with connect(self.database_path) as connection:
            with write_transaction(connection):
                self._insert(
                    connection,
                    session_id="linux",
                    project_id="linux-project",
                    project_key="project:linux:/work/repo",
                    host="linux",
                    root="/work//repo/",
                    remote=None,
                )
                self._insert(
                    connection,
                    session_id="windows",
                    project_id="windows-project",
                    project_key="project:windows:C:/work/repo",
                    host="windows",
                    root="C:\\WORK\\Repo\\",
                    remote=None,
                )
                sync_repository_registry(connection)
            rows = connection.execute(
                "SELECT repository_id FROM sessions ORDER BY id"
            ).fetchall()

        self.assertEqual(normalize_repository_root("/work//repo/"), "/work/repo")
        self.assertEqual(normalize_repository_root("C:\\WORK\\Repo\\"), "c:/work/repo")
        self.assertEqual(len({str(row["repository_id"]) for row in rows}), 2)

    def test_ambiguous_manual_project_stays_unlinked_without_repeated_sync(self) -> None:
        with connect(self.database_path) as connection:
            with write_transaction(connection):
                self._insert(
                    connection,
                    session_id="first-manual",
                    project_id="manual-project",
                    project_key="manual:first",
                    host="builder",
                    root="/work/first",
                    remote="https://github.com/acme/first.git",
                )
                self._insert(
                    connection,
                    session_id="second-manual",
                    project_id="manual-project",
                    project_key="manual:second",
                    host="builder",
                    root="/work/second",
                    remote="https://github.com/acme/second.git",
                )
                sync_repository_registry(connection)
            project = connection.execute(
                "SELECT repository_id FROM projects WHERE id = 'manual-project'"
            ).fetchone()
            listing = list_repository_projects(connection)
            needs_sync = repository_registry_needs_sync(connection)

        self.assertIsNone(project["repository_id"])
        self.assertEqual(len(listing["items"][0]["repository_ids"]), 2)
        self.assertIsNone(listing["items"][0]["repository"])
        self.assertFalse(needs_sync)

    def test_late_remote_identity_redirects_the_location_id(self) -> None:
        with connect(self.database_path) as connection:
            with write_transaction(connection):
                self._insert(
                    connection,
                    session_id="late-remote",
                    project_id="late-project",
                    project_key="project:builder:/work/repo",
                    host="builder",
                    root="/work/repo",
                    remote=None,
                )
                sync_repository_registry(connection)
                old_id = str(
                    connection.execute(
                        "SELECT repository_id FROM sessions WHERE id = 'late-remote'"
                    ).fetchone()["repository_id"]
                )
                connection.execute(
                    "UPDATE sessions SET git_repository_url = ? WHERE id = 'late-remote'",
                    ("https://github.com/acme/repo.git",),
                )
                self.assertTrue(repository_registry_needs_sync(connection))
                sync_repository_registry(connection)
                new_id = str(
                    connection.execute(
                        "SELECT repository_id FROM sessions WHERE id = 'late-remote'"
                    ).fetchone()["repository_id"]
                )

            redirected = resolve_repository_id(connection, old_id)

        self.assertNotEqual(old_id, new_id)
        self.assertEqual(redirected, new_id)


if __name__ == "__main__":
    unittest.main()
