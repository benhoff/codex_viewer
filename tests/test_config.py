from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from agent_operations_viewer.config import Settings


class ConfigTests(unittest.TestCase):
    def test_from_env_prefers_legacy_database_after_repo_rename(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            data_dir = project_root / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            legacy_path = data_dir / "codex_sessions.sqlite3"
            legacy_path.write_text("", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                settings = Settings.from_env(project_root=project_root)

            self.assertEqual(settings.database_path, legacy_path)

    def test_from_env_prefers_renamed_database_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            data_dir = project_root / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            legacy_path = data_dir / "codex_sessions.sqlite3"
            legacy_path.write_text("", encoding="utf-8")
            renamed_path = data_dir / "agent_operations_viewer_sessions.sqlite3"
            renamed_path.write_text("", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                settings = Settings.from_env(project_root=project_root)

            self.assertEqual(settings.database_path, renamed_path)

    def test_from_env_honors_database_override(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            override_path = project_root / "custom.sqlite3"

            with patch.dict(os.environ, {"CODEX_VIEWER_DB": str(override_path)}, clear=True):
                settings = Settings.from_env(project_root=project_root)

            self.assertEqual(settings.database_path, override_path)


if __name__ == "__main__":
    unittest.main()
