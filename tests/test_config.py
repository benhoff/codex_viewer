from __future__ import annotations

import unittest

from codex_session_viewer.config import _normalize_auth_mode


class ConfigAuthModeTests(unittest.TestCase):
    def test_auth_mode_defaults_to_password(self) -> None:
        self.assertEqual(_normalize_auth_mode(None), "password")
        self.assertEqual(_normalize_auth_mode(""), "password")
        self.assertEqual(_normalize_auth_mode("   "), "password")

    def test_auth_mode_preserves_explicit_none(self) -> None:
        self.assertEqual(_normalize_auth_mode("none"), "none")

