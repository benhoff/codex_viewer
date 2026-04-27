from __future__ import annotations

from pathlib import Path


def read_session_source_text(source_path: Path) -> str:
    with source_path.open("r", encoding="utf-8", newline="") as handle:
        return handle.read()
