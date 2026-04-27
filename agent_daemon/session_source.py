from __future__ import annotations

from pathlib import Path


def read_session_source_text(source_path: Path) -> str:
    with source_path.open("r", encoding="utf-8", newline="") as handle:
        return handle.read()


def read_session_source_tail(source_path: Path, offset: int) -> str:
    with source_path.open("rb") as handle:
        handle.seek(max(0, offset))
        return handle.read().decode("utf-8")
