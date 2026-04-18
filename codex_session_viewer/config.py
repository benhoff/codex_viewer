from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path

from . import SYNC_API_VERSION, __version__


def _split_roots(raw: str | None) -> list[Path]:
    if not raw:
        return [Path.home() / ".codex" / "sessions"]
    parts = [item.strip() for item in raw.split(",")]
    roots = [Path(item).expanduser() for item in parts if item.strip()]
    return roots or [Path.home() / ".codex" / "sessions"]


def _clean_url(raw: str | None) -> str | None:
    if not raw:
        return None
    stripped = raw.strip().rstrip("/")
    return stripped or None


@dataclass(slots=True)
class Settings:
    project_root: Path
    data_dir: Path
    database_path: Path
    session_roots: list[Path]
    sync_mode: str
    app_version: str
    sync_api_version: str
    expected_agent_version: str
    minimum_agent_version: str
    agent_update_command: str | None
    sync_on_start: bool
    page_size: int
    server_host: str
    server_port: int
    server_base_url: str | None
    sync_api_token: str | None
    sync_interval_seconds: int
    remote_timeout_seconds: int
    log_level: str
    source_host: str

    @classmethod
    def from_env(cls, project_root: Path | None = None) -> "Settings":
        root = (project_root or Path(__file__).resolve().parent.parent).resolve()
        data_dir = Path(os.getenv("CODEX_VIEWER_DATA_DIR", root / "data")).expanduser()
        database_path = Path(
            os.getenv("CODEX_VIEWER_DB", data_dir / "codex_sessions.sqlite3")
        ).expanduser()
        sync_mode = os.getenv("CODEX_VIEWER_SYNC_MODE", "local").strip().lower() or "local"
        app_version = os.getenv("CODEX_VIEWER_APP_VERSION", __version__).strip() or __version__
        sync_api_version = os.getenv("CODEX_VIEWER_API_VERSION", SYNC_API_VERSION).strip() or SYNC_API_VERSION
        expected_agent_version = os.getenv("CODEX_VIEWER_EXPECTED_AGENT_VERSION", app_version).strip() or app_version
        minimum_agent_version = os.getenv("CODEX_VIEWER_MIN_AGENT_VERSION", expected_agent_version).strip() or expected_agent_version
        agent_update_command = os.getenv("CODEX_VIEWER_AGENT_UPDATE_COMMAND", "").strip() or None
        page_size = int(os.getenv("CODEX_VIEWER_PAGE_SIZE", "24"))
        sync_on_start = os.getenv("CODEX_VIEWER_SYNC_ON_START", "1") != "0"
        session_roots = _split_roots(os.getenv("CODEX_SESSION_ROOTS"))
        server_host = os.getenv("CODEX_VIEWER_HOST", "127.0.0.1")
        server_port = int(os.getenv("CODEX_VIEWER_PORT", "8000"))
        server_base_url = _clean_url(os.getenv("CODEX_VIEWER_SERVER_URL"))
        sync_api_token = os.getenv("CODEX_VIEWER_SYNC_API_TOKEN", "").strip() or None
        sync_interval_seconds = int(os.getenv("CODEX_VIEWER_SYNC_INTERVAL", "30"))
        remote_timeout_seconds = int(os.getenv("CODEX_VIEWER_REMOTE_TIMEOUT", "15"))
        log_level = os.getenv("CODEX_VIEWER_LOG_LEVEL", "info")
        source_host = os.getenv("CODEX_VIEWER_SOURCE_HOST", socket.gethostname())
        return cls(
            project_root=root,
            data_dir=data_dir,
            database_path=database_path,
            session_roots=session_roots,
            sync_mode=sync_mode,
            app_version=app_version,
            sync_api_version=sync_api_version,
            expected_agent_version=expected_agent_version,
            minimum_agent_version=minimum_agent_version,
            agent_update_command=agent_update_command,
            sync_on_start=sync_on_start,
            page_size=page_size,
            server_host=server_host,
            server_port=server_port,
            server_base_url=server_base_url,
            sync_api_token=sync_api_token,
            sync_interval_seconds=sync_interval_seconds,
            remote_timeout_seconds=remote_timeout_seconds,
            log_level=log_level,
            source_host=source_host,
        )

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
