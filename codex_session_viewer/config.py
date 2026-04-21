from __future__ import annotations

import os
import secrets
import socket
from dataclasses import dataclass
from pathlib import Path

from . import SYNC_API_VERSION, __version__


SESSION_SECRET_FILENAME = ".session-secret"


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


def _clean_text(raw: str | None) -> str | None:
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def _session_secret_path(data_dir: Path) -> Path:
    return data_dir / SESSION_SECRET_FILENAME


def _read_session_secret(path: Path) -> str | None:
    try:
        return _clean_text(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def _write_session_secret(path: Path, secret: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        existing = _read_session_secret(path)
        if existing:
            return existing
        path.write_text(secret, encoding="utf-8")
        path.chmod(0o600)
        return secret

    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(secret)
    return secret


def load_or_create_session_secret(data_dir: Path) -> str:
    path = _session_secret_path(data_dir)
    existing = _read_session_secret(path)
    if existing:
        return existing
    return _write_session_secret(path, secrets.token_urlsafe(48))


def _normalize_auth_mode(raw: str | None) -> str:
    candidate = (raw or "none").strip().lower().replace("-", "_")
    if candidate in {"none", ""}:
        return "none"
    if candidate in {"password", "local"}:
        return "password"
    if candidate in {"proxy", "sso", "header", "headers"}:
        return "proxy"
    if candidate in {"password_or_proxy", "proxy_or_password", "both", "hybrid"}:
        return "password_or_proxy"
    return candidate


def _env_truthy(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _parse_dotenv_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if stripped.startswith("export "):
        stripped = stripped[7:].lstrip()
    if "=" not in stripped:
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    if not key:
        return None
    value = value.strip()

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    elif " #" in value:
        value = value.split(" #", 1)[0].rstrip()

    return key, value


def _load_dotenv_file(path: Path, protected_keys: set[str]) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_dotenv_line(line)
        if parsed is None:
            continue
        key, value = parsed
        if key in protected_keys:
            continue
        os.environ[key] = value


def load_project_env(project_root: Path) -> None:
    protected_keys = set(os.environ)
    _load_dotenv_file(project_root / ".env", protected_keys)
    environment_name = (os.getenv("CODEX_VIEWER_ENV") or "").strip()
    if environment_name:
        _load_dotenv_file(project_root / f".env.{environment_name}", protected_keys)
    _load_dotenv_file(project_root / ".env.local", protected_keys)
    if environment_name:
        _load_dotenv_file(project_root / f".env.{environment_name}.local", protected_keys)


@dataclass(slots=True)
class Settings:
    project_root: Path
    environment_name: str
    data_dir: Path
    database_path: Path
    session_roots: list[Path]
    sync_mode: str
    app_version: str
    sync_api_version: str
    expected_agent_version: str
    agent_update_command: str | None
    daemon_rebuild_on_start: bool
    sync_on_start: bool
    page_size: int
    alerts_enabled: bool
    alerts_provider: str
    alerts_webhook_url: str | None
    alerts_realert_minutes: int
    alerts_send_resolutions: bool
    server_host: str
    server_port: int
    server_base_url: str | None
    sync_api_token: str | None
    sync_interval_seconds: int
    remote_timeout_seconds: int
    remote_batch_size: int
    log_level: str
    source_host: str
    auth_mode: str
    session_secret: str | None
    auth_proxy_user_header: str
    auth_proxy_name_header: str | None
    auth_proxy_email_header: str | None
    auth_proxy_login_url: str | None
    auth_proxy_logout_url: str | None
    auth_cookie_secure: bool

    @classmethod
    def from_env(cls, project_root: Path | None = None) -> "Settings":
        root = (project_root or Path(__file__).resolve().parent.parent).resolve()
        load_project_env(root)
        environment_name = (os.getenv("CODEX_VIEWER_ENV") or "").strip() or "default"
        data_dir = Path(os.getenv("CODEX_VIEWER_DATA_DIR", root / "data")).expanduser()
        database_path = Path(
            os.getenv("CODEX_VIEWER_DB", data_dir / "codex_sessions.sqlite3")
        ).expanduser()
        sync_mode = os.getenv("CODEX_VIEWER_SYNC_MODE", "local").strip().lower() or "local"
        app_version = __version__
        sync_api_version = SYNC_API_VERSION
        expected_agent_version = app_version
        agent_update_command = os.getenv("CODEX_VIEWER_AGENT_UPDATE_COMMAND", "").strip() or None
        daemon_rebuild_on_start = _env_truthy(os.getenv("CODEX_VIEWER_DAEMON_REBUILD_ON_START"), False)
        page_size = 24
        sync_on_start = True
        alerts_enabled = _env_truthy(os.getenv("CODEX_VIEWER_ALERTS_ENABLED"), False)
        alerts_provider = (os.getenv("CODEX_VIEWER_ALERTS_PROVIDER") or "webhook").strip().lower() or "webhook"
        alerts_webhook_url = _clean_url(os.getenv("CODEX_VIEWER_ALERTS_WEBHOOK_URL"))
        alerts_realert_minutes = int(os.getenv("CODEX_VIEWER_ALERTS_REALERT_MINUTES", "60"))
        alerts_send_resolutions = _env_truthy(os.getenv("CODEX_VIEWER_ALERTS_SEND_RESOLUTIONS"), True)
        session_roots = _split_roots(os.getenv("CODEX_SESSION_ROOTS"))
        server_host = os.getenv("CODEX_VIEWER_HOST", "127.0.0.1")
        server_port = int(os.getenv("CODEX_VIEWER_PORT", "8000"))
        server_base_url = _clean_url(os.getenv("CODEX_VIEWER_SERVER_URL"))
        sync_api_token = os.getenv("CODEX_VIEWER_SYNC_API_TOKEN", "").strip() or None
        sync_interval_seconds = int(os.getenv("CODEX_VIEWER_SYNC_INTERVAL", "30"))
        remote_timeout_seconds = int(os.getenv("CODEX_VIEWER_REMOTE_TIMEOUT", "15"))
        remote_batch_size = max(1, min(int(os.getenv("CODEX_VIEWER_REMOTE_BATCH_SIZE", "25")), 25))
        log_level = os.getenv("CODEX_VIEWER_LOG_LEVEL", "info")
        source_host = os.getenv("CODEX_VIEWER_SOURCE_HOST", socket.gethostname())
        auth_mode = _normalize_auth_mode(os.getenv("CODEX_VIEWER_AUTH_MODE"))
        session_secret = _clean_text(os.getenv("CODEX_VIEWER_SESSION_SECRET"))
        auth_proxy_user_header = _clean_text(os.getenv("CODEX_VIEWER_AUTH_PROXY_USER_HEADER")) or "X-Forwarded-User"
        auth_proxy_name_header = _clean_text(os.getenv("CODEX_VIEWER_AUTH_PROXY_NAME_HEADER")) or "X-Forwarded-Name"
        auth_proxy_email_header = _clean_text(os.getenv("CODEX_VIEWER_AUTH_PROXY_EMAIL_HEADER")) or "X-Forwarded-Email"
        auth_proxy_login_url = _clean_text(os.getenv("CODEX_VIEWER_AUTH_PROXY_LOGIN_URL"))
        auth_proxy_logout_url = _clean_text(os.getenv("CODEX_VIEWER_AUTH_PROXY_LOGOUT_URL"))
        auth_cookie_secure = _env_truthy(os.getenv("CODEX_VIEWER_AUTH_COOKIE_SECURE"), False)
        return cls(
            project_root=root,
            environment_name=environment_name,
            data_dir=data_dir,
            database_path=database_path,
            session_roots=session_roots,
            sync_mode=sync_mode,
            app_version=app_version,
            sync_api_version=sync_api_version,
            expected_agent_version=expected_agent_version,
            agent_update_command=agent_update_command,
            daemon_rebuild_on_start=daemon_rebuild_on_start,
            sync_on_start=sync_on_start,
            page_size=page_size,
            alerts_enabled=alerts_enabled,
            alerts_provider=alerts_provider,
            alerts_webhook_url=alerts_webhook_url,
            alerts_realert_minutes=alerts_realert_minutes,
            alerts_send_resolutions=alerts_send_resolutions,
            server_host=server_host,
            server_port=server_port,
            server_base_url=server_base_url,
            sync_api_token=sync_api_token,
            sync_interval_seconds=sync_interval_seconds,
            remote_timeout_seconds=remote_timeout_seconds,
            remote_batch_size=remote_batch_size,
            log_level=log_level,
            source_host=source_host,
            auth_mode=auth_mode,
            session_secret=session_secret,
            auth_proxy_user_header=auth_proxy_user_header,
            auth_proxy_name_header=auth_proxy_name_header,
            auth_proxy_email_header=auth_proxy_email_header,
            auth_proxy_login_url=auth_proxy_login_url,
            auth_proxy_logout_url=auth_proxy_logout_url,
            auth_cookie_secure=auth_cookie_secure,
        )

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if self.auth_enabled() and not self.session_secret:
            self.session_secret = load_or_create_session_secret(self.data_dir)

    def auth_enabled(self) -> bool:
        return self.auth_mode != "none"

    def auth_allows_password(self) -> bool:
        return self.auth_mode in {"password", "password_or_proxy"}

    def auth_allows_proxy(self) -> bool:
        return self.auth_mode in {"proxy", "password_or_proxy"}
