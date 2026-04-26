from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import Settings


MACHINE_IDENTITY_FILENAME = "machine-identity.json"


@dataclass(slots=True)
class LocalMachineIdentity:
    machine_id: str
    label: str
    source_host: str
    server_base_url: str
    public_key: str
    private_key: str
    paired_at: str
    created_by_user_id: str | None = None


def machine_identity_path(settings: Settings) -> Path:
    return settings.data_dir / MACHINE_IDENTITY_FILENAME


def _write_secure_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(content)
    try:
        path.chmod(0o600)
    except OSError:
        pass


def store_machine_identity(settings: Settings, identity: LocalMachineIdentity) -> Path:
    path = machine_identity_path(settings)
    _write_secure_text(
        path,
        json.dumps(asdict(identity), indent=2, sort_keys=True) + "\n",
    )
    return path


def load_machine_identity(settings: Settings) -> LocalMachineIdentity | None:
    path = machine_identity_path(settings)
    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw_payload, dict):
        return None
    try:
        return LocalMachineIdentity(
            machine_id=str(raw_payload["machine_id"]),
            label=str(raw_payload["label"]),
            source_host=str(raw_payload["source_host"]),
            server_base_url=str(raw_payload["server_base_url"]),
            public_key=str(raw_payload["public_key"]),
            private_key=str(raw_payload["private_key"]),
            paired_at=str(raw_payload["paired_at"]),
            created_by_user_id=str(raw_payload["created_by_user_id"])
            if raw_payload.get("created_by_user_id") is not None
            else None,
        )
    except KeyError:
        return None


def delete_machine_identity(settings: Settings) -> bool:
    path = machine_identity_path(settings)
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    return True
