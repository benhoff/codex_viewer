from __future__ import annotations

from datetime import UTC, datetime
import gzip
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
from typing import Any

from .config import Settings

ARTIFACT_MEDIA_TYPE = "application/x-ndjson"
ARTIFACT_TEXT_ENCODING = "utf-8"
ARTIFACT_COMPRESSION = "gzip"
ARTIFACT_ROOT = Path("session_artifacts")


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def read_session_source_text(source_path: Path) -> str:
    with source_path.open("r", encoding=ARTIFACT_TEXT_ENCODING, newline="") as handle:
        return handle.read()


def raw_session_sha256(raw_jsonl: str) -> str:
    return hashlib.sha256(raw_jsonl.encode(ARTIFACT_TEXT_ENCODING)).hexdigest()


def artifact_storage_path(artifact_sha256: str) -> str:
    normalized = artifact_sha256.strip().lower()
    return str(ARTIFACT_ROOT / normalized[:2] / f"{normalized}.jsonl.gz")


def absolute_artifact_path(settings: Settings, storage_path: str) -> Path:
    return settings.data_dir / storage_path


def fetch_session_artifact(
    connection: sqlite3.Connection,
    artifact_sha256: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM session_artifacts
        WHERE sha256 = ?
        """,
        (artifact_sha256,),
    ).fetchone()


def _write_artifact_file(destination: Path, payload: bytes) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as handle:
        handle.write(payload)
        temp_name = handle.name
    os.replace(temp_name, destination)


def store_session_artifact(
    connection: sqlite3.Connection,
    settings: Settings,
    raw_jsonl: str,
) -> str:
    raw_bytes = raw_jsonl.encode(ARTIFACT_TEXT_ENCODING)
    artifact_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    storage_path = artifact_storage_path(artifact_sha256)
    absolute_path = absolute_artifact_path(settings, storage_path)
    compressed_payload = gzip.compress(raw_bytes, compresslevel=6)
    now = utc_now_iso()

    if not absolute_path.exists():
        _write_artifact_file(absolute_path, compressed_payload)

    existing = fetch_session_artifact(connection, artifact_sha256)
    if existing is None:
        connection.execute(
            """
            INSERT INTO session_artifacts (
                sha256,
                storage_path,
                media_type,
                text_encoding,
                compression,
                original_size,
                stored_size,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact_sha256,
                storage_path,
                ARTIFACT_MEDIA_TYPE,
                ARTIFACT_TEXT_ENCODING,
                ARTIFACT_COMPRESSION,
                len(raw_bytes),
                len(compressed_payload),
                now,
                now,
            ),
        )
    else:
        connection.execute(
            """
            UPDATE session_artifacts
            SET
                storage_path = ?,
                media_type = ?,
                text_encoding = ?,
                compression = ?,
                original_size = ?,
                stored_size = ?,
                updated_at = ?
            WHERE sha256 = ?
            """,
            (
                storage_path,
                ARTIFACT_MEDIA_TYPE,
                ARTIFACT_TEXT_ENCODING,
                ARTIFACT_COMPRESSION,
                len(raw_bytes),
                len(compressed_payload),
                now,
                artifact_sha256,
            ),
        )
    return artifact_sha256


def load_session_artifact_text(
    connection: sqlite3.Connection,
    settings: Settings,
    artifact_sha256: str,
) -> str | None:
    artifact = fetch_session_artifact(connection, artifact_sha256)
    if artifact is None:
        return None
    artifact_path = absolute_artifact_path(settings, str(artifact["storage_path"]))
    if not artifact_path.exists():
        return None

    stored_bytes = artifact_path.read_bytes()
    compression = str(artifact["compression"] or "").strip().lower()
    if compression == ARTIFACT_COMPRESSION:
        raw_bytes = gzip.decompress(stored_bytes)
    else:
        raw_bytes = stored_bytes
    encoding = str(artifact["text_encoding"] or ARTIFACT_TEXT_ENCODING).strip() or ARTIFACT_TEXT_ENCODING
    return raw_bytes.decode(encoding)


def resolve_session_raw_text(
    connection: sqlite3.Connection,
    settings: Settings,
    session: sqlite3.Row | dict[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    artifact_sha256 = str(session["raw_artifact_sha256"] or "").strip()
    if artifact_sha256:
        artifact = fetch_session_artifact(connection, artifact_sha256)
        artifact_text = load_session_artifact_text(connection, settings, artifact_sha256)
        if artifact is not None and artifact_text is not None:
            return artifact_text, {
                "source": "artifact",
                "artifact_sha256": artifact_sha256,
                "storage_path": str(artifact["storage_path"] or ""),
                "compression": str(artifact["compression"] or ""),
                "original_size": int(artifact["original_size"] or 0),
                "stored_size": int(artifact["stored_size"] or 0),
            }

    source_path = Path(str(session["source_path"] or "").strip())
    if source_path.exists():
        raw_text = read_session_source_text(source_path)
        return raw_text, {
            "source": "filesystem",
            "artifact_sha256": artifact_sha256 or None,
            "storage_path": "",
            "compression": "",
            "original_size": len(raw_text.encode(ARTIFACT_TEXT_ENCODING)),
            "stored_size": None,
        }

    return None, {
        "source": "missing",
        "artifact_sha256": artifact_sha256 or None,
        "storage_path": "",
        "compression": "",
        "original_size": None,
        "stored_size": None,
    }
