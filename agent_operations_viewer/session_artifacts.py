from __future__ import annotations

from datetime import UTC, datetime
import gzip
import hashlib
import logging
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

logger = logging.getLogger("agent_operations_viewer.session_artifacts")


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
    existing = fetch_session_artifact(connection, artifact_sha256)
    if existing is not None and absolute_path.exists():
        return artifact_sha256
    compressed_payload = gzip.compress(raw_bytes, compresslevel=6)
    now = utc_now_iso()

    if not absolute_path.exists():
        _write_artifact_file(absolute_path, compressed_payload)

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
                base_artifact_sha256 = NULL,
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


def store_session_artifact_tail(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    base_sha256: str,
    tail_jsonl: str,
    combined_sha256: str,
    original_size: int,
) -> str:
    """Store only the append; immutable ancestors remain shared and readable."""
    existing = fetch_session_artifact(connection, combined_sha256)
    if existing is not None:
        return combined_sha256
    base = fetch_session_artifact(connection, base_sha256)
    if base is None:
        raise ValueError("Missing base artifact")
    compressed = gzip.compress(tail_jsonl.encode(ARTIFACT_TEXT_ENCODING), compresslevel=6)
    storage_path = artifact_storage_path(combined_sha256)
    # Replace a possible file left by a rolled-back full import of the same
    # digest: the database row determines whether this is a full file or tail.
    _write_artifact_file(absolute_artifact_path(settings, storage_path), compressed)
    now = utc_now_iso()
    connection.execute("""
        INSERT INTO session_artifacts (
            sha256, base_artifact_sha256, storage_path, original_size,
            stored_size, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (combined_sha256, base_sha256, storage_path, original_size,
          int(base["stored_size"]) + len(compressed), now, now))
    return combined_sha256


def _artifact_chain(connection, artifact_sha256):
    chain, seen = [], set()
    current = artifact_sha256
    while current:
        if current in seen:
            raise ValueError("Artifact chain contains a cycle")
        seen.add(current)
        artifact = fetch_session_artifact(connection, current)
        if artifact is None:
            return None
        chain.append(artifact)
        current = str(artifact["base_artifact_sha256"] or "")
    return list(reversed(chain))


LIVE_ARTIFACTS_SQL = """
    WITH RECURSIVE live(sha256) AS (
        SELECT raw_artifact_sha256 FROM sessions WHERE raw_artifact_sha256 IS NOT NULL
        UNION
        SELECT a.base_artifact_sha256 FROM session_artifacts a
        JOIN live ON a.sha256 = live.sha256
        WHERE a.base_artifact_sha256 IS NOT NULL
    )
"""


def _managed_artifact_path(settings: Settings, artifact_sha256: str, storage_path: str) -> Path | None:
    normalized_sha256 = artifact_sha256.strip().lower()
    if len(normalized_sha256) != 64 or any(character not in "0123456789abcdef" for character in normalized_sha256):
        return None
    expected_storage_path = artifact_storage_path(normalized_sha256)
    if storage_path != expected_storage_path:
        return None
    return absolute_artifact_path(settings, expected_storage_path)


def prune_orphaned_session_artifacts(settings: Settings) -> int:
    """Remove raw artifacts that are not referenced by a current session.

    This runs in infrequent maintenance or after a local import pass. Holding the write lock while unlinking prevents another writer from
    adopting an artifact between the reference check and file removal.
    """
    from .db import connection_scope, write_transaction

    removed_rows = 0
    removed_files = 0
    removed_untracked_files = 0
    artifact_root = settings.data_dir / ARTIFACT_ROOT

    with connection_scope(settings.database_path) as connection:
        with write_transaction(connection):
            orphan_rows = connection.execute(
                LIVE_ARTIFACTS_SQL + """
                SELECT sha256, storage_path FROM session_artifacts
                WHERE sha256 NOT IN (SELECT sha256 FROM live)
                """
            ).fetchall()

            removable_rows: list[tuple[str]] = []
            for row in orphan_rows:
                artifact_sha256 = str(row["sha256"] or "").strip().lower()
                artifact_path = _managed_artifact_path(
                    settings,
                    artifact_sha256,
                    str(row["storage_path"] or ""),
                )
                if artifact_path is None:
                    logger.warning("Refusing to prune invalid artifact path for %s", artifact_sha256)
                    continue
                try:
                    existed = artifact_path.exists()
                    artifact_path.unlink(missing_ok=True)
                except OSError:
                    logger.warning("Unable to remove orphaned session artifact %s", artifact_path, exc_info=True)
                    continue
                if existed:
                    removed_files += 1
                removable_rows.append((artifact_sha256,))

            if removable_rows:
                before_changes = connection.total_changes
                connection.executemany(
                    # The same write transaction has held the writer since
                    # selecting these orphans; no other writer can adopt them.
                    "DELETE FROM session_artifacts WHERE sha256 = ?",
                    removable_rows,
                )
                removed_rows = connection.total_changes - before_changes

            protected_paths = {
                str(row["storage_path"])
                for row in connection.execute("SELECT storage_path FROM session_artifacts").fetchall()
            }
            protected_paths.update(
                artifact_storage_path(str(row["raw_artifact_sha256"]))
                for row in connection.execute(
                    """
                    SELECT DISTINCT raw_artifact_sha256
                    FROM sessions
                    WHERE raw_artifact_sha256 IS NOT NULL
                      AND TRIM(raw_artifact_sha256) <> ''
                    """
                ).fetchall()
            )
            if artifact_root.exists():
                for artifact_path in artifact_root.glob("[0-9a-f][0-9a-f]/*.jsonl.gz"):
                    storage_path = str(artifact_path.relative_to(settings.data_dir))
                    if storage_path in protected_paths:
                        continue
                    artifact_sha256 = artifact_path.name.removesuffix(".jsonl.gz")
                    if _managed_artifact_path(settings, artifact_sha256, storage_path) != artifact_path:
                        continue
                    try:
                        artifact_path.unlink()
                        removed_untracked_files += 1
                    except FileNotFoundError:
                        continue
                    except OSError:
                        logger.warning("Unable to remove untracked session artifact %s", artifact_path, exc_info=True)

    if artifact_root.exists():
        for directory in artifact_root.iterdir():
            if not directory.is_dir():
                continue
            try:
                directory.rmdir()
            except OSError:
                continue

    if removed_rows or removed_untracked_files:
        logger.info(
            "Pruned %d orphaned session artifacts (%d tracked files, %d untracked files)",
            removed_rows,
            removed_files,
            removed_untracked_files,
        )
    return removed_rows + removed_untracked_files


def iter_session_artifact_bytes(connection, settings, artifact_sha256):
    """Stream immutable pieces without expanding an entire transcript in RAM."""
    chain = _artifact_chain(connection, artifact_sha256)
    if chain is None:
        raise FileNotFoundError("Missing artifact ancestor")
    for artifact in chain:
        path = absolute_artifact_path(settings, str(artifact["storage_path"]))
        compression = str(artifact["compression"] or "").strip().lower()
        opener = gzip.open if compression == ARTIFACT_COMPRESSION else open
        with opener(path, "rb") as source:
            while block := source.read(256 * 1024):
                yield block


def load_session_artifact_text(
    connection: sqlite3.Connection,
    settings: Settings,
    artifact_sha256: str,
) -> str | None:
    artifact = fetch_session_artifact(connection, artifact_sha256)
    if artifact is None:
        return None
    try:
        raw_bytes = b"".join(iter_session_artifact_bytes(connection, settings, artifact_sha256))
    except FileNotFoundError:
        return None
    encoding = str(artifact["text_encoding"] or "").strip() or ARTIFACT_TEXT_ENCODING
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
