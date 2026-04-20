from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from typing import Any
import zipfile

from .config import SESSION_SECRET_FILENAME, Settings


BACKUP_FORMAT_VERSION = 1
BACKUP_MANIFEST_PATH = "manifest.json"
BACKUP_DATA_PREFIX = "data"
BACKUP_DATABASE_PREFIX = "database"


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def _normalize_member_path(raw: str) -> Path:
    candidate = Path(str(raw).strip())
    if not candidate.parts:
        raise ValueError("Archive member path was empty")
    if candidate.is_absolute():
        raise ValueError(f"Archive member path must be relative: {raw}")
    if any(part in {"", ".", ".."} for part in candidate.parts):
        raise ValueError(f"Archive member path was unsafe: {raw}")
    return candidate


def _write_bytes_atomic(destination: Path, payload: bytes, *, mode: int | None = None) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as handle:
        handle.write(payload)
        temp_name = handle.name
    os.replace(temp_name, destination)
    if mode is not None:
        destination.chmod(mode)


def _copy_database_snapshot(database_path: Path) -> Path:
    if not database_path.exists():
        raise RuntimeError(f"Database file does not exist: {database_path}")
    with tempfile.NamedTemporaryFile(prefix="codex-viewer-backup-", suffix=".sqlite3", delete=False) as handle:
        snapshot_path = Path(handle.name)
    try:
        with sqlite3.connect(database_path, timeout=30.0) as source:
            with sqlite3.connect(snapshot_path, timeout=30.0) as destination:
                source.backup(destination)
        return snapshot_path
    except Exception:
        snapshot_path.unlink(missing_ok=True)
        raise


def _database_sidecar_paths(database_path: Path) -> set[Path]:
    return {
        Path(f"{database_path}-wal"),
        Path(f"{database_path}-shm"),
        Path(f"{database_path}-journal"),
    }


def _iter_data_dir_files(data_dir: Path, *, skip_path: Path | None = None) -> list[Path]:
    if not data_dir.exists():
        return []
    files: list[Path] = []
    skip_paths: set[Path] = set()
    if skip_path is not None:
        skip_paths.add(skip_path)
        skip_paths.update(_database_sidecar_paths(skip_path))
    skip_resolved = {path.resolve() for path in skip_paths if path.exists()}
    for path in sorted(data_dir.rglob("*")):
        if not path.is_file():
            continue
        try:
            resolved = path.resolve()
        except FileNotFoundError:
            continue
        if resolved in skip_resolved:
            continue
        files.append(path)
    return files


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    if not _table_exists(connection, table_name):
        return 0
    row = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0] if row is not None else 0)


def _collect_database_stats(database_path: Path) -> dict[str, int]:
    with sqlite3.connect(database_path, timeout=30.0) as connection:
        return {
            "sessions": _count_rows(connection, "sessions"),
            "events": _count_rows(connection, "events"),
            "projects": _count_rows(connection, "projects"),
            "users": _count_rows(connection, "users"),
            "session_artifacts": _count_rows(connection, "session_artifacts"),
            "saved_turns": _count_rows(connection, "saved_turns"),
        }


def _build_manifest(
    settings: Settings,
    *,
    created_at: str,
    database_snapshot_path: Path,
    database_archive_path: str,
    data_files: list[Path],
) -> dict[str, Any]:
    stats = _collect_database_stats(database_snapshot_path)
    artifact_files = [
        path for path in data_files if str(path.relative_to(settings.data_dir).as_posix()).startswith("session_artifacts/")
    ]
    session_secret_path = settings.data_dir / SESSION_SECRET_FILENAME
    return {
        "backup_version": BACKUP_FORMAT_VERSION,
        "created_at": created_at,
        "app_version": settings.app_version,
        "source": {
            "data_dir": str(settings.data_dir),
            "database_path": str(settings.database_path),
        },
        "database": {
            "archive_path": database_archive_path,
            "filename": settings.database_path.name,
            "inside_data_dir": database_archive_path.startswith(f"{BACKUP_DATA_PREFIX}/"),
            "size": database_snapshot_path.stat().st_size,
        },
        "data_dir": {
            "archive_root": BACKUP_DATA_PREFIX,
            "file_count": len(data_files) + (1 if database_archive_path.startswith(f"{BACKUP_DATA_PREFIX}/") else 0),
            "artifact_file_count": len(artifact_files),
            "session_secret_included": session_secret_path.exists(),
            "total_bytes": sum(path.stat().st_size for path in data_files)
            + (database_snapshot_path.stat().st_size if database_archive_path.startswith(f"{BACKUP_DATA_PREFIX}/") else 0),
        },
        "stats": stats,
    }


def create_instance_backup(
    settings: Settings,
    *,
    output_path: Path,
) -> dict[str, Any]:
    created_at = utc_now_iso()
    database_snapshot_path = _copy_database_snapshot(settings.database_path)
    try:
        database_is_in_data_dir = False
        try:
            database_relative_path = settings.database_path.resolve().relative_to(settings.data_dir.resolve())
            database_is_in_data_dir = True
        except ValueError:
            database_relative_path = Path(settings.database_path.name)

        data_files = _iter_data_dir_files(
            settings.data_dir,
            skip_path=settings.database_path if database_is_in_data_dir else None,
        )
        if database_is_in_data_dir:
            database_archive_path = f"{BACKUP_DATA_PREFIX}/{database_relative_path.as_posix()}"
        else:
            database_archive_path = f"{BACKUP_DATABASE_PREFIX}/{settings.database_path.name}"
        manifest = _build_manifest(
            settings,
            created_at=created_at,
            database_snapshot_path=database_snapshot_path,
            database_archive_path=database_archive_path,
            data_files=data_files,
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=output_path.parent,
            prefix=".codex-viewer-backup-",
            suffix=".zip",
            delete=False,
        ) as handle:
            temp_archive_path = Path(handle.name)

        try:
            with zipfile.ZipFile(temp_archive_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
                for data_file in data_files:
                    relative_path = data_file.relative_to(settings.data_dir).as_posix()
                    archive.write(data_file, arcname=f"{BACKUP_DATA_PREFIX}/{relative_path}")
                archive.write(database_snapshot_path, arcname=database_archive_path)
                archive.writestr(
                    BACKUP_MANIFEST_PATH,
                    json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
                )
            verification = verify_backup_archive(temp_archive_path)
            if verification["status"] != "ok":
                raise RuntimeError(f"Backup verification failed for {temp_archive_path}")
            os.replace(temp_archive_path, output_path)
        except Exception:
            temp_archive_path.unlink(missing_ok=True)
            raise
    finally:
        database_snapshot_path.unlink(missing_ok=True)

    return {
        "status": "ok",
        "archive_path": str(output_path),
        "manifest": manifest,
    }


def _load_manifest(archive: zipfile.ZipFile) -> dict[str, Any]:
    try:
        raw_manifest = archive.read(BACKUP_MANIFEST_PATH).decode("utf-8")
    except KeyError as exc:
        raise RuntimeError("Backup archive did not contain manifest.json") from exc
    try:
        manifest = json.loads(raw_manifest)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Backup manifest was not valid JSON") from exc
    if not isinstance(manifest, dict):
        raise RuntimeError("Backup manifest had an invalid shape")
    if int(manifest.get("backup_version") or 0) != BACKUP_FORMAT_VERSION:
        raise RuntimeError(f"Unsupported backup format version: {manifest.get('backup_version')}")
    return manifest


def _extract_member_bytes(archive: zipfile.ZipFile, member_name: str) -> bytes:
    try:
        return archive.read(member_name)
    except KeyError as exc:
        raise RuntimeError(f"Backup archive was missing required entry: {member_name}") from exc


def verify_backup_archive(archive_path: Path) -> dict[str, Any]:
    if not archive_path.exists():
        raise RuntimeError(f"Backup archive does not exist: {archive_path}")

    with zipfile.ZipFile(archive_path) as archive:
        manifest = _load_manifest(archive)
        archive_members = set(archive.namelist())
        database_meta = manifest.get("database")
        data_meta = manifest.get("data_dir")
        stats_meta = manifest.get("stats")
        if not isinstance(database_meta, dict) or not isinstance(data_meta, dict) or not isinstance(stats_meta, dict):
            raise RuntimeError("Backup manifest was missing required sections")

        database_archive_path = str(database_meta.get("archive_path") or "").strip()
        if not database_archive_path:
            raise RuntimeError("Backup manifest was missing database archive_path")
        _normalize_member_path(database_archive_path)
        if database_archive_path not in archive_members:
            raise RuntimeError(f"Backup archive was missing database entry: {database_archive_path}")

        data_entries = [
            member
            for member in archive_members
            if member.startswith(f"{BACKUP_DATA_PREFIX}/") and not member.endswith("/")
        ]
        expected_file_count = int(data_meta.get("file_count") or 0)
        if expected_file_count != len(data_entries):
            raise RuntimeError(
                f"Backup archive had {len(data_entries)} data files but manifest expected {expected_file_count}"
            )

        artifact_entries = [
            member
            for member in data_entries
            if member.startswith(f"{BACKUP_DATA_PREFIX}/session_artifacts/")
        ]
        expected_artifact_count = int(data_meta.get("artifact_file_count") or 0)
        if expected_artifact_count != len(artifact_entries):
            raise RuntimeError(
                f"Backup archive had {len(artifact_entries)} artifact files but manifest expected {expected_artifact_count}"
            )

        with tempfile.NamedTemporaryFile(prefix="codex-viewer-backup-verify-", suffix=".sqlite3", delete=False) as handle:
            temp_database_path = Path(handle.name)
        try:
            temp_database_path.write_bytes(_extract_member_bytes(archive, database_archive_path))
            with sqlite3.connect(temp_database_path, timeout=30.0) as connection:
                result = connection.execute("PRAGMA integrity_check").fetchone()
                integrity = str(result[0] if result is not None else "")
            if integrity.lower() != "ok":
                raise RuntimeError(f"Backup database integrity check failed: {integrity}")

            actual_stats = _collect_database_stats(temp_database_path)
        finally:
            temp_database_path.unlink(missing_ok=True)

    for key, value in actual_stats.items():
        if int(stats_meta.get(key) or 0) != value:
            raise RuntimeError(
                f"Backup archive stat mismatch for {key}: manifest={int(stats_meta.get(key) or 0)} actual={value}"
            )

    return {
        "status": "ok",
        "archive_path": str(archive_path),
        "manifest": manifest,
        "verified_at": utc_now_iso(),
    }


def _prepare_restore_destination(target_data_dir: Path) -> None:
    if target_data_dir.exists():
        if any(target_data_dir.iterdir()):
            raise RuntimeError(f"Restore target must be empty: {target_data_dir}")
    else:
        target_data_dir.mkdir(parents=True, exist_ok=True)


def restore_instance_backup(
    archive_path: Path,
    *,
    target_data_dir: Path,
    target_database_path: Path | None = None,
) -> dict[str, Any]:
    verification = verify_backup_archive(archive_path)
    manifest = verification["manifest"]
    database_meta = manifest["database"]
    database_archive_path = str(database_meta["archive_path"])
    inside_data_dir = bool(database_meta.get("inside_data_dir"))
    _prepare_restore_destination(target_data_dir)

    if inside_data_dir:
        database_relative_path = _normalize_member_path(database_archive_path).relative_to(BACKUP_DATA_PREFIX)
        resolved_database_path = target_data_dir / database_relative_path
    else:
        resolved_database_path = target_database_path or (target_data_dir / str(database_meta.get("filename") or "codex_sessions.sqlite3"))
        if resolved_database_path.exists():
            raise RuntimeError(f"Restore database path already exists: {resolved_database_path}")

    with zipfile.ZipFile(archive_path) as archive:
        for member_name in archive.namelist():
            if member_name.endswith("/"):
                continue
            member_path = _normalize_member_path(member_name)
            member_bytes = archive.read(member_name)
            if member_name == BACKUP_MANIFEST_PATH:
                continue
            if member_name.startswith(f"{BACKUP_DATA_PREFIX}/"):
                relative_path = member_path.relative_to(BACKUP_DATA_PREFIX)
                destination = target_data_dir / relative_path
                mode = 0o600 if destination.name == SESSION_SECRET_FILENAME else None
                _write_bytes_atomic(destination, member_bytes, mode=mode)
            elif member_name == database_archive_path:
                _write_bytes_atomic(resolved_database_path, member_bytes)

    return {
        "status": "ok",
        "archive_path": str(archive_path),
        "restored_data_dir": str(target_data_dir),
        "restored_database_path": str(resolved_database_path),
        "manifest": manifest,
        "restored_at": utc_now_iso(),
    }
