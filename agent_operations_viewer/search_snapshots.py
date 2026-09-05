"""Durable, expiring SQLite read snapshots for evidence research.

Snapshots are private database backups, never connections held open against the
live WAL. They are owner-bound and fail closed when their access scope shrinks.
"""

from __future__ import annotations

from contextlib import contextmanager, closing
from dataclasses import asdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import json
import os
import secrets
import sqlite3
import tempfile
import time
import fcntl
import logging
import threading

from fastapi import HTTPException
from itsdangerous import BadData, URLSafeSerializer

from .db import connect
from .projects import (
    ProjectAccessContext,
    build_project_access_context,
    project_access_condition_sql,
    visible_session_where,
)
from .search import (
    _base_search_conditions,
    _search_coverage,
    coverage_readiness,
    prepare_coverage_inventory,
)
from .turn_index import TURN_INDEX_VERSION, TURN_SEARCH_VERSION, SEARCH_CHUNK_VERSION

SNAPSHOT_TTL_SECONDS = 900
MAX_SNAPSHOTS = 32
NORMALIZATION_VERSION = "evidence-1"
SNAPSHOT_REQUEST_WAIT_SECONDS = 1.0
# A full production backup alone can take about five minutes. Leave room for
# inventory/metadata work while retaining at least five minutes of snapshot TTL.
SNAPSHOT_BUILD_TIMEOUT_SECONDS = 600
logger = logging.getLogger(__name__)


def api_error(status: int, code: str, message: str, **details):
    return HTTPException(
        status_code=status, detail={"code": code, "message": message, **details}
    )


def digest(value) -> str:
    serialized = json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    return "sha256:" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _visible_members(connection, access):
    condition, params = project_access_condition_sql(access)
    rows = connection.execute(
        "SELECT s.id, p.id AS project_id FROM sessions s "
        "LEFT JOIN project_sources ps ON ps.match_project_key = s.inferred_project_key "
        "LEFT JOIN projects p ON p.id = ps.project_id "
        + visible_session_where([condition] if condition else []),
        params,
    )
    return {row["id"]: row["project_id"] for row in rows}


def _visible_projects(connection, access):
    condition, params = project_access_condition_sql(access)
    return {
        row[0]
        for row in connection.execute(
            "SELECT p.id FROM projects p "
            + (f"WHERE {condition}" if condition else ""),
            params,
        )
    }


def _signer(settings):
    directory = settings.database_path.parent / "search-snapshots"
    directory.mkdir(mode=0o700, exist_ok=True)
    key_path = directory / "signing-key"
    # Existing keys need no lock. Initial publication is atomic, so even a
    # simultaneous first request never observes a partial signing key.
    if not key_path.exists():
        fd, temporary = tempfile.mkstemp(dir=directory, prefix="key-")
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(secrets.token_bytes(32))
            try:
                os.link(temporary, key_path)
            except FileExistsError:
                pass
        finally:
            os.unlink(temporary)
    key = key_path.read_bytes()
    return directory, URLSafeSerializer(key, salt="search-evidence-v1")


def read_cursor(signer, cursor):
    if not cursor:
        return None
    try:
        value = signer.loads(cursor)
        if (
            not isinstance(value, dict)
            or value.get("type") != "cursor"
            or type(value.get("page")) is not int
            or value["page"] < 1
            or not isinstance(value.get("snapshot_id"), str)
        ):
            raise BadData("Malformed cursor")
        return value
    except BadData as exc:
        raise api_error(400, "invalid_cursor", "Invalid or tampered cursor") from exc


def cursor_page(cursor, fingerprint):
    if cursor is None:
        return 1
    if cursor.get("fingerprint") != fingerprint:
        raise api_error(
            400, "cursor_mismatch", "Cursor does not match the normalized request"
        )
    return cursor["page"]


def encode_cursor(signer, page, fingerprint, snapshot_id):
    return signer.dumps(
        {
            "type": "cursor",
            "page": page,
            "fingerprint": fingerprint,
            "snapshot_id": snapshot_id,
        }
    )


@dataclass
class _SnapshotBuild:
    snapshot_id: str
    generation: str
    expires: float
    done: threading.Event = field(default_factory=threading.Event)


_BUILDS: dict[tuple[str, str], _SnapshotBuild] = {}
_BUILDS_LOCK = threading.Lock()


def _building(snapshot_id=None, **progress):
    details = {"retry_after": 2, **progress}
    if snapshot_id:
        details["snapshot_id"] = snapshot_id
    error = api_error(
        503,
        "snapshot_building",
        "Snapshot is being prepared; retry with the returned snapshot_id"
        if snapshot_id
        else "Another snapshot is being prepared; retry later",
        **details,
    )
    error.headers = {"Retry-After": "2"}
    return error


def _write_status(path, detail):
    """Publish complete status documents so concurrent polls never read partial JSON."""
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix="status-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(detail, stream)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _failure_reason(exc, *, timed_out):
    # Public diagnostics must not contain SQL, filesystem paths or corpus data.
    if timed_out or isinstance(exc, TimeoutError):
        return "deadline_exceeded"
    if isinstance(exc, sqlite3.Error):
        code = getattr(exc, "sqlite_errorcode", 0) & 0xFF
        return {
            sqlite3.SQLITE_BUSY: "database_busy",
            sqlite3.SQLITE_LOCKED: "database_locked",
            sqlite3.SQLITE_FULL: "disk_full",
            sqlite3.SQLITE_IOERR: "database_io_error",
            sqlite3.SQLITE_CORRUPT: "database_corrupt",
        }.get(code, "sqlite_error")
    if isinstance(exc, PermissionError):
        return "permission_denied"
    if isinstance(exc, OSError):
        return "filesystem_error"
    return "internal_error"


def _validate_snapshot(signer, snapshot_id, owner):
    try:
        token = signer.loads(snapshot_id)
        if not isinstance(token, dict) or token.get("type") != "snapshot":
            raise BadData("Wrong token type")
        generation = token["generation"]
        if (
            not isinstance(generation, str)
            or len(generation) != 48
            or any(c not in "0123456789abcdef" for c in generation)
        ):
            raise BadData("Invalid generation")
        if token["owner"] != owner:
            raise api_error(
                403,
                "snapshot_forbidden",
                "Snapshot belongs to another authorization identity",
            )
        if token["expires"] <= time.time():
            raise api_error(410, "snapshot_expired", "Snapshot expired")
        return token
    except (BadData, KeyError, TypeError) as exc:
        raise api_error(409, "invalid_snapshot", "Invalid snapshot identifier") from exc


def _build_snapshot(
    settings, directory, job, lock, *, auth_user, auth_enabled, owner, created
):
    temporary = directory / f"{job.generation}.creating"
    pending = directory / f"{job.generation}.pending"
    started = time.monotonic()
    deadline = started + SNAPSHOT_BUILD_TIMEOUT_SECONDS
    stage = "cleanup"
    stage_started = started
    timings = {}
    backup_progress = {}
    last_progress = started

    def status():
        now = time.monotonic()
        return {
            "build_id": job.generation,
            "stage": stage,
            "elapsed_seconds": round(now - started, 3),
            "stage_elapsed_seconds": round(now - stage_started, 3),
            "stage_timings_seconds": dict(timings),
            "budget_seconds": SNAPSHOT_BUILD_TIMEOUT_SECONDS,
            **backup_progress,
        }

    def enter_stage(name):
        nonlocal stage, stage_started
        # Attribute an overrun to the stage that spent the time, not the next one.
        check_deadline()
        now = time.monotonic()
        timings[stage] = round(now - stage_started, 3)
        logger.info(
            "Evidence snapshot %s stage=%s completed in %.3fs",
            job.generation,
            stage,
            now - stage_started,
        )
        stage, stage_started = name, now
        _write_status(pending, status())

    def check_deadline(*_args):
        if time.monotonic() > deadline:
            raise TimeoutError("Snapshot build exceeded its time budget")

    def backup_callback(result, remaining, total):
        nonlocal last_progress
        backup_progress.update(
            backup_pages_copied=total - remaining, backup_pages_total=total
        )
        now = time.monotonic()
        if now - last_progress >= 2 or remaining == 0:
            _write_status(pending, status())
            last_progress = now
        check_deadline()

    try:
        # This lock only serializes builders. Readers/token validation never wait
        # for it. Clean up generated artifacts after expiry, not live generations.
        for pattern in ("*.sqlite3", "*.pending", "*.failed", "*.creating"):
            for path in directory.glob(pattern):
                if (
                    path != temporary
                    and path != pending
                    and created - path.stat().st_mtime > SNAPSHOT_TTL_SECONDS
                ):
                    path.unlink()
        if len(list(directory.glob("*.sqlite3"))) >= MAX_SNAPSHOTS:
            raise api_error(
                503,
                "snapshot_capacity",
                "Snapshot capacity reached; reuse an existing snapshot or retry after expiry",
            )
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        enter_stage("backup")
        with closing(sqlite3.connect(temporary)) as frozen:
            frozen.row_factory = sqlite3.Row
            with closing(connect(settings.database_path)) as live:
                # Hold a consistent read generation while the online backup runs;
                # concurrent imports cannot cause endless backup restarts.
                live.execute("BEGIN")
                live.execute("SELECT rootpage FROM sqlite_master LIMIT 1").fetchone()
                live.backup(frozen, pages=512, progress=backup_callback)
            enter_stage("coverage_inventory")
            frozen.execute("PRAGMA journal_mode=DELETE")
            frozen.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
            prepare_coverage_inventory(frozen, persistent=True)
            enter_stage("coverage")
            access = build_project_access_context(
                frozen, auth_user=auth_user, auth_enabled=auth_enabled
            )
            conditions, params = _base_search_conditions(
                project_id=None,
                repository_id=None,
                remote=None,
                root=None,
                host=None,
                from_timestamp=None,
                to_timestamp=None,
                project_access=access,
            )
            coverage = coverage_readiness(
                _search_coverage(frozen, base_conditions=conditions, base_params=params)
            )
            enter_stage("metadata")
            metadata = {
                "created_at": datetime.fromtimestamp(created, UTC).isoformat(),
                "expires_at": datetime.fromtimestamp(job.expires, UTC).isoformat(),
                "index_generation": job.generation,
                "normalization_version": NORMALIZATION_VERSION,
                "index_versions": coverage["index_versions"],
                "coverage": coverage,
                "coverage_scope": "authorized_corpus_at_creation_before_query_filters",
            }
            frozen.execute(
                "CREATE TABLE evidence_snapshot_metadata (payload TEXT NOT NULL)"
            )
            frozen.execute(
                "INSERT INTO evidence_snapshot_metadata VALUES (?)",
                (
                    json.dumps(
                        {
                            "metadata": metadata,
                            "access": asdict(access),
                            "members": _visible_members(frozen, access),
                            "owner": owner,
                            "projects": sorted(_visible_projects(frozen, access)),
                        }
                    ),
                ),
            )
            frozen.commit()
        enter_stage("publish")
        path = directory / f"{job.generation}.sqlite3"
        os.replace(temporary, path)
        os.utime(path, (created, created))
        logger.info(
            "Evidence snapshot %s ready in %.3fs; stage timings=%s",
            job.generation,
            time.monotonic() - started,
            timings,
        )
    except Exception as exc:
        diagnostics = status()
        diagnostics["reason"] = _failure_reason(
            exc, timed_out=time.monotonic() > deadline
        )
        diagnostics["error_type"] = type(exc).__name__
        if isinstance(exc, sqlite3.Error):
            diagnostics["sqlite_errorname"] = getattr(
                exc, "sqlite_errorname", "SQLITE_ERROR"
            )
        if not isinstance(exc, HTTPException):
            logger.exception(
                "Evidence snapshot %s build failed: %s", job.generation, diagnostics
            )
        detail = (
            exc.detail
            if isinstance(exc, HTTPException)
            else {
                "code": "snapshot_build_failed",
                "message": f"Snapshot preparation failed during {stage}: {diagnostics['reason']}; start a new snapshot",
            }
        )
        _write_status(directory / f"{job.generation}.failed", {**detail, **diagnostics})
    finally:
        try:
            temporary.unlink(missing_ok=True)
            pending.unlink(missing_ok=True)
        finally:
            lock.close()
            job.done.set()


def _new_snapshot(settings, directory, signer, *, auth_user, auth_enabled, owner):
    key = (str(settings.database_path.resolve()), owner)
    with _BUILDS_LOCK:
        now = time.time()
        for old_key, old_job in list(_BUILDS.items()):
            if old_job.done.is_set() and old_job.expires <= now:
                _BUILDS.pop(old_key, None)
        job = _BUILDS.get(key)
        if job is None:
            lock = (directory / "lock").open("a+b")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                lock.close()
                raise _building()
            generation = secrets.token_hex(24)
            expires = now + SNAPSHOT_TTL_SECONDS
            snapshot_id = signer.dumps(
                {
                    "type": "snapshot",
                    "generation": generation,
                    "expires": expires,
                    "owner": owner,
                }
            )
            job = _SnapshotBuild(snapshot_id, generation, expires)
            try:
                fd = os.open(
                    directory / f"{generation}.pending",
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
                os.close(fd)
                _BUILDS[key] = job
                threading.Thread(
                    target=_build_snapshot,
                    args=(settings, directory, job, lock),
                    kwargs={
                        "auth_user": auth_user,
                        "auth_enabled": auth_enabled,
                        "owner": owner,
                        "created": now,
                    },
                    name="evidence-snapshot-builder",
                    daemon=True,
                ).start()
            except BaseException:
                _BUILDS.pop(key, None)
                lock.close()
                (directory / f"{generation}.pending").unlink(missing_ok=True)
                raise
    # Fast/small snapshots preserve the ordinary 200 response. Large builds
    # continue independently of client disconnects and return bounded retry info.
    job.done.wait(SNAPSHOT_REQUEST_WAIT_SECONDS)
    return job.snapshot_id


def _ready_path(directory, token, snapshot_id):
    generation = token["generation"]
    path = directory / f"{generation}.sqlite3"
    if path.exists():
        return path
    failed = directory / f"{generation}.failed"
    if failed.exists():
        raise HTTPException(status_code=503, detail=json.loads(failed.read_text()))
    pending = directory / f"{generation}.pending"
    if pending.exists():
        # A worker crash/restart must not leave a permanently pending snapshot.
        with (directory / "lock").open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                try:
                    progress = json.loads(pending.read_text() or "{}")
                except FileNotFoundError:
                    progress = {}
                raise _building(snapshot_id, **progress)
        # The builder may have completed between our first check and the lock.
        if path.exists():
            return path
        if failed.exists():
            raise HTTPException(status_code=503, detail=json.loads(failed.read_text()))
        raise api_error(
            503,
            "snapshot_build_interrupted",
            "Snapshot preparation was interrupted; create a new snapshot",
        )
    if path.exists():
        return path
    if failed.exists():
        raise HTTPException(status_code=503, detail=json.loads(failed.read_text()))
    raise api_error(410, "snapshot_expired", "Snapshot is no longer available")


@contextmanager
def evidence_snapshot(
    settings, *, auth_user, auth_enabled, snapshot_id=None, cursor=None
):
    directory, signer = _signer(settings)
    cursor_data = read_cursor(signer, cursor)
    if cursor_data:
        if snapshot_id and snapshot_id != cursor_data["snapshot_id"]:
            raise api_error(
                400, "cursor_mismatch", "Cursor belongs to a different snapshot"
            )
        snapshot_id = cursor_data["snapshot_id"]
    owner = str((auth_user or {}).get("user_id") or "local")
    if not snapshot_id:
        snapshot_id = _new_snapshot(
            settings,
            directory,
            signer,
            auth_user=auth_user,
            auth_enabled=auth_enabled,
            owner=owner,
        )
    # Validate before opening the live database or taking any creation lock.
    token = _validate_snapshot(signer, snapshot_id, owner)
    try:
        path = _ready_path(directory, token, snapshot_id)
    finally:
        key = (str(settings.database_path.resolve()), owner)
        with _BUILDS_LOCK:
            job = _BUILDS.get(key)
            if job and job.snapshot_id == snapshot_id and job.done.is_set():
                _BUILDS.pop(key, None)
    with closing(connect(settings.database_path)) as live, closing(
        sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    ) as frozen:
        current_access = build_project_access_context(
            live, auth_user=auth_user, auth_enabled=auth_enabled
        )
        frozen.row_factory = sqlite3.Row
        stored = json.loads(
            frozen.execute("SELECT payload FROM evidence_snapshot_metadata").fetchone()[
                0
            ]
        )
        if stored["metadata"][
            "normalization_version"
        ] != NORMALIZATION_VERSION or stored["metadata"]["index_versions"] != {
            "turn": TURN_INDEX_VERSION,
            "turn_search": TURN_SEARCH_VERSION,
            "search_chunk": SEARCH_CHUNK_VERSION,
        }:
            raise api_error(
                409,
                "snapshot_version_mismatch",
                "Snapshot uses an unsupported normalization or index version",
            )
        current_members = _visible_members(live, current_access)
        if not set(stored["projects"]).issubset(
            _visible_projects(live, current_access)
        ) or any(
            sid not in current_members or current_members[sid] != pid
            for sid, pid in stored["members"].items()
        ):
            raise api_error(
                403,
                "snapshot_access_revoked",
                "Snapshot access scope has shrunk or changed; create a new snapshot",
            )
        metadata = {**stored["metadata"], "snapshot_id": snapshot_id}
        yield (
            frozen,
            ProjectAccessContext(**stored["access"]),
            metadata,
            signer,
            cursor_data,
        )
