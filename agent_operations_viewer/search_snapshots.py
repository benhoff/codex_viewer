"""Durable, expiring SQLite read snapshots for evidence research.

Generations are private shared database backups, never long-lived connections
against the live WAL. Owner-bound handles fail closed when their scope shrinks.
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
from pathlib import Path

from fastapi import HTTPException
from itsdangerous import BadData, URLSafeSerializer

from .db import connect
from .search_budget import search_work_budget
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
# Reuse recent ready generations; every handle still shares the fixed expiry.
SNAPSHOT_REUSE_SECONDS = 300
GENERATION_FORMAT = 1
MAX_SNAPSHOTS = 32
NORMALIZATION_VERSION = "evidence-2"
SNAPSHOT_REQUEST_WAIT_SECONDS = 1.0
# A full production backup alone can take about five minutes. Preparation has
# its own allowance; ready snapshots receive a separate useful read lifetime.
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


def _snapshot_directories(settings):
    legacy = (settings.database_path.parent / "search-snapshots").resolve()
    configured = getattr(settings, "search_snapshot_dir", None)
    directory = Path(configured).expanduser().resolve() if configured else legacy
    return directory, legacy


def _signer(settings):
    directory, legacy = _snapshot_directories(settings)
    directory.mkdir(mode=0o700, exist_ok=True)
    # Keep the signing key and builder lock stable when moving large snapshot
    # files to faster storage. Existing IDs and cross-process coordination survive.
    legacy.mkdir(mode=0o700, exist_ok=True)
    key_path = legacy / "signing-key"
    # Existing keys need no lock. Initial publication is atomic, so even a
    # simultaneous first request never observes a partial signing key.
    if not key_path.exists():
        fd, temporary = tempfile.mkstemp(dir=legacy, prefix="key-")
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
    started: float = field(default_factory=lambda: time.monotonic())
    done: threading.Event = field(default_factory=threading.Event)


_BUILDS: dict[tuple[str, str], _SnapshotBuild] = {}
_BUILDS_LOCK = threading.Lock()


def _public_progress(progress, *, running=False):
    progress = dict(progress)
    recorded = progress.pop("_recorded_monotonic", None)
    if running and recorded is not None:
        age = max(0.0, time.monotonic() - recorded)
        progress["progress_age_seconds"] = round(age, 3)
        for name in ("elapsed_seconds", "stage_elapsed_seconds"):
            progress[name] = round(progress[name] + age, 3)
    return progress


def _building(snapshot_id=None, **progress):
    progress = _public_progress(progress, running=True)
    if snapshot_id and progress.get("elapsed_seconds", 0) >= progress.get("budget_seconds", float("inf")):
        progress["stage_timings_seconds"] = {
            **progress.get("stage_timings_seconds", {}),
            progress["stage"]: progress["stage_elapsed_seconds"],
        }
        return api_error(
            503, "snapshot_build_failed",
            "Snapshot preparation exceeded its time budget. Storage work may still be stopping; start a new snapshot after the builder releases capacity.",
            **progress,
            reason="deadline_exceeded",
            worker_stopping=True,
        )
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
        handle = token.get("handle")
        if handle is not None and (
            not isinstance(handle, str) or len(handle) != 48
            or any(c not in "0123456789abcdef" for c in handle)
        ):
            raise BadData("Invalid handle")
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
    captured = None
    started = job.started
    deadline = started + SNAPSHOT_BUILD_TIMEOUT_SECONDS
    stage = "queued"
    stage_started = started
    timings = {}
    backup_progress = {}
    last_progress = started

    def status():
        now = time.monotonic()
        return {
            **_generation_contract(),
            "build_id": job.generation,
            "stage": stage,
            "elapsed_seconds": round(now - started, 3),
            "stage_elapsed_seconds": round(now - stage_started, 3),
            "stage_timings_seconds": dict(timings),
            "budget_seconds": SNAPSHOT_BUILD_TIMEOUT_SECONDS,
            "created_at": datetime.fromtimestamp(created, UTC).isoformat(),
            "progress_updated_at": datetime.now(UTC).isoformat(),
            "_recorded_monotonic": now,
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
            backup_pages_copied=total - remaining, backup_pages_total=total,
            backup_progress_scope="database_copy_only",
        )
        now = time.monotonic()
        if now - last_progress >= 2 or remaining == 0:
            _write_status(pending, status())
            last_progress = now
        check_deadline()

    try:
        enter_stage("cleanup")
        # This lock only serializes builders. Readers/token validation never wait
        # for it. Clean up generated artifacts after expiry, not live generations.
        directories = set(_snapshot_directories(settings))
        for location in directories:
            for pattern in ("*.sqlite3", "*.ready", "*.pending", "*.failed", "*.creating"):
                for path in location.glob(pattern):
                    if (
                        path != temporary
                        and path != pending
                        and created - path.stat().st_mtime > SNAPSHOT_TTL_SECONDS
                    ):
                        path.unlink()
        for location in directories:
            for path in location.glob("*.handle"):
                try:
                    if json.loads(path.read_text())["expires"] <= created:
                        path.unlink(missing_ok=True)
                        path.with_suffix(".handle-lock").unlink(missing_ok=True)
                except FileNotFoundError:
                    pass
        if sum(len(list(location.glob("*.sqlite3"))) for location in directories) >= MAX_SNAPSHOTS:
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
                captured = time.time()
                live.backup(frozen, pages=512, progress=backup_callback)
            enter_stage("coverage_inventory")
            frozen.execute("PRAGMA journal_mode=DELETE")
            frozen.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
            prepare_coverage_inventory(frozen, persistent=True)
            enter_stage("coverage")
            # The physical generation is owner-neutral. Each handle gets its
            # own current authorization scope after publication.
            access = ProjectAccessContext(False, True, None, {})
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
                **_generation_contract(),
                "created_at": datetime.fromtimestamp(created, UTC).isoformat(),
                "captured_at": datetime.fromtimestamp(captured, UTC).isoformat(),
                # Final timing cannot be written here without excluding this
                # database's commit/close/publication. A ready manifest follows.
                "preparation_status_version": 1,
                "index_generation": job.generation,
                "normalization_version": NORMALIZATION_VERSION,
                "index_versions": coverage["index_versions"],
            }
            frozen.execute(
                "CREATE TABLE evidence_generation_metadata (payload TEXT NOT NULL)"
            )
            frozen.execute(
                "INSERT INTO evidence_generation_metadata VALUES (?)",
                (
                    json.dumps(
                        {
                            "metadata": metadata,
                        }
                    ),
                ),
            )
            frozen.commit()
        enter_stage("publish")
        path = directory / f"{job.generation}.sqlite3"
        os.replace(temporary, path)
        check_deadline()
        if (directory / f"{job.generation}.failed").exists():
            raise TimeoutError("Snapshot deadline was reported while publication was in progress")
        completed = status()
        timings[stage] = completed["stage_elapsed_seconds"]
        ready = time.time()
        preparation = {
            "elapsed_seconds": completed["elapsed_seconds"],
            "stage_timings_seconds": dict(timings),
            "budget_seconds": SNAPSHOT_BUILD_TIMEOUT_SECONDS,
            "timing_scope": "accepted_build_to_database_publication",
        }
        manifest = {
            **_generation_contract(),
            "captured_at": datetime.fromtimestamp(captured, UTC).isoformat(),
            "ready_at": datetime.fromtimestamp(ready, UTC).isoformat(),
            "expires_at": datetime.fromtimestamp(
                min(job.expires, ready + SNAPSHOT_TTL_SECONDS), UTC
            ).isoformat(),
            "preparation": preparation,
        }
        # Cleanup must use the ready lifetime, not time consumed by preparation.
        os.utime(path, (ready, ready))
        ready_path = directory / f"{job.generation}.ready"
        _write_status(ready_path, manifest)
        os.utime(ready_path, (ready, ready))
        logger.info(
            "Evidence snapshot %s ready in %.3fs; stage timings=%s",
            job.generation,
            preparation["elapsed_seconds"],
            timings,
        )
    except Exception as exc:
        diagnostics = _public_progress(status())
        diagnostics["stage_timings_seconds"][stage] = diagnostics["stage_elapsed_seconds"]
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


def _generation_contract():
    return {
        "generation_format": GENERATION_FORMAT,
        "normalization_version": NORMALIZATION_VERSION,
        "index_versions": {
            "turn": TURN_INDEX_VERSION,
            "turn_search": TURN_SEARCH_VERSION,
            "search_chunk": SEARCH_CHUNK_VERSION,
        },
    }


def _compatible_generation(status):
    return isinstance(status, dict) and all(
        status.get(key) == value for key, value in _generation_contract().items()
    )


def _generation_candidate(directory, *, pending=False):
    now = time.time()
    suffix = ".pending" if pending else ".ready"
    candidates = []
    for path in directory.glob("*" + suffix):
        if (directory / f"{path.stem}.failed").exists():
            continue
        try:
            status = json.loads(path.read_text())
            if not _compatible_generation(status):
                continue
            if pending:
                created = datetime.fromisoformat(status["created_at"]).timestamp()
                expires = created + SNAPSHOT_BUILD_TIMEOUT_SECONDS + SNAPSHOT_TTL_SECONDS
                usable = now < created + SNAPSHOT_BUILD_TIMEOUT_SECONDS
                order = created
            else:
                ready = datetime.fromisoformat(status["ready_at"]).timestamp()
                expires = datetime.fromisoformat(status["expires_at"]).timestamp()
                usable = (0 <= now - ready < SNAPSHOT_REUSE_SECONDS and now < expires
                          and (directory / f"{path.stem}.sqlite3").exists())
                order = ready
            if usable:
                candidates.append((order, path.stem, expires))
        except (FileNotFoundError, ValueError, KeyError, TypeError):
            continue
    return max(candidates) if candidates else None


def _issue_handle(directory, signer, generation, expires, owner, reuse):
    handle = secrets.token_hex(24)
    token = {"type": "snapshot", "generation": generation, "handle": handle,
             "expires": expires, "owner": owner}
    _write_status(directory / f"{generation}.{handle}.handle", {
        **token,
        "handle_created_at": datetime.now(UTC).isoformat(),
        "generation_reuse": reuse,
    })
    return signer.dumps(token)


def _new_snapshot(settings, directory, signer, *, auth_user, auth_enabled, owner,
                  fresh_snapshot=False):
    key = (str(settings.database_path.resolve()), owner)
    with _BUILDS_LOCK:
        now = time.time()
        for old_key, old_job in list(_BUILDS.items()):
            if old_job.done.is_set():
                _BUILDS.pop(old_key, None)
        candidate = None if fresh_snapshot else _generation_candidate(directory)
        if candidate:
            _, generation, expires = candidate
            return _issue_handle(directory, signer, generation, expires, owner, "ready")
        lock = (_snapshot_directories(settings)[1] / "lock").open("a+b")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            # Disk status, not the process cache, coordinates other workers/users.
            candidate = _generation_candidate(directory, pending=True)
            if candidate:
                _, generation, expires = candidate
                return _issue_handle(directory, signer, generation, expires, owner, "building")
            raise _building()
        # Publication could have completed between the first scan and flock.
        candidate = None if fresh_snapshot else _generation_candidate(directory)
        if candidate:
            lock.close()
            _, generation, expires = candidate
            return _issue_handle(directory, signer, generation, expires, owner, "ready")
        generation = secrets.token_hex(24)
        expires = now + SNAPSHOT_BUILD_TIMEOUT_SECONDS + SNAPSHOT_TTL_SECONDS
        try:
            snapshot_id = _issue_handle(directory, signer, generation, expires, owner, "created")
            job = _SnapshotBuild(snapshot_id, generation, expires)
            _write_status(directory / f"{generation}.pending", {
                **_generation_contract(),
                "build_id": generation,
                "stage": "queued",
                "created_at": datetime.fromtimestamp(now, UTC).isoformat(),
                "progress_updated_at": datetime.fromtimestamp(now, UTC).isoformat(),
                "elapsed_seconds": 0.0,
                "stage_elapsed_seconds": 0.0,
                "stage_timings_seconds": {},
                "budget_seconds": SNAPSHOT_BUILD_TIMEOUT_SECONDS,
                "_recorded_monotonic": job.started,
            })
            _BUILDS[key] = job
            threading.Thread(
                target=_build_snapshot,
                args=(settings, directory, job, lock),
                kwargs={"auth_user": None, "auth_enabled": False, "owner": None, "created": now},
                name="evidence-snapshot-builder", daemon=True,
            ).start()
        except BaseException:
            _BUILDS.pop(key, None)
            lock.close()
            (directory / f"{generation}.pending").unlink(missing_ok=True)
            raise
    job.done.wait(SNAPSHOT_REQUEST_WAIT_SECONDS)
    return snapshot_id


def _ready_path(directory, token, snapshot_id, *, lock_directory=None):
    generation = token["generation"]
    path = directory / f"{generation}.sqlite3"
    ready = directory / f"{generation}.ready"
    failed = directory / f"{generation}.failed"
    if failed.exists():
        raise HTTPException(status_code=503, detail=json.loads(failed.read_text()))
    if path.exists() and ready.exists():
        return path
    pending = directory / f"{generation}.pending"
    if pending.exists():
        # A worker crash/restart must not leave a permanently pending snapshot.
        with ((lock_directory or directory) / "lock").open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                try:
                    progress = json.loads(pending.read_text() or "{}")
                except FileNotFoundError:
                    progress = {}
                error = _building(snapshot_id, **progress)
                if error.detail["code"] == "snapshot_build_failed":
                    # The backup callback cannot run during blocked disk I/O.
                    # Polling still reports a terminal timeout on schedule, and
                    # a late worker completion must not revive this generation.
                    _write_status(failed, error.detail)
                raise error
        # The builder may have completed between our first check and the lock.
        if path.exists() and ready.exists():
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


def _install_handle_scope(connection, stored):
    """Enforce exact session membership before any endpoint reads the shared file."""
    connection.execute("CREATE TEMP TABLE evidence_authorized_projects (project_id TEXT PRIMARY KEY)")
    connection.executemany("INSERT INTO evidence_authorized_projects VALUES (?)",
                           ((pid,) for pid in stored["projects"]))
    connection.execute("CREATE TEMP TABLE evidence_authorized_sessions (session_id TEXT PRIMARY KEY)")
    connection.executemany("INSERT INTO evidence_authorized_sessions VALUES (?)",
                           ((sid,) for sid in stored["members"]))
    # A project's visibility or a session's assignment may have changed since
    # capture. Filter the immutable corpus to the handle's exact membership.
    for table in ("sessions", "evidence_coverage_sessions"):
        connection.execute(f"""CREATE TEMP VIEW {table} AS
            SELECT s.* FROM main.{table} s JOIN evidence_authorized_sessions a
            ON a.session_id = s.id""")


def _handle_scope(directory, token, snapshot_id, frozen, metadata, current_members,
                  current_projects, *, auth_enabled):
    path = directory / f"{token['generation']}.{token['handle']}.handle"
    try:
        stored = json.loads(path.read_text())
    except FileNotFoundError as exc:
        raise api_error(410, "snapshot_expired", "Snapshot handle is no longer available") from exc
    if stored["owner"] != token["owner"] or stored["generation"] != token["generation"]:
        raise api_error(409, "invalid_snapshot", "Snapshot handle does not match its generation")
    if "access" in stored:
        _install_handle_scope(frozen, stored)
        return stored
    # Concurrent retries of one handle must observe one immutable grant record.
    lock_path = path.with_suffix(".handle-lock")
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, "a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise _building(snapshot_id, stage="authorization")
        stored = json.loads(path.read_text())
        if "access" not in stored:
            all_access = ProjectAccessContext(False, True, None, {})
            projects = sorted(_visible_projects(frozen, all_access) & current_projects)
            captured_members = _visible_members(frozen, all_access)
            if any(sid in current_members and current_members[sid] != pid
                   for sid, pid in captured_members.items()):
                # Omitting a still-visible, reassigned session could turn a
                # negative query into a misleading completeness claim.
                raise api_error(
                    409, "snapshot_scope_changed",
                    "Visible sessions changed project since capture; request fresh_snapshot=true without a snapshot_id or cursor",
                )
            members = {
                sid: pid for sid, pid in captured_members.items()
                if sid in current_members and current_members[sid] == pid
                and (pid is None or pid in projects)
            }
            access = ProjectAccessContext(
                auth_enabled, False, token["owner"],
                {pid: "viewer" for pid in projects}, snapshot_scope=True,
            )
            stored.update(access=asdict(access), projects=projects, members=members)
            _install_handle_scope(frozen, stored)
            condition, params = project_access_condition_sql(access)
            coverage = coverage_readiness(_search_coverage(
                frozen, base_conditions=[condition], base_params=params,
            ))
            stored["metadata"] = {
                **metadata,
                "handle_created_at": stored["handle_created_at"],
                "authorized_at": datetime.now(UTC).isoformat(),
                "generation_reuse": stored["generation_reuse"],
                "generation_shared": True,
                "coverage": coverage,
                "coverage_scope": "authorized_corpus_at_handle_creation_before_query_filters",
            }
            _write_status(path, stored)
        else:
            _install_handle_scope(frozen, stored)
    return stored


@contextmanager
def evidence_snapshot(
    settings, *, auth_user, auth_enabled, snapshot_id=None, cursor=None,
    fresh_snapshot=False,
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
            fresh_snapshot=fresh_snapshot,
        )
    # Validate before opening the live database or taking any creation lock.
    token = _validate_snapshot(signer, snapshot_id, owner)
    _, legacy = _snapshot_directories(settings)
    # Previously issued IDs keep finding snapshots made before relocation.
    if directory != legacy and not any(
        (directory / f"{token['generation']}{suffix}").exists()
        for suffix in (".sqlite3", ".pending", ".failed", ".creating")
    ):
        if any((legacy / f"{token['generation']}{suffix}").exists()
               for suffix in (".sqlite3", ".pending", ".failed", ".creating")):
            directory = legacy
    try:
        path = _ready_path(directory, token, snapshot_id, lock_directory=legacy)
    finally:
        key = (str(settings.database_path.resolve()), owner)
        with _BUILDS_LOCK:
            job = _BUILDS.get(key)
            if job and job.snapshot_id == snapshot_id and job.done.is_set():
                _BUILDS.pop(key, None)
    with closing(connect(settings.database_path)) as live, closing(
        sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    ) as frozen, search_work_budget(live, frozen, generation=token["generation"]):
        live.execute("BEGIN")
        current_access = build_project_access_context(
            live, auth_user=auth_user, auth_enabled=auth_enabled
        )
        frozen.row_factory = sqlite3.Row
        shared = "handle" in token
        table = "evidence_generation_metadata" if shared else "evidence_snapshot_metadata"
        stored = json.loads(frozen.execute(f"SELECT payload FROM {table}").fetchone()[0])
        if stored["metadata"].get("preparation_status_version") == 1:
            try:
                manifest = json.loads(
                    (directory / f"{token['generation']}.ready").read_text()
                )
            except FileNotFoundError as exc:
                raise api_error(
                    503, "snapshot_build_interrupted",
                    "Snapshot publication was interrupted; create a new snapshot",
                ) from exc
            stored["metadata"].update(manifest)
        if (
            datetime.fromisoformat(stored["metadata"]["expires_at"]).timestamp()
            <= time.time()
        ):
            raise api_error(410, "snapshot_expired", "Snapshot expired")
        if (shared and not _compatible_generation(stored["metadata"])) or stored["metadata"][
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
        current_projects = _visible_projects(live, current_access)
        if shared:
            stored = _handle_scope(
                directory, token, snapshot_id, frozen, stored["metadata"],
                current_members, current_projects, auth_enabled=auth_enabled,
            )
        if not set(stored["projects"]).issubset(current_projects) or any(
            sid not in current_members or current_members[sid] != pid
            for sid, pid in stored["members"].items()
        ):
            raise api_error(
                403,
                "snapshot_access_revoked",
                "Snapshot access scope has shrunk or changed; create a new snapshot",
            )
        if shared:
            # The shared file has no owner metadata. Keep its per-handle coverage
            # cache private to this connection, including for filtered endpoints.
            frozen.execute("CREATE TEMP TABLE evidence_snapshot_metadata (payload TEXT NOT NULL)")
            frozen.execute("INSERT INTO evidence_snapshot_metadata VALUES (?)", (json.dumps(stored),))
        metadata = {**stored["metadata"], "snapshot_id": snapshot_id}
        yield (
            frozen,
            ProjectAccessContext(**stored["access"]),
            metadata,
            signer,
            cursor_data,
        )
