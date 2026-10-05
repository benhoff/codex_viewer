from __future__ import annotations

import faulthandler
import logging
from pathlib import Path
import signal
import threading

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from ..config import Settings
from ..db import connection_scope, init_db, run_db_backfills
from ..importer import sync_sessions
from ..local_auth import fetch_auth_status
from ..session_artifacts import prune_orphaned_session_artifacts
from ..saved_turns import migrate_global_saved_turns_to_owner
from ..server_settings import apply_server_settings
from .auth import install_auth
from .concurrency import UploadAdmissionMiddleware, WorkQueueFull
from .context import AppContext, set_app_context
from .routes.machine_pairing import router as machine_pairing_router
from .routes.pages import router as pages_router
from .routes.projects import router as projects_router
from .routes.search_api import router as search_api_router
from .routes.sessions import router as sessions_router
from .routes.assessments import router as assessments_router
from .routes.sync_api import router as sync_api_router
from .templates import STATIC_ROOT, build_templates


PROJECT_ROOT = Path(__file__).resolve().parents[2]
logger = logging.getLogger("agent_operations_viewer.web.app")


def _install_stack_dump_signal() -> None:
    stack_signal = getattr(signal, "SIGUSR1", None)
    if stack_signal is None:
        return
    try:
        faulthandler.register(stack_signal, all_threads=True)
    except (OSError, RuntimeError, ValueError):
        logger.warning("Could not register the SIGUSR1 stack-dump handler", exc_info=True)


def _run_post_startup_maintenance(settings: Settings) -> None:
    try:
        run_db_backfills(settings.database_path)
    except Exception:
        logger.exception("Post-startup database backfills failed")

    if settings.sync_on_start and settings.sync_mode == "local":
        try:
            sync_sessions(settings)
        except Exception:
            logger.exception("Post-startup session sync failed")


def _start_post_startup_maintenance(settings: Settings) -> threading.Thread:
    worker = threading.Thread(
        target=_run_post_startup_maintenance,
        args=(settings,),
        name="history-post-startup-maintenance",
        daemon=True,
    )
    worker.start()
    return worker


def _run_artifact_maintenance(settings: Settings, stop: threading.Event) -> None:
    # A full disk walk belongs in infrequent maintenance, not every upload.
    while not stop.wait(3600):
        try:
            prune_orphaned_session_artifacts(settings)
        except Exception:
            logger.exception("Artifact maintenance failed")


def create_app(
    settings: Settings | None = None,
    *,
    preserve_sync_on_start: bool = False,
) -> FastAPI:
    _install_stack_dump_signal()
    app_settings = settings or Settings.from_env(PROJECT_ROOT)
    app_settings.ensure_directories()
    # Endpoints require current tables and columns, but derived session data is
    # resumable and can be expensive enough to trip container health checks.
    init_db(app_settings.database_path, defer_backfills=True)
    with connection_scope(app_settings.database_path) as connection:
        with connection:
            apply_server_settings(
                connection,
                app_settings,
                preserve_sync_on_start=preserve_sync_on_start,
            )
            if app_settings.auth_enabled():
                auth_status = fetch_auth_status(connection)
                if auth_status.admin_user and auth_status.admin_user.get("id"):
                    migrate_global_saved_turns_to_owner(
                        connection,
                        owner_scope=str(auth_status.admin_user["id"]),
                    )
    STATIC_ROOT.mkdir(parents=True, exist_ok=True)

    app = FastAPI(title="Agent Operations Viewer", version=app_settings.app_version)

    @app.exception_handler(WorkQueueFull)
    async def work_queue_full_response(
        _request: Request,
        exc: WorkQueueFull,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": str(exc), "retryable": True},
            headers={"Retry-After": "5"},
        )

    install_auth(app, app_settings)
    app.add_middleware(UploadAdmissionMiddleware)
    templates = build_templates(app_settings.app_version)
    set_app_context(app, AppContext(settings=app_settings, templates=templates))

    app.mount(
        "/static",
        StaticFiles(directory=str(STATIC_ROOT), check_dir=False),
        name="static",
    )

    @app.on_event("startup")
    def start_post_startup_maintenance() -> None:
        # Do not join this worker: Uvicorn must be able to finish startup and
        # serve /api/health while historical sessions are being indexed.
        app.state.post_startup_maintenance_thread = _start_post_startup_maintenance(
            app_settings
        )
        app.state.artifact_maintenance_stop = threading.Event()
        app.state.artifact_maintenance_thread = threading.Thread(
            target=_run_artifact_maintenance,
            args=(app_settings, app.state.artifact_maintenance_stop),
            name="artifact-maintenance", daemon=True,
        )
        app.state.artifact_maintenance_thread.start()

    @app.on_event("shutdown")
    def stop_artifact_maintenance() -> None:
        app.state.artifact_maintenance_stop.set()
        app.state.artifact_maintenance_thread.join(timeout=1)

    app.include_router(pages_router)
    app.include_router(search_api_router)
    app.include_router(sessions_router)
    app.include_router(assessments_router)
    app.include_router(sync_api_router)
    app.include_router(machine_pairing_router)
    app.include_router(projects_router)
    return app
