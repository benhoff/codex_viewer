from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from ..config import Settings
from ..db import connect, init_db
from ..importer import sync_sessions
from ..local_auth import fetch_auth_status
from ..saved_turns import migrate_global_saved_turns_to_owner
from ..server_settings import apply_server_settings
from .auth import install_auth
from .context import AppContext, set_app_context
from .routes.pages import router as pages_router
from .routes.projects import router as projects_router
from .routes.sessions import router as sessions_router
from .routes.sync_api import router as sync_api_router
from .templates import STATIC_ROOT, build_templates


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_app(
    settings: Settings | None = None,
    *,
    preserve_sync_on_start: bool = False,
) -> FastAPI:
    app_settings = settings or Settings.from_env(PROJECT_ROOT)
    app_settings.ensure_directories()
    init_db(app_settings.database_path)
    with connect(app_settings.database_path) as connection:
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
    install_auth(app, app_settings)
    templates = build_templates(app_settings.app_version)
    set_app_context(app, AppContext(settings=app_settings, templates=templates))

    app.mount(
        "/static",
        StaticFiles(directory=str(STATIC_ROOT), check_dir=False),
        name="static",
    )

    @app.on_event("startup")
    def startup_sync() -> None:
        if app_settings.sync_on_start and app_settings.sync_mode == "local":
            sync_sessions(app_settings)

    app.include_router(pages_router)
    app.include_router(projects_router)
    app.include_router(sessions_router)
    app.include_router(sync_api_router)
    return app
