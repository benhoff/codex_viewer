from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from ..config import Settings
from ..db import init_db
from ..importer import sync_sessions
from .auth import install_auth
from .context import AppContext, set_app_context
from .routes.pages import router as pages_router
from .routes.projects import router as projects_router
from .routes.sessions import router as sessions_router
from .routes.sync_api import router as sync_api_router
from .templates import STATIC_ROOT, build_templates


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or Settings.from_env(PROJECT_ROOT)
    app_settings.ensure_directories()
    init_db(app_settings.database_path)
    STATIC_ROOT.mkdir(parents=True, exist_ok=True)

    app = FastAPI(title="Codex Session Viewer", version=app_settings.app_version)
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
