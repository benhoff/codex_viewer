from __future__ import annotations

from dataclasses import dataclass

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

from ..config import Settings


@dataclass(slots=True)
class AppContext:
    settings: Settings
    templates: Jinja2Templates


def set_app_context(app: FastAPI, context: AppContext) -> None:
    app.state.codex_viewer_context = context


def get_app_context(request: Request) -> AppContext:
    context = getattr(request.app.state, "codex_viewer_context", None)
    if not isinstance(context, AppContext):
        raise RuntimeError("App context has not been initialized")
    return context


def get_settings(request: Request) -> Settings:
    return get_app_context(request).settings


def get_templates(request: Request) -> Jinja2Templates:
    return get_app_context(request).templates


def request_return_to(request: Request) -> str:
    return str(request.url.path) + (f"?{request.url.query}" if request.url.query else "")
