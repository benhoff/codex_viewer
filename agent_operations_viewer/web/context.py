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


APPROVAL_REVIEWS_COOKIE = "aov_show_approval_reviews"


def approval_reviews_visible(request: Request) -> bool:
    return request.cookies.get(APPROVAL_REVIEWS_COOKIE) == "1"


def approval_reviews_return_to(request: Request) -> str:
    # Restart pagination when the visible result set changes; keep search/view.
    from starlette.datastructures import QueryParams

    query = QueryParams([(k, v) for k, v in request.query_params.multi_items()
                         if k not in {"page", "sessions_page", "turns_page"}])
    return request.url.path + (f"?{query}" if query else "")
