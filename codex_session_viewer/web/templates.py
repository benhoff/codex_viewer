from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from urllib.parse import urlencode

from fastapi import Request
from fastapi.templating import Jinja2Templates

from ..markdown_utils import render_markdown
from ..db import connect
from ..saved_turns import count_saved_turns, owner_scope_from_request
from ..session_view import full_timestamp, humanize_timestamp
from ..text_utils import shorten
from .context import get_app_context


PACKAGE_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_ROOT = PACKAGE_ROOT / "templates"
STATIC_ROOT = PACKAGE_ROOT / "static"


@lru_cache(maxsize=128)
def asset_version_key(path: str, app_version: str) -> str:
    candidate = STATIC_ROOT / path.lstrip("/")
    if candidate.exists():
        return f"{app_version}-{candidate.stat().st_mtime_ns}"
    return app_version


def versioned_static_url(request: Request, path: str, app_version: str) -> str:
    base_url = str(request.url_for("static", path=path))
    return f"{base_url}?{urlencode({'v': asset_version_key(path, app_version)})}"


def build_templates(app_version: str) -> Jinja2Templates:
    templates = Jinja2Templates(directory=str(TEMPLATE_ROOT))
    env = templates.env
    env.filters["shorten"] = shorten
    env.filters["humanize_timestamp"] = humanize_timestamp
    env.filters["full_timestamp"] = full_timestamp
    env.filters["render_markdown"] = render_markdown
    env.globals["review_queue_open_count"] = review_queue_open_count
    env.globals["static_asset_url"] = (
        lambda request, path: versioned_static_url(request, path, app_version)
    )
    return templates


def review_queue_open_count(request: Request) -> int:
    cached = getattr(request.state, "_review_queue_open_count", None)
    if isinstance(cached, int):
        return cached
    try:
        context = get_app_context(request)
    except RuntimeError:
        return 0
    with connect(context.settings.database_path) as connection:
        count = count_saved_turns(connection, owner_scope_from_request(request), status="open")
    request.state._review_queue_open_count = count
    return count
