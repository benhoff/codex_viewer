from __future__ import annotations

from urllib.parse import parse_qs

from fastapi import Request


async def parse_form_fields(request: Request) -> dict[str, str]:
    body = await request.body()
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}
