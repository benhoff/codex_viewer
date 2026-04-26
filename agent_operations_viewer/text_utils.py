from __future__ import annotations

import re


def shorten(text: str, limit: int = 240) -> str:
    clean = " ".join(text.split())
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "…"


def strip_codex_wrappers(text: str) -> str:
    cleaned = text
    cleaned = re.sub(r"<environment_context>.*?</environment_context>", " ", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"<turn_aborted>.*?</turn_aborted>", " ", cleaned, flags=re.DOTALL)
    return " ".join(cleaned.split()).strip()


def strip_codex_wrappers_preserve_layout(text: str) -> str:
    cleaned = text
    cleaned = re.sub(r"<environment_context>.*?</environment_context>", "", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"<turn_aborted>.*?</turn_aborted>", "", cleaned, flags=re.DOTALL)
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()
