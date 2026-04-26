from __future__ import annotations

import re
from html import escape, unescape
from urllib.parse import urlparse

from markupsafe import Markup


_FENCE_RE = re.compile(r"^(```+|~~~+)\s*([A-Za-z0-9_+-]*)\s*$")
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")
_UNORDERED_RE = re.compile(r"^\s*[-+*]\s+(.*)$")
_ORDERED_RE = re.compile(r"^\s*(\d+)\.\s+(.*)$")
_BLOCKQUOTE_RE = re.compile(r"^>\s?(.*)$")
_CODE_SPAN_RE = re.compile(r"`([^`\n]+)`")
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_BOLD_RE = re.compile(r"(\*\*|__)(.+?)\1")
_ITALIC_RE = re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)|(?<!_)_([^_\n]+)_(?!_)")


def render_markdown(value: str | None) -> Markup:
    source = (value or "").strip()
    if not source:
        return Markup("")

    lines = source.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    blocks: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        if not stripped:
            index += 1
            continue

        fence_match = _FENCE_RE.match(line)
        if fence_match:
            fence, language = fence_match.groups()
            index += 1
            code_lines: list[str] = []
            while index < len(lines):
                current = lines[index]
                if current.startswith(fence):
                    index += 1
                    break
                code_lines.append(current)
                index += 1
            language_attr = (
                f' class="language-{escape(language, quote=True)}"' if language else ""
            )
            blocks.append(
                f"<pre><code{language_attr}>{escape(chr(10).join(code_lines))}</code></pre>"
            )
            continue

        heading_match = _HEADING_RE.match(line)
        if heading_match:
            hashes, content = heading_match.groups()
            level = min(len(hashes), 6)
            blocks.append(f"<h{level}>{_render_inline(content)}</h{level}>")
            index += 1
            continue

        if _BLOCKQUOTE_RE.match(line):
            quote_lines: list[str] = []
            while index < len(lines):
                match = _BLOCKQUOTE_RE.match(lines[index])
                if not match:
                    break
                quote_lines.append(match.group(1))
                index += 1
            inner = render_markdown("\n".join(quote_lines))
            blocks.append(f"<blockquote>{inner}</blockquote>")
            continue

        if _UNORDERED_RE.match(line):
            items, _, index = _collect_list(lines, index, ordered=False)
            blocks.append(
                "<ul>" + "".join(f"<li>{_render_inline(item)}</li>" for item in items) + "</ul>"
            )
            continue

        if _ORDERED_RE.match(line):
            items, start_number, index = _collect_list(lines, index, ordered=True)
            start_attr = f' start="{start_number}"' if start_number and start_number > 1 else ""
            blocks.append(
                f"<ol{start_attr}>"
                + "".join(f"<li>{_render_inline(item)}</li>" for item in items)
                + "</ol>"
            )
            continue

        paragraph_lines: list[str] = []
        while index < len(lines):
            current = lines[index]
            if not current.strip():
                break
            if (
                _FENCE_RE.match(current)
                or _HEADING_RE.match(current)
                or _BLOCKQUOTE_RE.match(current)
                or _UNORDERED_RE.match(current)
                or _ORDERED_RE.match(current)
            ):
                break
            paragraph_lines.append(current)
            index += 1
        blocks.append(f"<p>{_render_inline(chr(10).join(paragraph_lines))}</p>")

    return Markup("\n".join(blocks))


def _collect_list(lines: list[str], index: int, ordered: bool) -> tuple[list[str], int | None, int]:
    pattern = _ORDERED_RE if ordered else _UNORDERED_RE
    other_pattern = _UNORDERED_RE if ordered else _ORDERED_RE
    items: list[str] = []
    current_item: list[str] = []
    start_number: int | None = None

    while index < len(lines):
        line = lines[index]
        match = pattern.match(line)
        if match:
            if current_item:
                items.append("\n".join(current_item).strip())
            if ordered and start_number is None:
                start_number = int(match.group(1))
            content_index = 2 if ordered else 1
            current_item = [match.group(content_index).strip()]
            index += 1
            continue

        if not current_item:
            break

        if not line.strip():
            lookahead = index + 1
            while lookahead < len(lines) and not lines[lookahead].strip():
                lookahead += 1
            if lookahead < len(lines) and pattern.match(lines[lookahead]):
                items.append("\n".join(current_item).strip())
                current_item = []
                index = lookahead
                continue
            index = lookahead
            break

        if (
            pattern.match(line)
            or other_pattern.match(line)
            or _FENCE_RE.match(line)
            or _HEADING_RE.match(line)
            or _BLOCKQUOTE_RE.match(line)
        ):
            break

        current_item.append(line.strip())
        index += 1

    if current_item:
        items.append("\n".join(current_item).strip())
    return items, start_number, index


def _render_inline(value: str) -> str:
    placeholders: list[str] = []

    def _store(fragment: str) -> str:
        placeholders.append(fragment)
        return f"\x00{len(placeholders) - 1}\x00"

    text = value

    def _code_span(match: re.Match[str]) -> str:
        return _store(f"<code>{escape(match.group(1))}</code>")

    text = _CODE_SPAN_RE.sub(_code_span, text)
    text = escape(text)

    def _link(match: re.Match[str]) -> str:
        label = match.group(1)
        raw_url = unescape(match.group(2)).strip()
        safe_url = _sanitize_url(raw_url)
        if safe_url is None:
            return match.group(0)
        return _store(
            f'<a href="{safe_url}" target="_blank" rel="noreferrer">{label}</a>'
        )

    text = _LINK_RE.sub(_link, text)
    text = _BOLD_RE.sub(r"<strong>\2</strong>", text)
    text = _ITALIC_RE.sub(lambda match: f"<em>{match.group(1) or match.group(2) or ''}</em>", text)
    text = text.replace("\n", "<br>\n")

    for index, fragment in enumerate(placeholders):
        text = text.replace(f"\x00{index}\x00", fragment)
    return text


def _sanitize_url(value: str) -> str | None:
    if not value:
        return None
    if value.startswith(("/", "#")):
        return escape(value, quote=True)

    parsed = urlparse(value)
    if parsed.scheme.lower() not in {"http", "https", "mailto"}:
        return None
    return escape(value, quote=True)
