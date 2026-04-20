from __future__ import annotations

import io
import keyword
import re
import token
import tokenize
from html import escape

from markupsafe import Markup


_PYTHON_HEREDOC_RE = re.compile(
    r"^(?P<invocation>[^\n]*\bpython(?:\d+(?:\.\d+)?)?\b[^\n]*?-\s*<<(?P<quote>['\"]?)(?P<tag>[A-Za-z_][A-Za-z0-9_]*)(?P=quote)[^\n]*)\n(?P<body>[\s\S]*?)\n(?P=tag)\s*$"
)
_PYTHON_BUILTINS = {
    "print",
    "len",
    "range",
    "dict",
    "list",
    "set",
    "tuple",
    "str",
    "int",
    "float",
    "bool",
    "open",
    "enumerate",
    "zip",
    "sum",
    "min",
    "max",
    "sorted",
    "any",
    "all",
}


def render_command_markup(value: str | None) -> Markup:
    text = str(value or "").strip()
    if not text:
        return Markup("")

    parsed = _parse_python_heredoc(text)
    if parsed is None:
        return Markup(
            '<pre class="command-rendered-shell">'
            f"{escape(text)}"
            "</pre>"
        )

    invocation, body, terminator = parsed
    highlighted_body = _highlight_python(body)
    return Markup(
        '<div class="command-rendered">'
        f'<pre class="command-rendered-shell">{escape(invocation)}</pre>'
        '<div class="command-rendered-python-block">'
        '<div class="command-rendered-label">Python stdin</div>'
        f'<pre class="command-rendered-python">{highlighted_body}</pre>'
        f'<div class="command-rendered-footer">{escape(terminator)}</div>'
        "</div>"
        "</div>"
    )


def _parse_python_heredoc(text: str) -> tuple[str, str, str] | None:
    match = _PYTHON_HEREDOC_RE.match(text)
    if not match:
        return None
    invocation = match.group("invocation").strip()
    body = match.group("body")
    terminator = match.group("tag")
    if not invocation or not terminator:
        return None
    return invocation, body, terminator


def _highlight_python(source: str) -> Markup:
    if not source:
        return Markup("")

    line_offsets: list[int] = []
    position = 0
    for line in source.splitlines(keepends=True):
        line_offsets.append(position)
        position += len(line)
    if not line_offsets:
        line_offsets.append(0)

    def absolute_index(location: tuple[int, int]) -> int:
        line_number, column = location
        base = line_offsets[line_number - 1] if 0 < line_number <= len(line_offsets) else 0
        return base + column

    fragments: list[str] = []
    cursor = 0
    try:
        for tok in tokenize.generate_tokens(io.StringIO(source).readline):
            if tok.type == tokenize.ENDMARKER:
                break
            start = absolute_index(tok.start)
            end = absolute_index(tok.end)
            if start > cursor:
                fragments.append(escape(source[cursor:start]))
            token_text = source[start:end]
            css_class = _token_class(tok)
            if css_class:
                fragments.append(f'<span class="{css_class}">{escape(token_text)}</span>')
            else:
                fragments.append(escape(token_text))
            cursor = end
    except (tokenize.TokenError, IndentationError):
        return Markup(escape(source))

    if cursor < len(source):
        fragments.append(escape(source[cursor:]))
    return Markup("".join(fragments))


def _token_class(tok: tokenize.TokenInfo) -> str | None:
    if tok.type == token.STRING:
        return "command-python-string"
    if tok.type == token.NUMBER:
        return "command-python-number"
    if tok.type == tokenize.COMMENT:
        return "command-python-comment"
    if tok.type == token.NAME:
        if keyword.iskeyword(tok.string):
            return "command-python-keyword"
        if tok.string in _PYTHON_BUILTINS:
            return "command-python-builtin"
    return None
