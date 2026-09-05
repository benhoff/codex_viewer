from __future__ import annotations

from dataclasses import dataclass
import re

from .projects import build_turn_search_match_expression


SEARCH_INTENT_KEYWORD = "keyword"
SEARCH_INTENT_LATEST_NEXT_STEP = "latest_next_step"
SEARCH_INTENT_REMAINING_ISSUES = "remaining_issues"
SEARCH_INTENT_RESOLVED_ISSUES = "resolved_issues"
SEARCH_MODES = frozenset({"all", "any", "phrase", "exact"})

_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+")
_PROJECT_REFERENCE_PATTERNS = (
    re.compile(
        r"\b([A-Za-z0-9][A-Za-z0-9_.:/-]{0,127})\s+(?:project|repo|repository)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:on|for|in|about)\s+(?:the\s+)?(?:project|repo|repository)\s+"
        r"([A-Za-z0-9][A-Za-z0-9_.:/-]{0,127})\b",
        re.IGNORECASE,
    ),
)
_REMAINING_PATTERN = re.compile(
    r"\b(?:remain|remains|remaining|unresolved|outstanding|pending|"
    r"open\s+issues?|left\s+to\s+do|blockers?)\b",
    re.IGNORECASE,
)
_RESOLVED_PATTERN = re.compile(
    r"\b(?:resolve|resolved|fixed|complete|completed|closed|done)\b",
    re.IGNORECASE,
)
_LATEST_PATTERN = re.compile(
    r"\b(?:last|latest|most\s+recent|next\s+step|going\s+to\s+do|"
    r"were\s+going\s+to|where\s+(?:did|do)\s+we\s+leave\s+off|resume)\b",
    re.IGNORECASE,
)
_NATURAL_LANGUAGE_PATTERN = re.compile(
    r"(?:^\s*(?:what|which|how|where|when|who|tell|show|list)\b|\?\s*$)",
    re.IGNORECASE,
)

_STOP_WORDS = {
    "a",
    "about",
    "all",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "been",
    "can",
    "could",
    "did",
    "do",
    "for",
    "from",
    "had",
    "has",
    "have",
    "how",
    "i",
    "in",
    "is",
    "it",
    "list",
    "me",
    "of",
    "on",
    "our",
    "project",
    "please",
    "repo",
    "repository",
    "show",
    "that",
    "the",
    "thing",
    "things",
    "tell",
    "this",
    "to",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "with",
    "would",
}

_INTENT_TERMS = {
    SEARCH_INTENT_LATEST_NEXT_STEP: (
        "next",
        "step",
        "plan",
        "planned",
        "todo",
        "going",
        "resume",
    ),
    SEARCH_INTENT_REMAINING_ISSUES: (
        "remaining",
        "remain",
        "unresolved",
        "outstanding",
        "open",
        "pending",
        "blocker",
        "issue",
        "todo",
    ),
    SEARCH_INTENT_RESOLVED_ISSUES: (
        "resolved",
        "resolve",
        "fixed",
        "complete",
        "completed",
        "closed",
        "done",
    ),
}
_ABSTRACT_VOCABULARY = {
    "blocker",
    "blockers",
    "closed",
    "complete",
    "completed",
    "done",
    "fixed",
    "going",
    "issue",
    "issues",
    "last",
    "latest",
    "next",
    "open",
    "outstanding",
    "pending",
    "plan",
    "planned",
    "remain",
    "remaining",
    "remains",
    "resolve",
    "resolved",
    "resume",
    "step",
    "todo",
    "unresolved",
}


def _unique(values: list[str], *, limit: int = 12) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
        if len(result) >= limit:
            break
    return tuple(result)


def _query_tokens(query: str | None) -> tuple[str, ...]:
    return _unique(
        [match.group(0).lower() for match in _TOKEN_PATTERN.finditer(str(query or ""))],
        limit=32,
    )


def _ordered_query_tokens(query: str | None) -> tuple[str, ...]:
    return tuple(
        match.group(0).lower()
        for match in list(_TOKEN_PATTERN.finditer(str(query or "")))[:32]
    )


def _project_hint(query: str) -> str | None:
    for pattern in _PROJECT_REFERENCE_PATTERNS:
        match = pattern.search(query)
        if match is not None:
            return match.group(1).strip().lower() or None
    return None


def _intent(query: str) -> str:
    if _REMAINING_PATTERN.search(query):
        return SEARCH_INTENT_REMAINING_ISSUES
    if _RESOLVED_PATTERN.search(query):
        return SEARCH_INTENT_RESOLVED_ISSUES
    if _LATEST_PATTERN.search(query):
        return SEARCH_INTENT_LATEST_NEXT_STEP
    return SEARCH_INTENT_KEYWORD


def _fts_or_expression(terms: tuple[str, ...]) -> str | None:
    if not terms:
        return None
    return " OR ".join(
        f'"{term}"*' if len(term) >= 3 else f'"{term}"'
        for term in terms
    )


def build_lexical_match_expression(query: str | None, *, mode: str = "all") -> str | None:
    normalized_mode = str(mode or "all").strip().lower()
    if normalized_mode not in SEARCH_MODES:
        raise ValueError(f"Unsupported search mode: {mode}")
    tokens = (
        _ordered_query_tokens(query)
        if normalized_mode in {"phrase", "exact"}
        else _query_tokens(query)
    )
    if not tokens:
        return None
    if normalized_mode == "all":
        return build_turn_search_match_expression(query)
    if normalized_mode == "any":
        return _fts_or_expression(tokens)
    phrase = " ".join(tokens)
    if normalized_mode == "phrase":
        return f'"{phrase}"*'
    return f'"{phrase}"'


@dataclass(frozen=True)
class SearchQueryPlan:
    query: str
    mode: str
    intent: str
    time_focus: str
    status_focus: str | None
    natural_language: bool
    project_hint: str | None
    content_terms: tuple[str, ...]
    relaxed_terms: tuple[str, ...]
    strict_expression: str | None
    relaxed_expression: str | None

    @property
    def is_abstract(self) -> bool:
        return self.intent != SEARCH_INTENT_KEYWORD

    @property
    def prefer_recent(self) -> bool:
        return self.is_abstract

    @property
    def allows_history_fallback(self) -> bool:
        return self.is_abstract or self.natural_language


def plan_search_query(query: str | None, *, mode: str = "all") -> SearchQueryPlan:
    normalized_query = str(query or "").strip()
    normalized_mode = str(mode or "all").strip().lower()
    if normalized_mode not in SEARCH_MODES:
        raise ValueError(f"Unsupported search mode: {mode}")
    deterministic_mode = normalized_mode != "all"
    intent = SEARCH_INTENT_KEYWORD if deterministic_mode else _intent(normalized_query)
    natural_language = (
        False
        if deterministic_mode
        else bool(_NATURAL_LANGUAGE_PATTERN.search(normalized_query))
    )
    project_hint = (
        _project_hint(normalized_query)
        if intent != SEARCH_INTENT_KEYWORD or natural_language
        else None
    )
    tokens = _query_tokens(normalized_query)
    project_tokens = set(_query_tokens(project_hint))

    meaningful_terms = [
        token
        for token in tokens
        if token not in _STOP_WORDS and token not in project_tokens
    ]
    if intent == SEARCH_INTENT_KEYWORD:
        content_terms = _unique(meaningful_terms)
        relaxed_terms = content_terms
        time_focus = "any"
        status_focus = None
    else:
        content_terms = _unique(
            [token for token in meaningful_terms if token not in _ABSTRACT_VOCABULARY]
        )
        relaxed_terms = _unique(
            [*content_terms, *_INTENT_TERMS[intent]]
        )
        time_focus = "latest" if intent == SEARCH_INTENT_LATEST_NEXT_STEP else "recent"
        status_focus = {
            SEARCH_INTENT_REMAINING_ISSUES: "open",
            SEARCH_INTENT_RESOLVED_ISSUES: "resolved",
        }.get(intent)

    return SearchQueryPlan(
        query=normalized_query,
        mode=normalized_mode,
        intent=intent,
        time_focus=time_focus,
        status_focus=status_focus,
        natural_language=natural_language,
        project_hint=project_hint,
        content_terms=content_terms,
        relaxed_terms=relaxed_terms,
        strict_expression=build_lexical_match_expression(
            normalized_query,
            mode=normalized_mode,
        ),
        relaxed_expression=(
            None if deterministic_mode else _fts_or_expression(relaxed_terms)
        ),
    )
