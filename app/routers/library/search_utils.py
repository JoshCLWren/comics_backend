"""Utilities shared by the library routers for fuzzy title matching."""

from __future__ import annotations

import re
from difflib import SequenceMatcher

_TOKEN_RE = re.compile(r"[0-9a-zA-Z]+")


def tokenize(text: str) -> list[str]:
    """Break a string into lowercase alphanumeric tokens."""
    return [token for token in _TOKEN_RE.findall(text.lower()) if token]


def _normalized_text(tokens: list[str]) -> str:
    """Join normalized tokens with single spaces."""
    return " ".join(tokens)


def _collapsed_text(tokens: list[str]) -> str:
    """Return all tokens concatenated together."""
    return "".join(tokens)


def fuzzy_score(title: str, query: str) -> float:
    """Return a fuzzy matching score between 0 and 1."""
    title_tokens = tokenize(title)
    query_tokens = tokenize(query)
    if not title_tokens or not query_tokens:
        return 0.0

    normalized_title = _normalized_text(title_tokens)
    normalized_query = _normalized_text(query_tokens)
    collapsed_title = _collapsed_text(title_tokens)
    collapsed_query = _collapsed_text(query_tokens)

    ratio = SequenceMatcher(None, normalized_query, normalized_title).ratio()

    if normalized_title == normalized_query or collapsed_title == collapsed_query:
        ratio += 0.3
    if normalized_title.startswith(normalized_query):
        ratio += 0.1
    if normalized_title.endswith(normalized_query):
        ratio += 0.08
    if normalized_query in normalized_title:
        ratio += 0.05
    if collapsed_query in collapsed_title:
        ratio += 0.07
    if set(query_tokens).issubset(title_tokens):
        ratio += 0.05
    return min(ratio, 1.0)


def matches_search(title: str, query: str) -> bool:
    """Return True when the title should be considered a match for the query."""
    query_tokens = tokenize(query)
    if not query_tokens:
        return True

    title_tokens = tokenize(title)
    if not title_tokens:
        return False

    normalized_title = _normalized_text(title_tokens)
    normalized_query = _normalized_text(query_tokens)
    collapsed_title = _collapsed_text(title_tokens)
    collapsed_query = _collapsed_text(query_tokens)

    if normalized_query in normalized_title:
        return True
    if collapsed_query and collapsed_query in collapsed_title:
        return True

    title_token_set = set(title_tokens)
    query_token_set = set(query_tokens)
    if query_token_set <= title_token_set:
        return True
    if title_token_set & query_token_set:
        return True

    for q in query_token_set:
        for token in title_tokens:
            if len(q) >= 3 and q in token:
                return True
            if len(token) >= 3 and token in q:
                return True
    return False


__all__ = ["fuzzy_score", "matches_search", "tokenize"]
