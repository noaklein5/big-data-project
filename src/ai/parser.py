"""Parse and validate LLM JSON output."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from src.elastic.schema import ALL_INDEX_NAMES, FORBIDDEN_FIELDS

_FENCE_PATTERN = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)


class ParseError(ValueError):
    """Raised when LLM output cannot be parsed into a query."""


@dataclass(frozen=True)
class ParsedQuery:
    index: str
    body: dict


def _strip_markdown_fences(text: str) -> str:
    match = _FENCE_PATTERN.search(text)
    if match:
        return match.group(1).strip()
    return text.strip()


def _find_forbidden_fields(value: object, path: str = "") -> list[str]:
    hits: list[str] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            current = f"{path}.{key}" if path else key
            if key in FORBIDDEN_FIELDS:
                hits.append(current)
            hits.extend(_find_forbidden_fields(nested, current))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            hits.extend(_find_forbidden_fields(item, f"{path}[{index}]"))
    return hits


def parse_llm_response(raw: str) -> ParsedQuery:
    """Parse LLM output into index + Elasticsearch request body."""
    cleaned = _strip_markdown_fences(raw)

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ParseError(f"Response is not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise ParseError("Response must be a JSON object")

    index = data.get("index")
    body = data.get("body")

    if not index or not isinstance(index, str):
        raise ParseError('Response must include a string "index" field')

    if index not in ALL_INDEX_NAMES:
        raise ParseError(
            f'Unknown index "{index}". Allowed: {", ".join(ALL_INDEX_NAMES)}'
        )

    if not isinstance(body, dict) or not body:
        raise ParseError('Response must include a non-empty object "body" field')

    forbidden = _find_forbidden_fields(body)
    if forbidden:
        raise ParseError(
            f"Forbidden field(s) in query body: {', '.join(sorted(forbidden))}. "
            "Use release_year or rating_year instead of year."
        )

    return ParsedQuery(index=index, body=body)
