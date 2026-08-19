"""Shared MovieLens transform helpers — same logic as Stage 1 exploration."""

from __future__ import annotations

import re
from datetime import datetime, timezone

TITLE_YEAR_PATTERN = re.compile(r"\((\d{4})\)\s*$")
VALID_RATINGS = frozenset({i / 2 for i in range(1, 11)})  # 0.5 … 5.0


def extract_release_year(title: str) -> int | None:
    match = TITLE_YEAR_PATTERN.search(str(title))
    return int(match.group(1)) if match else None


def parse_genres(genres: str) -> list[str]:
    if genres is None or str(genres).strip() in {"", "(no genres listed)"}:
        return []
    return [part.strip() for part in str(genres).split("|") if part.strip()]


def normalize_tag(tag: str) -> str:
    return str(tag).strip().lower()


def is_valid_rating(value: float) -> bool:
    return float(value) in VALID_RATINGS


def timestamp_to_rating_year(timestamp: int | str) -> int:
    if isinstance(timestamp, str):
        return datetime.fromisoformat(timestamp.strip()).year
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).year
