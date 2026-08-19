"""Kafka message contract for the raw ratings stream."""

from __future__ import annotations

from typing import Any

REQUIRED_FIELDS = frozenset({"userId", "movieId", "rating", "timestamp"})


def normalize_timestamp(value: Any) -> int | str:
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    return text


def row_to_message(row: dict[str, Any]) -> dict[str, Any]:
    """Convert a ratings.csv row to a Kafka JSON payload."""
    return {
        "userId": int(row["userId"]),
        "movieId": int(row["movieId"]),
        "rating": float(row["rating"]),
        "timestamp": normalize_timestamp(row["timestamp"]),
    }


def validate_message(message: dict[str, Any]) -> bool:
    return REQUIRED_FIELDS.issubset(message.keys())
