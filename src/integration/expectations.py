"""Sample vs full pipeline expectations for integration verification."""

from __future__ import annotations

from dataclasses import dataclass

from src.config import (
    DATA_MODE,
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
    SAMPLE_RATINGS,
)


@dataclass(frozen=True)
class PipelineExpectations:
    mode: str
    min_kafka_messages: int
    min_index_counts: dict[str, int]


SAMPLE_EXPECTATIONS = PipelineExpectations(
    mode="sample",
    min_kafka_messages=max(1_000, int(SAMPLE_RATINGS * 0.9)),
    min_index_counts={
        INDEX_MOVIES: 5_000,
        INDEX_MOVIES_BY_RELEASE_YEAR: 50,
        INDEX_MOVIE_RATINGS_BY_RATING_YEAR: 10_000,
    },
)

FULL_EXPECTATIONS = PipelineExpectations(
    mode="full",
    min_kafka_messages=15_000_000,
    min_index_counts={
        INDEX_MOVIES: 25_000,
        INDEX_MOVIES_BY_RELEASE_YEAR: 80,
        # Aggregated (movie_id, rating_year) pairs — not one doc per rating
        INDEX_MOVIE_RATINGS_BY_RATING_YEAR: 150_000,
    },
)


def resolve_expectations(
    mode: str = "auto",
    *,
    kafka_count: int | None = None,
    movies_count: int | None = None,
) -> PipelineExpectations:
    """Return thresholds for sample or full pipeline verification."""
    normalized = mode.lower().strip()
    if normalized == "full":
        return FULL_EXPECTATIONS
    if normalized == "sample":
        return SAMPLE_EXPECTATIONS

    if normalized not in {"auto", ""}:
        raise ValueError(f"Unknown mode: {mode!r} (use sample, full, or auto)")

    if DATA_MODE == "full":
        return FULL_EXPECTATIONS
    if movies_count is not None and movies_count >= 20_000:
        return FULL_EXPECTATIONS
    if kafka_count is not None and kafka_count >= 1_000_000:
        return FULL_EXPECTATIONS
    return SAMPLE_EXPECTATIONS
