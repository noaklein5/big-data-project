"""Verify Stage 4 sample data loaded into Elasticsearch."""

from __future__ import annotations

import sys

from elasticsearch import Elasticsearch

from src.config import (
    ELASTICSEARCH_URL,
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
    SAMPLE_RATINGS,
)


def _count(client: Elasticsearch, index_name: str) -> int:
    return int(client.count(index=index_name)["count"])


def _sample_doc(client: Elasticsearch, index_name: str) -> dict | None:
    response = client.search(index=index_name, size=1)
    hits = response.get("hits", {}).get("hits", [])
    return hits[0]["_source"] if hits else None


def main() -> int:
    client = Elasticsearch(ELASTICSEARCH_URL)

    indexes = [
        INDEX_MOVIES,
        INDEX_MOVIES_BY_RELEASE_YEAR,
        INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    ]

    print("Verifying sample ETL indexes...\n")
    ok = True

    for index_name in indexes:
        try:
            doc_count = _count(client, index_name)
            sample = _sample_doc(client, index_name)
            print(f"{index_name}: {doc_count:,} documents")
            if sample:
                print(f"  sample: {sample}")
            if doc_count == 0:
                print("  WARNING: index is empty")
                ok = False
        except Exception as exc:
            print(f"{index_name}: FAIL ({exc})")
            ok = False
        print()

    movies_count = _count(client, INDEX_MOVIES)
    if movies_count > SAMPLE_RATINGS:
        print(
            f"WARNING: movies index has {movies_count:,} docs "
            f"(expected fewer than sample ratings {SAMPLE_RATINGS:,})"
        )

    if ok and movies_count > 0:
        print("Sample ETL verification passed.")
        return 0

    print("Sample ETL verification failed — run scripts/run_sample_etl.py first.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
