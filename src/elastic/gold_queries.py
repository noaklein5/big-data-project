"""Load and run Stage 8 gold Elasticsearch queries."""

from __future__ import annotations

import json
from pathlib import Path

from elasticsearch import Elasticsearch

from src.config import ELASTICSEARCH_URL

QUERIES_PATH = Path(__file__).resolve().parent.parent.parent / "tests" / "gold_queries" / "queries.json"


def load_gold_queries(path: Path | None = None) -> list[dict]:
    queries_file = path or QUERIES_PATH
    return json.loads(queries_file.read_text(encoding="utf-8"))


def _count_agg_buckets(aggregations: dict | None) -> int:
    if not aggregations:
        return 0
    for value in aggregations.values():
        if isinstance(value, dict) and "buckets" in value:
            return len(value["buckets"])
    return 0


def run_query(client: Elasticsearch, case: dict) -> dict:
    response = client.search(index=case["index"], **case["body"])
    hits = int(response["hits"]["total"]["value"])
    aggregations = response.get("aggregations")
    agg_buckets = _count_agg_buckets(aggregations)

    min_hits = case.get("min_hits", 0)
    min_agg = case.get("min_agg_buckets", 0)
    passed = hits >= min_hits and agg_buckets >= min_agg

    return {
        "id": case["id"],
        "index": case["index"],
        "question": case["question"],
        "passed": passed,
        "hits": hits,
        "agg_buckets": agg_buckets,
        "response": response,
    }


def run_all_queries(
    client: Elasticsearch | None = None,
    *,
    query_id: str | None = None,
) -> list[dict]:
    es = client or Elasticsearch(ELASTICSEARCH_URL)
    cases = load_gold_queries()
    if query_id:
        cases = [case for case in cases if case["id"] == query_id]
        if not cases:
            raise ValueError(f"Unknown query id: {query_id}")
    return [run_query(es, case) for case in cases]
