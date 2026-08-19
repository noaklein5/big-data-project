"""Verify Elasticsearch indexes, mappings, and query behavior (Stage 7)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from elasticsearch import Elasticsearch

from src.config import (
    ELASTICSEARCH_URL,
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
)
from src.elastic.schema import ALL_INDEX_NAMES, INDEX_FIELDS, INDEX_MAPPINGS


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def get_client() -> Elasticsearch:
    return Elasticsearch(ELASTICSEARCH_URL)


def _expected_property_type(field_spec_type: str) -> str:
    if field_spec_type == "text+keyword":
        return "text"
    if field_spec_type.endswith("[]"):
        return "keyword"
    mapping = {
        "integer": "integer",
        "float": "float",
        "text+keyword": "text",
        "keyword[]": "keyword",
    }
    return mapping.get(field_spec_type, field_spec_type)


def _live_property_type(properties: dict[str, Any], field_name: str) -> str | None:
    field = properties.get(field_name)
    if not field:
        return None
    return field.get("type")


def check_indexes_exist(client: Elasticsearch) -> list[CheckResult]:
    results: list[CheckResult] = []
    for index_name in ALL_INDEX_NAMES:
        exists = client.indices.exists(index=index_name)
        results.append(
            CheckResult(
                name=f"index exists: {index_name}",
                passed=bool(exists),
                detail="ok" if exists else "missing — run setup_infrastructure.py",
            )
        )
    return results


def check_mappings_match_schema(client: Elasticsearch) -> list[CheckResult]:
    results: list[CheckResult] = []
    for index_name in ALL_INDEX_NAMES:
        live = client.indices.get_mapping(index=index_name)
        properties = live[index_name]["mappings"].get("properties", {})
        expected_props = INDEX_MAPPINGS[index_name]["mappings"]["properties"]

        expected_fields = set(expected_props.keys())
        live_fields = set(properties.keys())
        missing = expected_fields - live_fields
        extra = live_fields - expected_fields

        if missing:
            results.append(
                CheckResult(
                    name=f"mapping fields: {index_name}",
                    passed=False,
                    detail=f"missing fields: {sorted(missing)}",
                )
            )
            continue

        type_mismatches: list[str] = []
        for field_name, spec in INDEX_FIELDS[index_name].items():
            expected_type = _expected_property_type(spec["type"])
            live_type = _live_property_type(properties, field_name)
            if live_type != expected_type:
                type_mismatches.append(f"{field_name}: expected {expected_type}, got {live_type}")

            if spec["type"] == "text+keyword":
                keyword_field = properties[field_name].get("fields", {}).get("keyword")
                if not keyword_field or keyword_field.get("type") != "keyword":
                    type_mismatches.append(f"{field_name}.keyword subfield missing")

        passed = not type_mismatches
        detail = "ok"
        if extra:
            detail = f"ok (extra fields ignored: {sorted(extra)})"
        if type_mismatches:
            detail = "; ".join(type_mismatches)

        results.append(
            CheckResult(
                name=f"mapping types: {index_name}",
                passed=passed,
                detail=detail,
            )
        )
    return results


def check_document_counts(client: Elasticsearch) -> list[CheckResult]:
    results: list[CheckResult] = []
    for index_name in ALL_INDEX_NAMES:
        count = int(client.count(index=index_name)["count"])
        results.append(
            CheckResult(
                name=f"documents: {index_name}",
                passed=count > 0,
                detail=f"{count:,} docs" if count else "empty — run ETL pipeline first",
            )
        )
    return results


def _search_hits(client: Elasticsearch, index: str, body: dict[str, Any]) -> int:
    response = client.search(index=index, **body)
    return int(response["hits"]["total"]["value"])


def check_query_behavior(client: Elasticsearch) -> list[CheckResult]:
    results: list[CheckResult] = []

    comedy_hits = _search_hits(
        client,
        INDEX_MOVIES,
        {
            "query": {"term": {"genres": "Comedy"}},
            "size": 0,
        },
    )
    results.append(
        CheckResult(
            name="filter: genres exact match (Comedy)",
            passed=comedy_hits > 0,
            detail=f"{comedy_hits:,} hits",
        )
    )

    after_2000_hits = _search_hits(
        client,
        INDEX_MOVIES,
        {
            "query": {"range": {"release_year": {"gt": 2000}}},
            "size": 0,
        },
    )
    results.append(
        CheckResult(
            name="filter: release_year range (> 2000)",
            passed=after_2000_hits > 0,
            detail=f"{after_2000_hits:,} hits",
        )
    )

    high_rated = client.search(
        index=INDEX_MOVIES,
        query={
            "bool": {
                "filter": [
                    {"term": {"genres": "Comedy"}},
                    {"range": {"rating_count": {"gte": 10}}},
                    {"range": {"average_rating": {"gte": 3.5}}},
                ]
            }
        },
        sort=[{"average_rating": "desc"}],
        size=5,
    )
    sorted_hits = high_rated["hits"]["hits"]
    ratings = [hit["_source"]["average_rating"] for hit in sorted_hits]
    sort_ok = len(sorted_hits) > 0 and ratings == sorted(ratings, reverse=True)
    results.append(
        CheckResult(
            name="sort: average_rating descending",
            passed=sort_ok,
            detail=f"top ratings {ratings}" if ratings else "no hits",
        )
    )

    tag_hits = _search_hits(
        client,
        INDEX_MOVIES,
        {
            "query": {"term": {"tags": "pixar"}},
            "size": 0,
        },
    )
    results.append(
        CheckResult(
            name="filter/search: tags exact match (pixar)",
            passed=tag_hits > 0,
            detail=f"{tag_hits:,} hits",
        )
    )

    genre_agg = client.search(
        index=INDEX_MOVIES,
        size=0,
        aggs={
            "top_genres": {
                "terms": {"field": "genres", "size": 5},
                "aggs": {"avg_rating": {"avg": {"field": "average_rating"}}},
            }
        },
    )
    buckets = genre_agg["aggregations"]["top_genres"]["buckets"]
    results.append(
        CheckResult(
            name="aggregation: genres + average_rating",
            passed=len(buckets) > 0,
            detail=f"{len(buckets)} buckets; top={buckets[0]['key'] if buckets else 'none'}",
        )
    )

    cohort_hits = _search_hits(
        client,
        INDEX_MOVIES_BY_RELEASE_YEAR,
        {
            "query": {"range": {"release_year": {"gte": 1990, "lte": 1999}}},
            "size": 0,
        },
    )
    results.append(
        CheckResult(
            name="filter: release_year cohort (1990s)",
            passed=cohort_hits > 0,
            detail=f"{cohort_hits:,} hits",
        )
    )

    cohort_sort = client.search(
        index=INDEX_MOVIES_BY_RELEASE_YEAR,
        query={"match_all": {}},
        sort=[{"movie_count": "desc"}],
        size=3,
    )
    cohort_docs = [hit["_source"] for hit in cohort_sort["hits"]["hits"]]
    results.append(
        CheckResult(
            name="sort: movie_count on release_year index",
            passed=len(cohort_docs) > 0,
            detail=f"top years {[doc['release_year'] for doc in cohort_docs]}",
        )
    )

    rating_year_hits = _search_hits(
        client,
        INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
        {
            "query": {"term": {"rating_year": 2010}},
            "size": 0,
        },
    )
    results.append(
        CheckResult(
            name="filter: rating_year exact match (2010)",
            passed=rating_year_hits > 0,
            detail=f"{rating_year_hits:,} hits",
        )
    )

    rating_year_agg = client.search(
        index=INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
        size=0,
        aggs={
            "by_year": {
                "terms": {"field": "rating_year", "size": 5, "order": {"_key": "desc"}},
                "aggs": {"total_ratings": {"sum": {"field": "rating_count"}}},
            }
        },
    )
    year_buckets = rating_year_agg["aggregations"]["by_year"]["buckets"]
    results.append(
        CheckResult(
            name="aggregation: rating_year trends",
            passed=len(year_buckets) > 0,
            detail=f"{len(year_buckets)} buckets; latest={year_buckets[0]['key'] if year_buckets else 'none'}",
        )
    )

    return results


def run_all_checks(client: Elasticsearch | None = None) -> list[CheckResult]:
    es = client or get_client()
    results: list[CheckResult] = []
    results.extend(check_indexes_exist(es))
    if not all(r.passed for r in results):
        return results
    results.extend(check_mappings_match_schema(es))
    results.extend(check_document_counts(es))
    if not all(r.passed for r in results[-len(ALL_INDEX_NAMES) :]):
        return results
    results.extend(check_query_behavior(es))
    return results
