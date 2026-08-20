"""Stage 13 end-to-end integration verification."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from elasticsearch import Elasticsearch
from kafka import KafkaConsumer, TopicPartition

from src.ai.generator import execute_query, generate_query
from src.config import (
    ELASTICSEARCH_URL,
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_RATINGS,
    OLLAMA_MODEL,
)
from src.elastic.verify_indexes import run_all_checks as run_es_checks
from src.integration.expectations import PipelineExpectations, resolve_expectations


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def _kafka_message_count() -> int:
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        group_id=f"verify-integration-{uuid.uuid4()}",
    )
    try:
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC_RAW_RATINGS)
        if not partitions:
            return 0
        topic_partitions = [TopicPartition(KAFKA_TOPIC_RAW_RATINGS, p) for p in partitions]
        end_offsets = consumer.end_offsets(topic_partitions)
        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        return sum(end_offsets[tp] - beginning_offsets[tp] for tp in topic_partitions)
    finally:
        consumer.close()


def _index_counts(client: Elasticsearch) -> dict[str, int]:
    return {
        INDEX_MOVIES: int(client.count(index=INDEX_MOVIES)["count"]),
        INDEX_MOVIES_BY_RELEASE_YEAR: int(
            client.count(index=INDEX_MOVIES_BY_RELEASE_YEAR)["count"]
        ),
        INDEX_MOVIE_RATINGS_BY_RATING_YEAR: int(
            client.count(index=INDEX_MOVIE_RATINGS_BY_RATING_YEAR)["count"]
        ),
    }


def check_kafka_messages(expectations: PipelineExpectations) -> CheckResult:
    try:
        total = _kafka_message_count()
    except Exception as exc:
        return CheckResult(
            name="Kafka topic message count",
            passed=False,
            detail=f"could not inspect topic: {exc}",
        )

    passed = total >= expectations.min_kafka_messages
    detail = (
        f"{total:,} messages (min {expectations.min_kafka_messages:,} for {expectations.mode} mode)"
    )
    if total == 0:
        detail = "topic empty — run scripts/run_producer.py first"
    return CheckResult(name="Kafka topic message count", passed=passed, detail=detail)


def check_index_counts(
    counts: dict[str, int],
    expectations: PipelineExpectations,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    for index_name, minimum in expectations.min_index_counts.items():
        actual = counts.get(index_name, 0)
        passed = actual >= minimum
        results.append(
            CheckResult(
                name=f"documents: {index_name}",
                passed=passed,
                detail=f"{actual:,} docs (min {minimum:,} for {expectations.mode} mode)",
            )
        )
    return results


def check_gold_queries() -> CheckResult:
    from src.elastic.gold_queries import run_all_queries

    results = run_all_queries()
    passed_count = sum(1 for result in results if result["passed"])
    failures = [f"{result['id']}: hits={result['hits']}" for result in results if not result["passed"]]

    passed = passed_count == len(results)
    detail = f"{passed_count}/{len(results)} gold queries passed"
    if failures:
        detail = f"{detail}; first failure: {failures[0]}"
    return CheckResult(name="gold queries", passed=passed, detail=detail)


def check_ai_smoke() -> CheckResult:
    question = "Show Comedy movies with at least 100 ratings sorted by average rating."
    try:
        generated = generate_query(question)
        result = execute_query(generated)
    except Exception as exc:
        return CheckResult(
            name="AI smoke test",
            passed=False,
            detail=f"failed on `{question}`: {exc}",
        )

    passed = result.hits > 0 or result.agg_buckets > 0
    detail = (
        f"index={generated.index}, hits={result.hits:,}, "
        f"agg_buckets={result.agg_buckets} (model={OLLAMA_MODEL})"
    )
    return CheckResult(name="AI smoke test", passed=passed, detail=detail)


def verify_integration(
    *,
    mode: str = "auto",
    include_ai: bool = False,
    client: Elasticsearch | None = None,
) -> tuple[PipelineExpectations, list[CheckResult]]:
    """Run Stage 13 integration checks across Kafka, ES, gold queries, and AI."""
    es = client or Elasticsearch(ELASTICSEARCH_URL)

    kafka_count = _kafka_message_count()
    counts = _index_counts(es)
    expectations = resolve_expectations(
        mode,
        kafka_count=kafka_count,
        movies_count=counts[INDEX_MOVIES],
    )

    results: list[CheckResult] = [
        CheckResult(
            name="pipeline mode",
            passed=True,
            detail=f"{expectations.mode} thresholds applied",
        ),
        CheckResult(
            name="Kafka topic message count",
            passed=kafka_count >= expectations.min_kafka_messages,
            detail=(
                f"{kafka_count:,} messages "
                f"(min {expectations.min_kafka_messages:,} for {expectations.mode} mode)"
                if kafka_count
                else "topic empty — run scripts/run_producer.py first"
            ),
        ),
    ]
    results.extend(check_index_counts(counts, expectations))

    es_results = run_es_checks(es)
    es_passed = sum(1 for item in es_results if item.passed)
    results.append(
        CheckResult(
            name="Elasticsearch indexes and queries",
            passed=all(item.passed for item in es_results),
            detail=f"{es_passed}/{len(es_results)} Stage 7 checks passed",
        )
    )

    results.append(check_gold_queries())

    if include_ai:
        results.append(check_ai_smoke())

    return expectations, results
