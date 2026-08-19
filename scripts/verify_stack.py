"""Verify that all Docker services are reachable."""

from __future__ import annotations

import sys

import httpx
from elasticsearch import Elasticsearch
from kafka import KafkaAdminClient

from src.config import (
    ELASTICSEARCH_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_RATINGS,
    KIBANA_URL,
    OLLAMA_MODEL,
    OLLAMA_URL,
    SPARK_UI_URL,
)


def check_elasticsearch() -> bool:
    try:
        client = Elasticsearch(ELASTICSEARCH_URL)
        health = client.cluster.health()
        print(f"Elasticsearch: OK ({health.get('status')})")
        return True
    except Exception as exc:
        print(f"Elasticsearch: FAIL ({exc})")
        return False


def check_kafka() -> bool:
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000,
        )
        topics = set(admin.list_topics())
        admin.close()
        if KAFKA_TOPIC_RAW_RATINGS in topics:
            print(f"Kafka: OK (topic `{KAFKA_TOPIC_RAW_RATINGS}` exists)")
        else:
            print(f"Kafka: WARN (broker up, topic `{KAFKA_TOPIC_RAW_RATINGS}` missing)")
            print("  Run: docker exec movielens-app python scripts/setup_infrastructure.py")
        return True
    except Exception as exc:
        print(f"Kafka: FAIL ({exc})")
        return False


def check_kibana() -> bool:
    try:
        response = httpx.get(f"{KIBANA_URL}/api/status", timeout=10.0)
        response.raise_for_status()
        print("Kibana: OK")
        return True
    except Exception as exc:
        print(f"Kibana: FAIL ({exc})")
        return False


def check_spark() -> bool:
    try:
        response = httpx.get(SPARK_UI_URL, timeout=10.0)
        response.raise_for_status()
        print("Spark: OK")
        return True
    except Exception as exc:
        print(f"Spark: FAIL ({exc})")
        return False


def check_ollama() -> bool:
    try:
        response = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=5.0)
        response.raise_for_status()
        models = {item.get("name") for item in response.json().get("models", [])}
        if OLLAMA_MODEL in models or f"{OLLAMA_MODEL}:latest" in models:
            print(f"Ollama: OK (model `{OLLAMA_MODEL}` available)")
        else:
            print(f"Ollama: WARN (service up, model `{OLLAMA_MODEL}` not pulled)")
            print("  Run: docker exec movielens-ollama ollama pull llama3.2:3b")
        return True
    except Exception as exc:
        print(f"Ollama: FAIL ({exc})")
        return False


def main() -> int:
    checks = [
        check_elasticsearch(),
        check_kafka(),
        check_kibana(),
        check_spark(),
        check_ollama(),
    ]
    if all(checks):
        print("\nAll infrastructure services are reachable.")
        return 0
    print("\nOne or more services are unavailable.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
