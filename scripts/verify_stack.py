"""Verify that core Docker services are reachable."""

from __future__ import annotations

import sys

import httpx
from elasticsearch import Elasticsearch
from kafka import KafkaAdminClient

from src.config import ELASTICSEARCH_URL, KAFKA_BOOTSTRAP_SERVERS, OLLAMA_URL


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
        admin.close()
        print("Kafka: OK")
        return True
    except Exception as exc:
        print(f"Kafka: FAIL ({exc})")
        return False


def check_ollama() -> bool:
    try:
        response = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=5.0)
        response.raise_for_status()
        print("Ollama: OK")
        return True
    except Exception as exc:
        print(f"Ollama: FAIL ({exc})")
        return False


def main() -> int:
    checks = [check_elasticsearch(), check_kafka(), check_ollama()]
    if all(checks):
        print("\nAll core services are reachable.")
        return 0
    print("\nOne or more services are unavailable.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
