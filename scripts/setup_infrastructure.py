"""Verify and initialize Docker infrastructure (Stage 3)."""

from __future__ import annotations

import sys

import httpx
from elasticsearch import Elasticsearch
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from src.config import (
    ELASTICSEARCH_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_RATINGS,
    KIBANA_URL,
    OLLAMA_MODEL,
    OLLAMA_URL,
    SPARK_UI_URL,
)
from src.elastic.setup_indexes import create_indexes


def ensure_kafka_topic() -> bool:
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        request_timeout_ms=10000,
    )
    try:
        existing = set(admin.list_topics())
        if KAFKA_TOPIC_RAW_RATINGS in existing:
            print(f"Kafka topic: OK ({KAFKA_TOPIC_RAW_RATINGS} exists)")
            return True

        admin.create_topics(
            [
                NewTopic(
                    name=KAFKA_TOPIC_RAW_RATINGS,
                    num_partitions=1,
                    replication_factor=1,
                )
            ]
        )
        print(f"Kafka topic: created ({KAFKA_TOPIC_RAW_RATINGS})")
        return True
    except TopicAlreadyExistsError:
        print(f"Kafka topic: OK ({KAFKA_TOPIC_RAW_RATINGS} exists)")
        return True
    except Exception as exc:
        print(f"Kafka topic: FAIL ({exc})")
        return False
    finally:
        admin.close()


def check_elasticsearch() -> bool:
    try:
        client = Elasticsearch(ELASTICSEARCH_URL)
        health = client.cluster.health()
        print(f"Elasticsearch: OK ({health.get('status')})")
        return True
    except Exception as exc:
        print(f"Elasticsearch: FAIL ({exc})")
        return False


def check_kafka_broker() -> bool:
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000,
        )
        admin.close()
        print("Kafka broker: OK")
        return True
    except Exception as exc:
        print(f"Kafka broker: FAIL ({exc})")
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
        print("Spark master UI: OK")
        return True
    except Exception as exc:
        print(f"Spark master UI: FAIL ({exc})")
        return False


def check_ollama() -> bool:
    try:
        response = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=10.0)
        response.raise_for_status()
        print("Ollama: OK")
        return True
    except Exception as exc:
        print(f"Ollama: FAIL ({exc})")
        return False


def check_ollama_model() -> bool:
    try:
        response = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=10.0)
        response.raise_for_status()
        models = {item.get("name") for item in response.json().get("models", [])}
        if OLLAMA_MODEL in models or f"{OLLAMA_MODEL}:latest" in models:
            print(f"Ollama model: OK ({OLLAMA_MODEL})")
            return True
        print(f"Ollama model: MISSING ({OLLAMA_MODEL})")
        print("  Run: docker exec movielens-ollama ollama pull llama3.2:3b")
        return False
    except Exception as exc:
        print(f"Ollama model: FAIL ({exc})")
        return False


def main() -> int:
    print("Setting up infrastructure...\n")

    checks = [
        check_elasticsearch(),
        check_kafka_broker(),
        ensure_kafka_topic(),
        check_kibana(),
        check_spark(),
        check_ollama(),
        check_ollama_model(),
    ]

    try:
        create_indexes()
        print("Elasticsearch indexes: OK")
    except Exception as exc:
        print(f"Elasticsearch indexes: FAIL ({exc})")
        checks.append(False)

    if all(checks):
        print("\nInfrastructure setup complete.")
        return 0

    print("\nInfrastructure setup incomplete — see failures above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
