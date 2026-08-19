"""Verify Kafka ratings messages from the Stage 5 producer."""

from __future__ import annotations

import argparse
import json
import sys
import uuid

from kafka import KafkaConsumer, TopicPartition

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW_RATINGS, SAMPLE_RATINGS
from src.producer.message import REQUIRED_FIELDS, validate_message


def _topic_message_count() -> int:
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        group_id=f"verify-producer-{uuid.uuid4()}",
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


def consume_sample_messages(max_messages: int, timeout_ms: int) -> list[dict]:
    consumer = KafkaConsumer(
        KAFKA_TOPIC_RAW_RATINGS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout_ms,
        group_id=f"verify-producer-{uuid.uuid4()}",
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    messages: list[dict] = []
    try:
        for record in consumer:
            messages.append(record.value)
            if len(messages) >= max_messages:
                break
    finally:
        consumer.close()

    return messages


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Kafka ratings producer output")
    parser.add_argument(
        "--sample-messages",
        type=int,
        default=5,
        help="Number of messages to read and print",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=10000,
        help="Consumer timeout in milliseconds",
    )
    args = parser.parse_args()

    print(f"Verifying topic `{KAFKA_TOPIC_RAW_RATINGS}`...\n")

    try:
        total = _topic_message_count()
    except Exception as exc:
        print(f"FAIL: could not inspect topic ({exc})")
        return 1

    print(f"Topic message count: {total:,}")
    if total == 0:
        print("FAIL: topic is empty — run scripts/run_producer.py first")
        return 1

    messages = consume_sample_messages(args.sample_messages, args.timeout_ms)
    if not messages:
        print("FAIL: could not consume any messages")
        return 1

    invalid = [msg for msg in messages if not validate_message(msg)]
    if invalid:
        print(f"FAIL: {len(invalid)} message(s) missing required fields {sorted(REQUIRED_FIELDS)}")
        return 1

    print(f"Consumed {len(messages)} sample message(s):\n")
    for index, message in enumerate(messages, start=1):
        print(f"  [{index}] {json.dumps(message, sort_keys=True)}")

    if total < SAMPLE_RATINGS and total > 0:
        print(
            f"\nNote: {total:,} messages in topic (sample mode default is {SAMPLE_RATINGS:,}). "
            "Re-run producer or check DATA_MODE."
        )

    print("\nProducer verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
