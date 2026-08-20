"""Stream MovieLens ratings from CSV into Kafka."""

from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path

from kafka import KafkaProducer

from src.config import (
    DATA_MODE,
    DATA_RAW_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_RATINGS,
    SAMPLE_RATINGS,
)
from src.producer.message import row_to_message


@dataclass(frozen=True)
class ProducerStats:
    rows_read: int
    rows_sent: int
    topic: str
    mode: str


def _ratings_path() -> Path:
    path = DATA_RAW_PATH / "ratings.csv"
    if not path.exists():
        raise FileNotFoundError(f"Ratings file not found: {path}")
    return path


def _resolve_limit(sample_size: int | None, *, full: bool = False) -> int | None:
    if full:
        return None
    if sample_size is not None:
        return sample_size
    if DATA_MODE == "sample":
        return SAMPLE_RATINGS
    return None


def stream_ratings_to_kafka(
    *,
    sample_size: int | None = None,
    full: bool = False,
    progress_every: int = 10000,
) -> ProducerStats:
    limit = _resolve_limit(sample_size, full=full)
    path = _ratings_path()
    mode = "sample" if limit is not None else "full"

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
    )

    rows_read = 0
    rows_sent = 0
    pending: list = []
    started = time.monotonic()

    print(
        f"Streaming ratings from {path} to topic `{KAFKA_TOPIC_RAW_RATINGS}` "
        f"(mode={mode}, limit={limit or 'all'})"
    )
    if mode == "full":
        print("Do not press Ctrl+C — full mode may take 15–30 minutes for ~20M ratings.")
    else:
        print("Do not press Ctrl+C — interrupting leaves a partial topic. Estimated ~2–3 min for 100k.")

    def flush_pending() -> None:
        nonlocal pending
        for future in pending:
            future.get(timeout=60)
        pending = []

    try:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if limit is not None and rows_read >= limit:
                    break

                message = row_to_message(row)
                pending.append(
                    producer.send(
                        KAFKA_TOPIC_RAW_RATINGS,
                        key=message["movieId"],
                        value=message,
                    )
                )
                rows_read += 1
                rows_sent += 1

                if len(pending) >= 5000:
                    flush_pending()

                if progress_every and rows_sent % progress_every == 0:
                    flush_pending()
                    elapsed = time.monotonic() - started
                    rate = rows_sent / elapsed if elapsed else 0
                    print(f"  sent {rows_sent:,} messages ({rate:,.0f}/s)")

        flush_pending()
        producer.flush()
    finally:
        producer.close()

    elapsed = time.monotonic() - started
    print(f"Finished: {rows_sent:,} messages in {elapsed:.1f}s")

    return ProducerStats(
        rows_read=rows_read,
        rows_sent=rows_sent,
        topic=KAFKA_TOPIC_RAW_RATINGS,
        mode=mode,
    )
