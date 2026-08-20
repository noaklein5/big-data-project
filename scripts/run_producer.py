"""Run the Stage 5 Kafka ratings producer."""

from __future__ import annotations

import argparse
import sys

from src.producer.ratings_producer import stream_ratings_to_kafka


def main() -> int:
    parser = argparse.ArgumentParser(description="Stream MovieLens ratings into Kafka")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Number of rating rows to send (default: SAMPLE_RATINGS in sample mode, all in full mode)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Send all ratings regardless of DATA_MODE",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=10000,
        help="Print progress every N messages (0 to disable)",
    )
    args = parser.parse_args()

    try:
        stats = stream_ratings_to_kafka(
            sample_size=None if args.full else args.sample_size,
            full=args.full,
            progress_every=args.progress_every,
        )
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}")
        print("Ensure ratings.csv exists under data/raw/")
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1

    print(
        "\nProducer complete:\n"
        f"  topic:     {stats.topic}\n"
        f"  mode:      {stats.mode}\n"
        f"  rows read: {stats.rows_read:,}\n"
        f"  rows sent: {stats.rows_sent:,}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
