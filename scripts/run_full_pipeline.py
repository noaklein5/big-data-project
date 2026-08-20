"""Run the full MovieLens pipeline (Stage 13)."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run(command: list[str], *, label: str) -> int:
    print(f"\n=== {label} ===")
    print(" ".join(command))
    return subprocess.call(command, cwd=PROJECT_ROOT)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run producer → Spark ETL → integration verification"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Send all ratings (~20M). Slow — allow 30–90 minutes on a laptop.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Override sample size for the producer",
    )
    parser.add_argument(
        "--skip-producer",
        action="store_true",
        help="Skip Kafka producer (reuse existing topic data)",
    )
    parser.add_argument(
        "--skip-spark",
        action="store_true",
        help="Skip Spark ETL submit",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip integration verification at the end",
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip infrastructure/index setup (not recommended after down -v)",
    )
    parser.add_argument(
        "--include-ai",
        action="store_true",
        help="Include Ollama smoke test in final verification",
    )
    args = parser.parse_args()

    python = sys.executable
    mode = "full" if args.full else "auto"

    if args.full:
        print(
            "Full 20M mode selected.\n"
            "Expected runtime: producer ~15–30 min, Spark ETL ~20–60 min.\n"
            "Do not interrupt — partial Kafka topics require `docker compose down -v` to reset."
        )

    if not args.skip_setup:
        code = _run(
            ["docker", "exec", "movielens-app", "python", "scripts/setup_infrastructure.py"],
            label="Infrastructure setup (Kafka topic + ES indexes)",
        )
        if code != 0:
            return code

    if not args.skip_producer:
        producer_cmd = [
            "docker",
            "exec",
            "movielens-app",
            "python",
            "scripts/run_producer.py",
        ]
        if args.full:
            producer_cmd.append("--full")
        elif args.sample_size is not None:
            producer_cmd.extend(["--sample-size", str(args.sample_size)])

        code = _run(producer_cmd, label="Stage 5 — Kafka producer")
        if code != 0:
            return code

        code = _run(
            ["docker", "exec", "movielens-app", "python", "scripts/verify_producer.py"],
            label="Verify producer",
        )
        if code != 0:
            return code

    if not args.skip_spark:
        spark_cmd = [python, "scripts/run_spark_etl.py"]
        if args.full:
            spark_cmd.append("--full")
        code = _run(spark_cmd, label="Stage 6 — Spark ETL")
        if code != 0:
            return code

        code = _run(
            ["docker", "exec", "movielens-app", "python", "scripts/verify_spark_etl.py"],
            label="Verify Spark ETL",
        )
        if code != 0:
            return code

    if not args.skip_verify:
        verify_cmd = [
            "docker",
            "exec",
            "movielens-app",
            "python",
            "scripts/verify_integration.py",
            "--mode",
            mode,
        ]
        if args.include_ai:
            verify_cmd.append("--include-ai")
        code = _run(verify_cmd, label="Stage 13 — integration verification")
        if code != 0:
            return code

    print("\nFull pipeline run complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
