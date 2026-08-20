"""Submit the Stage 6 Spark ETL job (run from project root on the host)."""

from __future__ import annotations

import argparse
import subprocess
import sys

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,"
    "org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0"
)

SAMPLE_SPARK_RESOURCES = {
    "driver_memory": "512m",
    "executor_memory": "768m",
}

FULL_SPARK_RESOURCES = {
    "driver_memory": "512m",
    "executor_memory": "1280m",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit Spark ETL to movielens-spark-worker")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Use higher Spark memory settings for the full 20M dataset",
    )
    args = parser.parse_args()

    resources = FULL_SPARK_RESOURCES if args.full else SAMPLE_SPARK_RESOURCES
    if args.full:
        print("Full-mode Spark submit (driver=512m, executor=1280m)")

    cmd = [
        "docker",
        "exec",
        "movielens-spark-worker",
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark:7077",
        "--packages",
        SPARK_PACKAGES,
        "--driver-memory",
        resources["driver_memory"],
        "--executor-memory",
        resources["executor_memory"],
        "--conf",
        "spark.jars.ivy=/opt/spark-jobs/.ivy2",
        "--conf",
        "spark.driver.extraJavaOptions=-Divy.message.logger.level=4",
        "/opt/spark-jobs/etl_job.py",
    ]

    print("Submitting Spark ETL job (first run downloads JARs — may take a few minutes)...")
    print(" ".join(cmd))
    return subprocess.call(cmd)


if __name__ == "__main__":
    sys.exit(main())
