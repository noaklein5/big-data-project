"""Submit the Stage 6 Spark ETL job (run from project root on the host)."""

from __future__ import annotations

import subprocess
import sys

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,"
    "org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0"
)


def main() -> int:
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
        "512m",
        "--executor-memory",
        "768m",
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
