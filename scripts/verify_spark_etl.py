"""Verify Stage 6 Spark ETL output in Elasticsearch."""

from __future__ import annotations

import subprocess
import sys


def main() -> int:
    print("Verifying Spark ETL Elasticsearch indexes...\n")
    return subprocess.call([sys.executable, "scripts/verify_sample_etl.py"])


if __name__ == "__main__":
    sys.exit(main())
