"""Run the Stage 4 sample ETL pipeline."""

from __future__ import annotations

import argparse
import sys

from src.etl.sample_pipeline import run_sample_etl


def main() -> int:
    parser = argparse.ArgumentParser(description="Load sample MovieLens data into Elasticsearch")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Number of rating rows to read (default: SAMPLE_RATINGS from env)",
    )
    parser.add_argument(
        "--skip-es",
        action="store_true",
        help="Build documents and parquet only; do not write to Elasticsearch",
    )
    parser.add_argument(
        "--skip-parquet",
        action="store_true",
        help="Do not save processed parquet files",
    )
    args = parser.parse_args()

    try:
        stats = run_sample_etl(
            write_elasticsearch=not args.skip_es,
            save_parquet=not args.skip_parquet,
            sample_size=args.sample_size,
        )
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}")
        print("Ensure MovieLens CSV files exist under data/raw/ (ratings.csv, movies.csv, tags.csv)")
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1

    print(
        "\nSample ETL complete:\n"
        f"  ratings loaded: {stats.ratings_loaded:,}\n"
        f"  ratings valid:  {stats.ratings_valid:,}\n"
        f"  movies:         {stats.movies_index_docs:,}\n"
        f"  release years:  {stats.release_year_index_docs:,}\n"
        f"  rating years:   {stats.rating_year_index_docs:,}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
