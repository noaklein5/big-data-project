"""Verify Stage 15 submission deliverables exist and meet minimum requirements."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

REQUIRED_FILES = {
    "README.md": ["docker compose up", ".env.example"],
    ".env.example": ["KAFKA_BOOTSTRAP_SERVERS", "ELASTICSEARCH_URL", "OLLAMA_MODEL", "DATA_MODE"],
    "docs/design.md": ["Architecture", "Data flow", "AI capability", "trade-off"],
    "docs/presentation.md": ["Problem and goal", "Architecture", "Demo", "Evaluation"],
    "docs/demo_script.md": ["movies_02", "rating_year_01", "movies_03", "movies_08"],
    "docs/ai_evaluation.md": ["Semantic correctness"],
    "docs/schema.md": ["movies_by_release_year"],
    "docker-compose.yml": ["movielens-app"],
}

OPTIONAL_BUT_RECOMMENDED = [
    "docs/plan.md",
    "docs/infrastructure.md",
    "docs/data_quality_summary.md",
    "kibana/insights.md",
]


def _check_file(path: Path, markers: list[str]) -> tuple[bool, str]:
    if not path.exists():
        return False, f"missing: {path.relative_to(REPO_ROOT)}"

    text = path.read_text(encoding="utf-8").lower()
    missing = [marker for marker in markers if marker.lower() not in text]
    if missing:
        return False, f"{path.relative_to(REPO_ROOT)} missing sections: {', '.join(missing)}"
    return True, f"{path.relative_to(REPO_ROOT)}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Stage 15 deliverables")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Also require optional documentation files",
    )
    args = parser.parse_args()

    print("Verifying Stage 15 deliverables...\n")
    passed = 0
    total = len(REQUIRED_FILES)

    for rel_path, markers in REQUIRED_FILES.items():
        ok, detail = _check_file(REPO_ROOT / rel_path, markers)
        status = "OK" if ok else "FAIL"
        print(f"[{status}] {detail}")
        if ok:
            passed += 1

    if args.strict:
        print("\nOptional files:")
        for rel_path in OPTIONAL_BUT_RECOMMENDED:
            path = REPO_ROOT / rel_path
            status = "OK" if path.exists() else "WARN"
            print(f"[{status}] {rel_path}")

    print(f"\n{passed}/{total} required deliverable checks passed.")
    if passed == total:
        print("\nStage 15 deliverables verification passed.")
        print("Submission bundle:")
        print("  - Source: git repo or ZIP")
        print("  - Design: docs/design.md")
        print("  - Slides: docs/presentation.md")
        print("  - Demo:   docs/demo_script.md")
        print("  - Dataset: https://grouplens.org/datasets/movielens/20m/")
        return 0

    print("\nStage 15 deliverables verification failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
