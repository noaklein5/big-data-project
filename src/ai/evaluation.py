"""Stage 14 AI evaluation — metrics and report generation."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.ai.generator import execute_query, generate_query
from src.ai.ollama_client import OllamaError
from src.ai.parser import ParseError
from src.ai.validator import ValidationError
from src.config import OLLAMA_MODEL
from src.elastic.gold_queries import load_gold_queries

DEFAULT_RESULTS_PATH = Path(__file__).resolve().parent.parent.parent / "docs" / "ai_evaluation_results.json"
DEFAULT_REPORT_PATH = Path(__file__).resolve().parent.parent.parent / "docs" / "ai_evaluation.md"


@dataclass(frozen=True)
class EvaluationOutcome:
    id: str
    question: str
    category: str
    expected_index: str
    valid_query: bool
    index_ok: bool
    executed: bool
    results_ok: bool
    semantic_ok: bool
    index: str | None
    hits: int
    agg_buckets: int
    error: str | None
    body: dict | None = None


@dataclass(frozen=True)
class EvaluationMetrics:
    total: int
    valid_query_rate: float
    index_selection_accuracy: float
    correct_result_rate: float
    semantic_correctness_rate: float
    elapsed_seconds: float

    def as_dict(self) -> dict:
        return asdict(self)


def evaluate_case(case: dict) -> EvaluationOutcome:
    """Run one NL question through Ollama and score against gold expectations."""
    question = case["question"]
    expected_index = case["index"]
    min_hits = case.get("min_hits", 0)
    min_agg = case.get("min_agg_buckets", 0)
    category = case.get("category", "unknown")

    outcome = EvaluationOutcome(
        id=case["id"],
        question=question,
        category=category,
        expected_index=expected_index,
        valid_query=False,
        index_ok=False,
        executed=False,
        results_ok=False,
        semantic_ok=False,
        index=None,
        hits=0,
        agg_buckets=0,
        error=None,
        body=None,
    )

    try:
        generated = generate_query(question)
    except (OllamaError, ParseError, ValidationError, ValueError) as exc:
        return outcome.__class__(**{**asdict(outcome), "error": str(exc)})

    outcome = outcome.__class__(
        **{
            **asdict(outcome),
            "valid_query": True,
            "index": generated.index,
            "index_ok": generated.index == expected_index,
            "body": generated.body,
        }
    )

    try:
        exec_result = execute_query(generated)
    except Exception as exc:
        return outcome.__class__(**{**asdict(outcome), "error": f"Elasticsearch error: {exc}"})

    results_ok = exec_result.hits >= min_hits and exec_result.agg_buckets >= min_agg
    semantic_ok = outcome.index_ok and results_ok

    return outcome.__class__(
        **{
            **asdict(outcome),
            "executed": True,
            "hits": exec_result.hits,
            "agg_buckets": exec_result.agg_buckets,
            "results_ok": results_ok,
            "semantic_ok": semantic_ok,
        }
    )


def evaluate_all_cases(
    cases: list[dict] | None = None,
    *,
    query_id: str | None = None,
) -> list[EvaluationOutcome]:
    loaded = cases if cases is not None else load_gold_queries()
    if query_id:
        loaded = [case for case in loaded if case["id"] == query_id]
        if not loaded:
            raise ValueError(f"Unknown query id: {query_id}")
    return [evaluate_case(case) for case in loaded]


def compute_metrics(outcomes: list[EvaluationOutcome], elapsed_seconds: float) -> EvaluationMetrics:
    total = len(outcomes)
    if total == 0:
        return EvaluationMetrics(0, 0.0, 0.0, 0.0, 0.0, elapsed_seconds)

    def rate(flag: str) -> float:
        return sum(1 for item in outcomes if getattr(item, flag)) / total

    return EvaluationMetrics(
        total=total,
        valid_query_rate=rate("valid_query"),
        index_selection_accuracy=rate("index_ok"),
        correct_result_rate=rate("results_ok"),
        semantic_correctness_rate=rate("semantic_ok"),
        elapsed_seconds=elapsed_seconds,
    )


def _pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _status_icon(outcome: EvaluationOutcome) -> str:
    if outcome.error:
        return "FAIL"
    if outcome.semantic_ok:
        return "OK"
    if outcome.executed:
        return "WARN"
    return "FAIL"


def format_report_markdown(
    outcomes: list[EvaluationOutcome],
    metrics: EvaluationMetrics,
    *,
    model: str = OLLAMA_MODEL,
) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# AI Evaluation Report (Stage 14)",
        "",
        f"Generated: {timestamp}",
        f"Model: `{model}`",
        f"Questions evaluated: {metrics.total}",
        f"Runtime: {metrics.elapsed_seconds:.1f}s",
        "",
        "## Summary metrics",
        "",
        "| Metric | Rate |",
        "| --- | --- |",
        f"| Valid query rate | {_pct(metrics.valid_query_rate)} |",
        f"| Index selection accuracy | {_pct(metrics.index_selection_accuracy)} |",
        f"| Correct-result rate | {_pct(metrics.correct_result_rate)} |",
        f"| Semantic correctness rate | {_pct(metrics.semantic_correctness_rate)} |",
        "",
        "Semantic correctness = correct index **and** results meet gold thresholds "
        "(min hits / aggregation buckets).",
        "",
        "## Results by question",
        "",
        "| ID | Category | Valid? | Index OK? | Results OK? | Semantic OK? | Notes |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]

    for outcome in outcomes:
        notes = outcome.error or (
            f"index={outcome.index}, hits={outcome.hits:,}, agg={outcome.agg_buckets}"
        )
        if len(notes) > 80:
            notes = notes[:77] + "..."
        lines.append(
            f"| {outcome.id} | {outcome.category} | "
            f"{'yes' if outcome.valid_query else 'no'} | "
            f"{'yes' if outcome.index_ok else 'no'} | "
            f"{'yes' if outcome.results_ok else 'no'} | "
            f"{'yes' if outcome.semantic_ok else 'no'} | {notes} |"
        )

    lines.extend(["", "## Question details", ""])
    for outcome in outcomes:
        lines.append(f"### {outcome.id}")
        lines.append("")
        lines.append(f"**Question:** {outcome.question}")
        lines.append("")
        lines.append(f"**Status:** {_status_icon(outcome)}")
        lines.append("")
        lines.append(f"- Expected index: `{outcome.expected_index}`")
        if outcome.index:
            lines.append(f"- Generated index: `{outcome.index}`")
        if outcome.error:
            lines.append(f"- Error: {outcome.error}")
        else:
            lines.append(f"- Hits: {outcome.hits:,}")
            lines.append(f"- Aggregation buckets: {outcome.agg_buckets}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def save_results_json(outcomes: list[EvaluationOutcome], metrics: EvaluationMetrics, path: Path) -> None:
    payload = {
        "metrics": metrics.as_dict(),
        "outcomes": [asdict(item) for item in outcomes],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_results_json(path: Path) -> tuple[list[EvaluationOutcome], EvaluationMetrics]:
    data = json.loads(path.read_text(encoding="utf-8"))
    outcomes = [EvaluationOutcome(**item) for item in data["outcomes"]]
    metrics = EvaluationMetrics(**data["metrics"])
    return outcomes, metrics
