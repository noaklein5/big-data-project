"""Generate Elasticsearch queries from natural language."""

from __future__ import annotations

from dataclasses import dataclass

from elasticsearch import Elasticsearch

from src.ai.ollama_client import OllamaError, chat
from src.ai.parser import ParseError, ParsedQuery, parse_llm_response
from src.ai.prompt import build_system_prompt
from src.ai.validator import ValidationError, validate_query
from src.config import ELASTICSEARCH_URL
from src.elastic.gold_queries import count_agg_buckets


@dataclass(frozen=True)
class GeneratedQuery:
    question: str
    index: str
    body: dict
    raw_response: str


@dataclass(frozen=True)
class ExecutionResult:
    hits: int
    agg_buckets: int
    response: dict


def generate_query(question: str) -> GeneratedQuery:
    """Ask Ollama to produce an Elasticsearch index + DSL body for a question."""
    question = question.strip()
    if not question:
        raise ValueError("Question must not be empty")

    raw = chat(build_system_prompt(), question)
    parsed = parse_llm_response(raw)
    validated = validate_query(parsed.index, parsed.body)
    return GeneratedQuery(
        question=question,
        index=validated.index,
        body=validated.body,
        raw_response=raw,
    )


def execute_query(
    generated: GeneratedQuery | ParsedQuery,
    client: Elasticsearch | None = None,
) -> ExecutionResult:
    """Run a generated query against Elasticsearch."""
    es = client or Elasticsearch(ELASTICSEARCH_URL)
    validated = validate_query(generated.index, generated.body)
    response = es.search(index=validated.index, **validated.body)
    hits = int(response["hits"]["total"]["value"])
    agg_buckets = count_agg_buckets(response.get("aggregations"))
    return ExecutionResult(hits=hits, agg_buckets=agg_buckets, response=response)


def generate_and_execute(question: str) -> tuple[GeneratedQuery, ExecutionResult]:
    """Generate a query from natural language and execute it."""
    generated = generate_query(question)
    try:
        result = execute_query(generated)
    except Exception as exc:
        raise RuntimeError(
            f"Elasticsearch rejected query for index `{generated.index}`: {exc}"
        ) from exc
    return generated, result
