# MovieLens Smart Analytics — Presentation Outline

**Duration:** 5–10 minutes + Q&A · **Format:** use these slides in PowerPoint, Google Slides, or Marp

---

## Slide 1 — Title

**MovieLens Smart Analytics**

Big Data pipeline + natural-language search over 20M movie ratings

Team · Big Data and AI course · 2026

---

## Slide 2 — Problem and goal

- **Problem:** Exploring movie rating data requires Elasticsearch expertise most analysts do not have.
- **Goal:** Build an end-to-end pipeline that loads MovieLens 20M and lets users ask questions in plain English.
- **Deliverables:** Working Docker stack, three ES indexes, Kibana insights, Streamlit demo, measured AI evaluation.

---

## Slide 3 — Dataset

- **Source:** [MovieLens 20M](https://grouplens.org/datasets/movielens/20m/) (GroupLens)
- **Scale:** 20M ratings · 27k movies · 465k tags
- **Semi-structured data:** pipe-separated genres, free-text titles, user-generated tags
- **Why it fits:** Rich enough for streaming ETL, search, and NL queries — not just a flat CSV

---

## Slide 4 — Architecture

*(Insert diagram from `docs/design.md` or draw on whiteboard)*

```text
CSV → Kafka → Spark ETL → Elasticsearch → Kibana / Streamlit
                              ↑
                         Ollama (NL → DSL)
```

Seven Docker services, one `docker compose up`.

---

## Slide 5 — Big Data pipeline

1. **Producer** publishes rating JSON to Kafka (`raw_ratings`)
2. **Spark** joins ratings + movies + tags, aggregates three index shapes
3. **Elasticsearch** stores searchable documents (~27k movies, ~118 release years, ~178k movie×year pairs on full load)
4. **Verification:** automated scripts at each stage (producer, Spark, indexes, integration)

---

## Slide 6 — Spark transformations

- Parse `release_year` from title `(YYYY)`
- Normalize tags (lowercase, dedupe per movie)
- **movies:** per-movie average rating, rating count, genres, tags
- **movies_by_release_year:** cohort rollups by release year
- **movie_ratings_by_rating_year:** rating activity per movie per calendar year
- Write directly to ES with explicit document IDs

---

## Slide 7 — Elasticsearch data model

| Index | Granularity | Example question |
| --- | --- | --- |
| `movies` | 1 doc / movie | Top Comedy films with 100+ ratings |
| `movies_by_release_year` | 1 doc / year | Compare 1990s vs 2000s cohorts |
| `movie_ratings_by_rating_year` | 1 doc / movie × year | Most active movies in 2010 |

**Rule:** `release_year` ≠ `rating_year` — never mix them.

---

## Slide 8 — Natural Language → Query (AI)

- **Model:** Ollama `llama3.2:3b` (local, no API cost)
- **Flow:** question → LLM → JSON DSL → validator → Elasticsearch → UI
- **Validator:** strict schema — allowed indexes/fields only, blocks scripts and malformed queries
- **Course AI type:** text-to-DSL (6.2d)

---

## Slide 9 — Demo (live)

Open **Streamlit** at http://localhost:8501

Use the four rehearsed questions from [`docs/demo_script.md`](demo_script.md):

1. Highest-rated Comedy movies (movies index)
2. Most popular movies in 2010 (rating activity index)
3. Movies tagged "pixar" (tag filter)
4. Action vs Comedy average rating (aggregation)

Fallback: run gold query IDs via `run_ai_evaluation.py --id … --show-dsl`.

---

## Slide 10 — Evaluation and insights

**AI evaluation (20 questions):**

| Metric | Result |
| --- | --- |
| Valid query rate | 95% |
| Index selection | 95% |
| Semantic correctness | 75% |

**Data insights (Kibana):**

- Drama is the largest genre bucket
- 2006 peak release year; 2000 peak rating activity
- Film-Noir highest avg rating among genres with 100+ movies

Full report: `docs/ai_evaluation.md` · Dashboard: Kibana → MovieLens Analytics

---

## Slide 11 — Challenges and trade-offs

- **Simulated streaming:** CSV replay into Kafka, not live user events
- **Single-node ES:** laptop-friendly, not production HA
- **Small LLM:** fast and private, but ~25% of gold questions fail semantic check
- **Spark memory tuning:** full 20M required worker memory limits and clean volume reset
- **Validator strictness:** rejects some valid DSL to keep demos safe

---

## Slide 12 — Summary and Q&A

- End-to-end pipeline over semi-structured MovieLens data ✅
- Course tech: Kafka, Spark, Elasticsearch, Docker ✅
- AI: NL → validated Elasticsearch DSL with measured accuracy ✅
- Repo: README + design doc + demo script + verification scripts

**Questions?**
