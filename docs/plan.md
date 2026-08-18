# MovieLens Smart Analytics — Project Plan

## Project Goal

Build a Big Data system over the **MovieLens 20M** dataset that allows users to explore movie ratings, genres, tags, popularity, and trends using **natural-language questions**.

The Big Data pipeline will process MovieLens data using Kafka, Spark, and Elasticsearch.  
The AI component will implement **Natural Language → Elasticsearch Query**:

```text
User Question
    ↓
LLM (Ollama)
    ↓
Elasticsearch DSL Query
    ↓
Validation
    ↓
Elasticsearch
    ↓
Real Results from MovieLens
```

The LLM generates the query, but the final answer is based on real results returned from the dataset.

---

## Project Constraints

| Constraint | Decision |
|---|---|
| Team size | Up to 3 members |
| Timeline | Flexible — sample-first, then full dataset |
| Deployment | Local laptop demo only — no cloud deployment |
| Source control | Git repository shared by the team |
| Infrastructure | All components run in Docker via `docker compose` |
| Docker footprint | Minimize container count and image size for a single laptop (~8–12 GB RAM) |
| LLM | Ollama with a small open-source model (`llama3.2:3b` recommended) |
| Spark output | Spark writes **directly** to Elasticsearch (elasticsearch-spark connector) |

### Semi-structured / unstructured data (course requirement)

MovieLens is mostly CSV, but the project satisfies the course requirement through:

- **Tags** — user-generated free text, normalized and aggregated per movie
- **Genres** — pipe-delimited semi-structured field parsed into arrays
- **Titles** — unstructured text parsed to extract release year

Document this explicitly in the design doc.

---

# Overall Architecture

```text
MovieLens 20M
ratings.csv + movies.csv + tags.csv
             │
             ▼
           Kafka
             │
             ▼
           Spark
      Cleaning + Joins
      + Aggregations
             │
             ▼ (direct write)
       Elasticsearch
             │
      ┌──────┴──────┐
      ▼             ▼
 Analytics        AI Layer
 / Kibana        User Question
                     ↓
                 Ollama (LLM)
                     ↓
              Elasticsearch DSL
                     ↓
                Validation
                     ↓
              Elasticsearch
                     ↓
                Real Results
```

All services run locally in Docker. Raw data is volume-mounted from `./data/raw`.

---

# Project Stages

## Stage 0 — Project Repository and Development Environment

**Current status:** in progress.

### Tasks
- Create the shared Git repository.
- Define the project folder structure.
- Create:
  - `requirements.txt`
  - `.gitignore` (include `data/raw/`, `.env`, `.venv/`)
  - `.env.example` (template — do not commit secrets)
  - initial `README.md`
- Create `docker-compose.yml` with the full local stack (see Stage 3).
- Download MovieLens 20M locally into `data/raw/`.
- Keep the large raw dataset outside Git.
- Verify `docker compose up` starts all services on one laptop.

### Suggested project structure

```text
movielens-bigdata-ai/
│
├── data/
│   ├── raw/              # MovieLens 20M — not in Git
│   └── processed/
│
├── notebooks/
│   └── 01_data_exploration.ipynb
│
├── src/
│   ├── producer/
│   ├── spark/
│   ├── elastic/
│   ├── ai/
│   └── app/
│
├── tests/
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

### End result
All team members can clone the repo, mount the dataset, and run `docker compose up`.

### Parallel work
No. Do this once as a team.

---

## Stage 1 — Explore and Understand MovieLens

Focus on three files:

### `ratings.csv`
```text
userId
movieId
rating
timestamp
```

### `movies.csv`
```text
movieId
title
genres
```

### `tags.csv`
```text
userId
movieId
tag
timestamp
```

### Tasks

#### Ratings analysis
- Number of rows.
- Number of users.
- Number of movies.
- Missing values.
- Duplicate rows.
- Rating distribution.
- Validate rating range.
- Timestamp range.
- Ratings per movie.
- Ratings per user.
- Derive `rating_year` from `timestamp` for time-trend analysis.

#### Movies analysis
- Number of movies.
- Missing values.
- Duplicate `movieId`.
- Parse movie year from title where possible.
- Parse `genres` separated by `|`.
- Count movies per genre.
- Check movies with `(no genres listed)`.

#### Tags analysis
- Number of tag records.
- Number of unique tags.
- Missing values.
- Duplicate tags.
- Normalize case for investigation.
- Most common tags.
- Tags per movie.
- Timestamp range.

#### Cross-file checks
- Verify `movieId` joins correctly.
- Movies with no ratings.
- Movies with no tags.
- Ratings referencing unknown movies.
- Tags referencing unknown movies.

### End result
A data-exploration notebook and a short data-quality summary.

### Parallel work
Yes.

Suggested split (up to 3 members):
- **Member A:** `ratings.csv`
- **Member B:** `movies.csv`
- **Member C:** `tags.csv`

Then combine conclusions together.

---

## Stage 2 — Define the Final Data Model

This stage must be agreed on by the whole team before the implementation branches diverge.

### Index 1: `movies` (main)

One document per movie with all-time aggregated stats.

```json
{
  "movie_id": 1,
  "title": "Toy Story",
  "release_year": 1995,
  "genres": ["Adventure", "Animation", "Children", "Comedy", "Fantasy"],
  "average_rating": 3.92,
  "rating_count": 49695,
  "tags": ["pixar", "animation", "funny"],
  "tag_count": 1520
}
```

Supports questions such as:

> Show Comedy movies released after 2000.

> What are the 10 highest-rated Comedy movies with at least 5,000 ratings?

### Index 2: `movies_by_release_year` (release-year cohorts)

One document per release year with cohort-level rollups.

```json
{
  "release_year": 2010,
  "movie_count": 842,
  "total_rating_count": 1250000,
  "average_rating": 3.45
}
```

Supports questions such as:

> Compare average ratings of movies released in the 1990s vs the 2000s.

> Which release years have the most movies?

> What were the highest-rated movies **released** in 2010?

For the last question, query the `movies` index with `release_year: 2010` and sort by `average_rating` or `rating_count`. The cohort index supports year-level comparisons and aggregations.

### Index 3: `movie_ratings_by_rating_year` (rating activity over time)

One document per `(movie_id, rating_year)` — `rating_year` is derived from the rating `timestamp`, **not** the movie release year.

```json
{
  "movie_id": 1,
  "title": "Toy Story",
  "rating_year": 2010,
  "rating_count": 1450,
  "average_rating": 4.02
}
```

Supports questions such as:

> What were the most popular movies **in 2010** (by rating activity that year)?

> How did rating activity for Toy Story change over time?

### Field naming rules (important for the LLM)

| Field | Meaning | Used in |
|---|---|---|
| `release_year` | Year the movie was released | `movies`, `movies_by_release_year` |
| `rating_year` | Calendar year the rating was submitted | `movie_ratings_by_rating_year` |

Never use a generic `year` field — always use `release_year` or `rating_year`.

### Decide together
- Exact field names and data types (locked as above).
- Fields that are searchable vs aggregation-only.
- Which Spark outputs map to which index.
- Which Natural Language questions the system must support.
- Spark → Elasticsearch direct-write configuration (index names, id fields).

### End result
A documented schema shared by Spark, Elasticsearch, and the AI layer.

### Parallel work
No. This is a synchronization point for the whole team.

---

## Stage 3 — Docker and Infrastructure

All components run in Docker on a single laptop. Minimize container count and memory usage.

### Target stack (~5–6 containers)

| Service | Image / notes | Memory hint |
|---|---|---|
| **kafka** | Bitnami Kafka, KRaft mode (no Zookeeper) | ~512 MB |
| **elasticsearch** | Single-node, `discovery.type=single-node` | 512 MB–1 GB heap |
| **kibana** | Matches ES version | ~512 MB |
| **spark** | Bitnami Spark — 1 master + 1 worker | 1–2 GB |
| **ollama** | Official Ollama image | 2–4 GB (model-dependent) |
| **app** | Python — producer, Streamlit UI, AI client, validator | ~512 MB |

### Design choices to reduce footprint
- Kafka KRaft instead of Kafka + Zookeeper (−1 container).
- Single Elasticsearch node with capped heap.
- One combined **app** container instead of separate producer/UI/AI services.
- Raw MovieLens data mounted as a volume (`./data/raw:/data`).
- Pull one small Ollama model: `llama3.2:3b`.

### Components
- Kafka (KRaft)
- Elasticsearch
- Kibana
- Spark
- Ollama
- App (Python)

### Tasks
- Create `docker-compose.yml` with all services above.
- Configure shared Docker network and volume mounts.
- Set memory limits where possible (`ES_JAVA_OPTS`, Spark worker memory).
- Start all services and verify connectivity.
- Create Kafka topic: `raw_ratings`.
- Verify Elasticsearch health (`/_cluster/health`).
- Verify Kibana connects to Elasticsearch.
- Pull Ollama model: `docker exec ollama ollama pull llama3.2:3b`.
- Document startup order and expected ports in `README.md`.

### End result
`docker compose up` starts the full local stack on one laptop.

### Parallel work
Yes. Can be done in parallel with Stage 4 after the schema is agreed.

---

## Stage 4 — Build a Small ETL Prototype

Do **not** start immediately with all 20 million ratings.

Use a sample, for example:

```text
100,000 ratings
```

### Tasks
- Load a small sample.
- Validate rating values.
- Convert timestamps and derive `rating_year`.
- Join ratings with movies.
- Parse genres and release year from title.
- Aggregate ratings by movie.
- Aggregate tags.
- Calculate movie-level: `average_rating`, `rating_count`, `tag_count`.
- Calculate release-year cohort stats for `movies_by_release_year`.
- Calculate `(movie_id, rating_year)` stats for `movie_ratings_by_rating_year`.
- Write sample output directly to Elasticsearch (validate connector setup).

### End result
A small processed dataset loaded into all three Elasticsearch indexes, matching the Stage 2 schema.

### Parallel work
Yes. Can be done in parallel with Stage 3.

---

## Stage 5 — Kafka Producer

Use Kafka primarily for the large ratings stream.

### Proposed design

```text
ratings.csv → Kafka
movies.csv  → Spark static input (volume mount)
tags.csv    → Spark static input (volume mount)
```

This demonstrates **streaming data + batch/static reference data** without unnecessary complexity.

The producer runs inside the **app** container (or is triggered from it).

### Kafka topic
```text
raw_ratings
```

### Example Kafka message

```json
{
  "userId": 23,
  "movieId": 356,
  "rating": 4.5,
  "timestamp": 123456789
}
```

### Tasks
- Build Python Kafka producer in `src/producer/`.
- Read `ratings.csv` from the mounted volume.
- Convert each row to JSON.
- Send records to `raw_ratings`.
- Add configurable **sample / full** modes via environment variable.
- Verify messages can be consumed.

### End result
Real MovieLens rating events are entering Kafka.

### Parallel work
Yes. Can overlap with Stage 6 once the schema and Kafka contract are fixed.

---

## Stage 6 — Spark ETL Pipeline

Spark performs the meaningful transformation and writes **directly** to Elasticsearch using the **elasticsearch-spark connector**.

### Proposed flow

```text
Read ratings from Kafka
        ↓
Validate rating range
        ↓
Convert timestamp → derive rating_year
        ↓
Read movies.csv (static)
        ↓
Parse title / release_year / genres
        ↓
Read tags.csv (static)
        ↓
Normalize and aggregate tags
        ↓
Join datasets
        ↓
Aggregate by movie → write to index: movies
        ↓
Aggregate by release_year → write to index: movies_by_release_year
        ↓
Aggregate by (movie_id, rating_year) → write to index: movie_ratings_by_rating_year
```

### Spark → Elasticsearch direct write

- Use `org.elasticsearch:elasticsearch-spark-30_2.12` (match Spark/Scala versions).
- Configure ES host, port, and index names via Spark config or `docker-compose` environment.
- Set document IDs explicitly (`movie_id` for `movies`; composite key for `movie_ratings_by_rating_year`).
- Test with the Stage 4 sample before running the full 20M dataset.

### Tasks
- Configure Spark Kafka source.
- Parse Kafka JSON messages.
- Validate and clean fields.
- Load static movie/tag data from volume mount.
- Perform joins.
- Parse genres and extract `release_year` from titles.
- Normalize tags (lowercase, deduplicate).
- Compute all three aggregation outputs.
- Write each output directly to its Elasticsearch index.
- Handle missing/invalid records.
- Support sample/full mode aligned with the Kafka producer.

### End result
The full ETL pipeline produces clean, aggregated data written directly into Elasticsearch.

### Parallel work
Partially. Kafka producer and Spark ETL can be developed by different members after their interface is agreed.

---

## Stage 7 — Elasticsearch Indexes and Mappings

Create all three indexes:

```text
movies
movies_by_release_year
movie_ratings_by_rating_year
```

### Mapping reference

#### `movies`
```text
movie_id         integer
title            text + keyword subfield
genres           keyword
release_year     integer
average_rating   float
rating_count     integer
tags             keyword
tag_count        integer
```

#### `movies_by_release_year`
```text
release_year         integer
movie_count          integer
total_rating_count   integer
average_rating       float
```

#### `movie_ratings_by_rating_year`
```text
movie_id         integer
title            text + keyword subfield
rating_year      integer
rating_count     integer
average_rating   float
```

### Tasks
- Define mappings for all three indexes.
- Create indexes (via `src/elastic/` setup script or Spark write with mapping hints).
- Load sample data from Stage 4.
- Verify exact-match filters (`genres`, `tags`, `release_year`, `rating_year`).
- Verify numeric ranges and sorting.
- Verify aggregations (genre averages, release-year cohorts, rating-year trends).
- Verify tag searching.

### End result
Elasticsearch contains queryable MovieLens data across all three indexes.

### Parallel work
Yes. Mappings can be developed using Stage 4 sample output while Spark is being completed.

---

## Stage 8 — Build Manual Elasticsearch Queries

Before using an LLM, manually create the queries the system is expected to generate.

These become **gold/reference queries** for testing the AI.

Include queries for **all three indexes** and both **filter/sort** and **aggregation** patterns.

### Example questions

#### Filters and sorting (`movies` index)
> Show Comedy movies released after 2000.

> What are the 10 highest-rated Comedy movies with at least 5,000 ratings?

> Show movies tagged "dark" with an average rating above 4.

> Show highly rated Horror movies tagged "funny".

#### Aggregations (`movies` index)
> Which genres have the highest average rating?

> Compare the average ratings of Action and Comedy movies.

#### Release year (`movies` or `movies_by_release_year`)
> What are the highest-rated movies **released** in 2010?

> Which release decades have the highest average rating?

#### Rating activity over time (`movie_ratings_by_rating_year`)
> What were the most popular movies **by rating activity** in 2010?

> Show how rating activity changed for a specific movie across years.

### Tasks
- Define 15–20 target natural-language questions covering all categories above.
- Write the correct Elasticsearch DSL manually for each.
- Specify which index each query targets.
- Verify each query against Elasticsearch.
- Save cases in `tests/gold_queries/` for later evaluation.

### End result
A tested query set independent of the LLM.

### Parallel work
Yes. Can be performed while Spark and Elasticsearch integration are being completed.

---

## Stage 9 — AI: Natural Language → Elasticsearch Query

This is the graded AI capability.

### LLM setup
- **Runtime:** Ollama in Docker
- **Model:** `llama3.2:3b` (small, runs on a laptop)
- **Endpoint:** `http://ollama:11434` (from app container)

### Input to the LLM

#### User question
```text
What are the 10 highest-rated Comedy movies
with at least 5,000 ratings?
```

#### Elasticsearch schema (provide all three indexes)
```text
Index: movies
  title: text
  genres: keyword[]
  release_year: integer
  average_rating: float
  rating_count: integer
  tags: keyword[]
  tag_count: integer

Index: movies_by_release_year
  release_year: integer
  movie_count: integer
  total_rating_count: integer
  average_rating: float

Index: movie_ratings_by_rating_year
  movie_id: integer
  title: text
  rating_year: integer   ← year the rating was submitted, NOT release year
  rating_count: integer
  average_rating: float
```

#### Instructions for the LLM
- Generate Elasticsearch DSL JSON only.
- Choose the correct index based on the question.
- Use `release_year` for when a movie came out.
- Use `rating_year` for when ratings were submitted.
- Include `aggs` for aggregation questions (genres, decades, comparisons).

### Example output (filter + sort on `movies`)

```json
{
  "size": 10,
  "query": {
    "bool": {
      "filter": [
        { "term": { "genres": "Comedy" } },
        { "range": { "rating_count": { "gte": 5000 } } }
      ]
    }
  },
  "sort": [{ "average_rating": "desc" }]
}
```

### Example output (aggregation on `movies`)

```json
{
  "size": 0,
  "aggs": {
    "by_genre": {
      "terms": { "field": "genres", "size": 20 },
      "aggs": {
        "avg_rating": { "avg": { "field": "average_rating" } }
      }
    }
  }
}
```

### Tasks
- Integrate Ollama client in `src/ai/`.
- Define system prompt with full schema and field semantics.
- Restrict output to JSON/DSL only.
- Parse model output (strip markdown fences if present).
- Handle malformed output gracefully.
- Test against the Stage 8 gold query set.

### End result
Natural-language questions reliably generate Elasticsearch queries against the correct index.

### Parallel work
Yes. Prompt design and gold queries can begin before the full dataset pipeline is finished.

---

## Stage 10 — Query Validator

Never execute LLM output blindly.

### Validation flow

```text
LLM output
    ↓
Valid JSON?
    ↓
Allowed Elasticsearch structure?
    ↓
Uses only existing fields on the target index?
    ↓
Read-only query?
    ↓
Reasonable result size?
    ↓
Execute query
```

### Tasks
- Validate JSON syntax.
- Allow only search/query operations (`query`, `aggs`, `sort`, `size`, `_source`).
- Reject update/delete/index-management operations.
- Validate field names against the schema for the target index.
- Reject queries using `year` — require `release_year` or `rating_year`.
- Apply result-size limits (e.g. `size` ≤ 100).
- Return useful errors to the UI.
- Log rejected queries for testing.

### End result
Only safe, valid search queries reach Elasticsearch.

### Parallel work
Yes. Can be developed in parallel with Stage 9.

---

## Stage 11 — Demo Application

Keep the interface simple. Runs locally in the **app** Docker container (Streamlit).

### Proposed UI

```text
┌──────────────────────────────────────────────┐
│ MovieLens Smart Analytics                   │
│                                              │
│ Ask a question about MovieLens:             │
│                                              │
│ [ Which Comedy movies have the highest... ] │
│                                              │
│                   [ Search ]                 │
└──────────────────────────────────────────────┘
```

### Display
1. User question.
2. Target index (detected or inferred).
3. Generated Elasticsearch query.
4. Real returned results.
5. Optional short explanation (clearly labeled as LLM-generated).

### Tasks
- Build Streamlit UI in `src/app/`.
- Connect to Ollama and Elasticsearch over the Docker network.
- Show generated DSL before execution.
- Execute validated query.
- Display results clearly (table for hits, JSON for aggregations).
- Handle errors (LLM failure, invalid query, ES timeout).

### End result
A working local demo interface — no deployment required.

### Parallel work
Yes. Can run in parallel with Stage 12.

---

## Stage 12 — Kibana and Data Insights

Create a small dashboard showing that the Big Data pipeline produces useful analytical results.

### Possible visualizations
- Most-rated movies.
- Average rating by genre.
- Rating distribution.
- Ratings over time (`rating_year` from `movie_ratings_by_rating_year`).
- Movies by release year (`movies_by_release_year`).
- Most common tags.
- Average rating vs. number of ratings.

### Tasks
- Create Kibana data views for all three indexes.
- Build 4–6 useful charts.
- Extract 3–5 meaningful observations from the actual data.

### End result
A dashboard and several evidence-based insights for the presentation.

### Parallel work
Yes. Fully parallel with the demo/UI work.

---

## Stage 13 — Full Integration

Stop working independently and connect everything.

### End-to-end flow

```text
docker compose up
        ↓
Kafka + Spark + Elasticsearch + Ollama + App
        ↓
Producer (sample or full mode)
        ↓
Spark ETL → direct write to Elasticsearch
        ↓
Streamlit App
```

### Test AI flow

```text
User Question
        ↓
Ollama (llama3.2:3b)
        ↓
Elasticsearch DSL
        ↓
Validator
        ↓
Elasticsearch
        ↓
Real Results
```

### Tasks
- Run the complete stack on one laptop.
- Test pipeline from raw data through all three ES indexes.
- Switch from sample to **full 20M dataset**.
- Test all AI components against the final indexes.
- Fix schema mismatches between Spark output and ES mappings.
- Test restart behavior (`docker compose down && docker compose up`).
- Document startup order and sample/full mode in `README.md`.

### End result
A complete working system running locally.

### Parallel work
No. This is a whole-team synchronization point.

---

## Stage 14 — Evaluate the AI Component

Prepare approximately **20 natural-language questions**.

Suggested categories:

```text
5 simple filters        (movies index)
5 aggregations          (movies index)
5 release_year questions (movies / movies_by_release_year)
5 rating_year / tag questions (movie_ratings_by_rating_year / movies)
```

### Evaluation table

| Question | Index | Valid query? | Semantically correct? | Correct result? |
|---|---|---:|---:|---:|
| Top Comedy movies | movies | ✅ | ✅ | ✅ |
| Movies tagged dark | movies | ✅ | ✅ | ✅ |
| Genre avg rating | movies | ✅ | ✅ | ✅ |
| Popular by rating activity in 2010 | movie_ratings_by_rating_year | ✅ | ✅ | ✅ |
| Best movies released in 2010 | movies | ✅ | ✅ | ✅ |
| ... | ... | ... | ... | ... |

### Metrics
```text
Valid query rate
Semantic correctness rate
Correct-result rate
Index selection accuracy (correct index chosen)
```

Do not invent the final percentages. Report the actual test results.

### End result
Evidence that the AI component was tested, not only demonstrated on cherry-picked examples.

### Parallel work
Yes. Evaluation questions/results can be divided among team members.

---

## Stage 15 — Deliverables and Presentation

### Required deliverables

#### Source code
- Git repository (or ZIP export).
- Clean project structure.
- `README.md` with `docker compose up` instructions.
- `.env.example` documenting all configuration variables.

#### Design document (1–2 pages)
- Problem and goal.
- Dataset (link to MovieLens 20M).
- Architecture diagram.
- Data flow (Kafka → Spark → Elasticsearch → Ollama → App).
- Semi-structured/unstructured data justification (tags, genres, titles).
- Technologies and why each was chosen.
- AI capability (NL → ES DSL with validation).
- Main trade-offs (simulated streaming, single-node ES, small LLM).

#### Presentation (5–10 minutes)
1. Problem and goal.
2. MovieLens dataset.
3. Architecture.
4. Big Data pipeline.
5. Spark transformations and direct ES write.
6. Elasticsearch data model (three indexes, field semantics).
7. Natural Language → Query AI (Ollama).
8. Demo.
9. Evaluation/results.
10. Challenges and trade-offs.

#### Demo
Prepare 3–4 reliable questions in advance:

- Highest-rated Comedy movies with enough ratings (`movies`).
- Most popular movies by rating activity in a specific year (`movie_ratings_by_rating_year`).
- Movies associated with a specific tag (`movies`).
- Genre comparison or release-year cohort question (`movies` / `movies_by_release_year`).

### End result
All submission requirements are complete.

### Parallel work
Yes.

Suggested split:
- One member: README and run instructions.
- One member: architecture/design document.
- One member: presentation/demo preparation.

Final review should be done by the whole team.

---

# Recommended Team Split (up to 3 members)

| Member | Primary Responsibility | Secondary Responsibility |
|---|---|---|
| **A — Data** | Kafka + Spark ETL + direct ES write | Data exploration |
| **B — Search** | Elasticsearch mappings + Kibana | Manual/reference queries |
| **C — AI/App** | Ollama + Validator + Streamlit UI | AI evaluation |

If the team has fewer than 3 members, combine adjacent tracks (e.g. A takes Data + Search infra, C takes AI/App).

---

# Parallel Development Plan

## Phase 1 — Together

```text
Stage 0 — Environment + Docker stack
Stage 1 — Dataset exploration
Stage 2 — Final data model (3 indexes, field semantics)
```

Everyone should understand and agree on these.

## Phase 2 — Parallel Tracks

```text
                     AGREED DATA MODEL
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
     MEMBER A           MEMBER B           MEMBER C
        DATA               SEARCH              AI
          │                 │                 │
       Kafka             ES mappings       Gold queries
       Spark             Kibana            Ollama prompt
       ES direct write  Manual queries    Validator + UI
          │                 │                 │
          └─────────────────┼─────────────────┘
                            ▼
                       INTEGRATION
```

### Track A — Data
- Kafka producer (sample/full mode).
- Spark ETL pipeline.
- Direct write to all three ES indexes.

### Track B — Search
- Elasticsearch mappings and index setup.
- Sample loading and query verification.
- Kibana dashboard.

### Track C — AI
- Gold natural-language questions and reference DSL.
- Ollama integration (`llama3.2:3b`).
- Query validator.
- Streamlit demo UI.

## Phase 3 — Together

```text
Stage 13 — Integration (full dataset)
Stage 14 — Evaluation
Stage 15 — Final review and demo
```

All team members should test and understand the complete project.

---

# Critical Path

```text
Dataset
   ↓
Data Model (3 indexes, release_year vs rating_year)
   ↓
Docker Stack
   ↓
Elasticsearch Mappings
   ↓
Spark ETL → Direct ES Write
   ↓
Gold Queries
   ↓
Ollama Query Generation + Validator
   ↓
Full Demo (local laptop)
```

The **Data Model** is the main synchronization point. Do not let development tracks diverge before field names, index purposes, and `release_year` vs `rating_year` semantics are agreed.

---

# Recommended Project Scope

## Include
- MovieLens `ratings.csv`, `movies.csv`, `tags.csv`
- Docker Compose stack (Kafka, Spark, Elasticsearch, Kibana, Ollama, App)
- Kafka for rating ingestion (simulated stream from CSV)
- Spark for transformation, aggregation, and **direct Elasticsearch write**
- Three Elasticsearch indexes with clear field semantics
- Kibana for analytical dashboards
- Ollama (`llama3.2:3b`) for Natural Language → Elasticsearch Query
- Query validation
- Streamlit demo UI (local laptop)
- AI evaluation set (~20 questions)

## Do not include unless there is extra time
- Recommendation engine
- Embeddings/vector search
- RAG
- Deep-learning models
- Cloud deployment
- `genome-scores.csv`, `genome-tags.csv`

The goal is to deliver a working system that the whole team can explain clearly.
