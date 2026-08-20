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


| Constraint       | Decision                                                                   |
| ---------------- | -------------------------------------------------------------------------- |
| Team size        | Up to 3 members                                                            |
| Timeline         | Flexible — sample-first, then full dataset                                 |
| Deployment       | Local laptop demo only — no cloud deployment                               |
| Source control   | Git repository shared by the team                                          |
| Infrastructure   | All components run in Docker via `docker compose`                          |
| Docker footprint | Minimize container count and image size for a single laptop (~8–12 GB RAM) |
| LLM              | Ollama with a small open-source model (`llama3.2:3b` recommended)          |
| Spark output     | Spark writes **directly** to Elasticsearch (elasticsearch-spark connector) |


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

# Full Setup and Run Instructions

This section is the **complete operator guide** for getting the project running on a laptop. Follow steps in order.


| Step | What                                                 |
| ---- | ---------------------------------------------------- |
| 1    | Clone repo and configure `.env`                      |
| 2    | Download MovieLens 20M into `data/raw/`              |
| 3    | Start Docker stack                                   |
| 4    | Pull Ollama model                                    |
| 5    | Initialize infrastructure (Kafka topic + ES indexes) |
| 6    | Verify all services                                  |
| 7    | Run data exploration notebook (optional)             |
| 8    | Review locked schema                                 |
| 9    | Run sample ETL → load Elasticsearch                  |
| 10   | Stream ratings into Kafka (Stage 5)                  |
| 11   | Run Spark ETL → Elasticsearch (Stage 6)              |
| 12   | Verify ES indexes and queries (Stage 7)              |
| 13   | Run gold query verification (Stage 8)                |
| 14   | Run NL query / AI evaluation (Stage 9)               |
| 15   | Verify query validator (Stage 10)                    |
| 16   | Open Streamlit demo UI (Stage 11)                    |

**All copy-paste commands in one place:** [Command cheat sheet — copy/paste restore](#command-cheat-sheet--copypaste-restore)

Stages 0–11 are complete when Steps 1–16 pass.

## Progress tracker


| Stage | Description                        | Status     |
| ----- | ---------------------------------- | ---------- |
| 0     | Repo, Docker stack, config         | ✅ Complete |
| 1     | Data exploration                   | ✅ Complete |
| 2     | Locked data model                  | ✅ Complete |
| 3     | Docker infrastructure              | ✅ Complete |
| 4     | Small ETL prototype (100k ratings) | ✅ Complete |
| 5     | Kafka producer                     | ✅ Complete |
| 6     | Spark ETL pipeline                 | ✅ Complete |
| 7     | ES mappings + verify Spark output  | ✅ Complete |
| 8     | Gold queries                       | ✅ Complete |
| 9     | AI: NL → Elasticsearch query       | ✅ Complete |
| 10    | Query validator                      | ✅ Complete |
| 11    | Streamlit demo UI                    | ✅ Complete |
| 12    | Kibana dashboards                  | ⬜ Next     |
| 13    | Full integration (20M)             | ⬜ Pending  |
| 14–15 | Evaluation + deliverables          | ⬜ Pending  |


---

## Prerequisites


| Requirement             | Notes                                                                         |
| ----------------------- | ----------------------------------------------------------------------------- |
| Docker Desktop          | Must be **running** before any `docker` command                               |
| RAM                     | ~8–12 GB free (Ollama alone uses 2–4 GB with model loaded)                    |
| Disk                    | ~3 GB for Docker images + ~700 MB for MovieLens CSVs + ~2 GB for Ollama model |
| Git                     | Clone the shared repository                                                   |
| Python 3.11+ (optional) | For local notebooks only — Docker app uses Python 3.11                        |


---

## Step 1 — Clone and configure environment

```bash
git clone <repository-url>
cd bigData
cp .env.example .env
```

On Windows PowerShell:

```powershell
git clone <repository-url>
cd bigData
copy .env.example .env
```

### `.env` variables (do not change hostnames)

These URLs use **Docker internal hostnames** (`kafka`, `elasticsearch`, etc.). They are correct for containers; do **not** replace with `localhost` in `.env`.


| Variable                  | Default                     | Purpose                           |
| ------------------------- | --------------------------- | --------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092`                | Kafka broker                      |
| `KAFKA_TOPIC_RAW_RATINGS` | `raw_ratings`               | Ratings stream topic              |
| `ELASTICSEARCH_URL`       | `http://elasticsearch:9200` | Elasticsearch                     |
| `KIBANA_URL`              | `http://kibana:5601`        | Kibana                            |
| `SPARK_MASTER_URL`        | `spark://spark:7077`        | Spark cluster                     |
| `SPARK_UI_URL`            | `http://spark:8080`         | Spark master UI                   |
| `OLLAMA_URL`              | `http://ollama:11434`       | Ollama API                        |
| `OLLAMA_MODEL`            | `llama3.2:3b`               | LLM for query generation          |
| `DATA_MODE`               | `sample`                    | `sample` or `full`                |
| `SAMPLE_RATINGS`          | `100000`                    | Ratings count in sample mode      |
| `DATA_RAW_PATH`           | `/data/raw`                 | Raw CSV path inside containers    |
| `DATA_PROCESSED_PATH`     | `/data/processed`           | ETL output path inside containers |


---

## Step 2 — Download MovieLens 20M

1. Download from [MovieLens 20M](https://grouplens.org/datasets/movielens/20m/)
2. Extract into `data/raw/`
3. Ensure **exact filenames**:

```text
data/raw/
  ratings.csv    ← not rating.csv
  movies.csv     ← not movie.csv
  tags.csv       ← not tag.csv
```

These files are **not** committed to Git.

### Expected file sizes (approximate)


| File          | Rows        | Size    |
| ------------- | ----------- | ------- |
| `ratings.csv` | ~20,000,263 | ~659 MB |
| `movies.csv`  | ~27,278     | ~1.5 MB |
| `tags.csv`    | ~465,564    | ~21 MB  |


---

## Step 3 — Start Docker stack

**Before running:** open Docker Desktop and wait until it says **running**.

```bash
docker compose up -d --build
```

### What happens

1. **Build** (~1 min) — builds the Python `app` image
2. **Pull images** (first time only) — Kafka, ES, Spark, Ollama, etc.
3. **Start containers** — Elasticsearch and Kafka first, then others
4. **Healthchecks** — app waits until Kafka, Elasticsearch, and Ollama are healthy

**First boot:** 2–5 minutes is normal. The command may appear idle while waiting for healthchecks — **do not cancel** unless it exceeds ~10 minutes.

### Monitor progress

```bash
docker compose ps
```

Wait until all containers show `running` and key services show `(healthy)`:

```text
movielens-elasticsearch   running (healthy)
movielens-kafka           running (healthy)
movielens-kibana          running (healthy)
movielens-ollama          running (healthy)
movielens-spark           running (healthy)
movielens-app             running
movielens-spark-worker    running
```

### Service URLs (from your browser)


| Service         | URL                                              | Purpose              |
| --------------- | ------------------------------------------------ | -------------------- |
| Elasticsearch   | [http://localhost:9200](http://localhost:9200)   | API / cluster health |
| Kibana          | [http://localhost:5601](http://localhost:5601)   | Dashboards           |
| Spark master UI | [http://localhost:8080](http://localhost:8080)   | Spark cluster status |
| Ollama          | [http://localhost:11434](http://localhost:11434) | LLM API              |
| App (Streamlit) | [http://localhost:8501](http://localhost:8501)   | Demo UI (Stage 11)   |


### Docker stack details


| Container                 | Image                | Memory limit |
| ------------------------- | -------------------- | ------------ |
| `movielens-kafka`         | `apache/kafka:3.7.0` | 768 MB       |
| `movielens-elasticsearch` | ES 8.15.0            | 1 GB         |
| `movielens-kibana`        | Kibana 8.15.0        | 768 MB       |
| `movielens-spark`         | Apache Spark 3.5.9   | 768 MB       |
| `movielens-spark-worker`  | Apache Spark 3.5.9   | 1.5 GB       |
| `movielens-ollama`        | Ollama latest        | 4 GB         |
| `movielens-app`           | Python 3.11 (built)  | 512 MB       |


**Note:** Bitnami Kafka/Spark images are unavailable on Docker Hub; this project uses official **Apache** images.

### Startup order (automatic via healthchecks)

```text
elasticsearch + kafka + ollama
        ↓
kibana (after ES) · spark → spark-worker · app (after kafka + ES + ollama)
```

---

## Step 4 — Pull Ollama model

```bash
docker exec movielens-ollama ollama pull llama3.2:3b
```

Download is ~2 GB; may take several minutes.

Verify:

```bash
docker exec movielens-ollama ollama list
```

Expected: `llama3.2:3b` listed (~2.0 GB).

---

## Step 5 — Initialize infrastructure

Creates Kafka topic and Elasticsearch indexes:

```bash
docker exec movielens-app python scripts/setup_infrastructure.py
```

### Expected output

```text
Elasticsearch: OK (yellow)
Kafka broker: OK
Kafka topic: created (raw_ratings)
Kibana: OK
Spark master UI: OK
Ollama: OK
Ollama model: OK (llama3.2:3b)
Created index: movies                    ← or "Index already exists"
Created index: movies_by_release_year
Created index: movie_ratings_by_rating_year
Infrastructure setup complete.
```

`**yellow` Elasticsearch status is normal** on a single-node laptop.

---

## Step 6 — Verify all services

```bash
docker exec movielens-app python scripts/verify_stack.py
```

### Expected output

```text
Elasticsearch: OK (yellow)
Kafka: OK (topic `raw_ratings` exists)
Kibana: OK
Spark: OK
Ollama: OK (model `llama3.2:3b` available)

All infrastructure services are reachable.
```

Optional schema check:

```bash
docker exec movielens-app python scripts/validate_schema.py
```

---

## Step 7 — Run data exploration (Stage 1)

### Option A — Local notebook (recommended for exploration)

```powershell
cd bigData
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt ipykernel
.\.venv\Scripts\python -m ipykernel install --user --name=bigdata --display-name="Python (bigData)"
```

Open `notebooks/01_data_exploration.ipynb` and select kernel **Python (bigData)**.

### Option B — Read the summary

See `docs/data_quality_summary.md` for key findings without re-running the notebook.

---

## Step 8 — Review locked schema (Stage 2)

Before building ETL or AI components, read:

- `docs/schema.md` — human-readable schema
- `src/elastic/schema.py` — code module shared by Spark, ES, and AI

Key rule: use `release_year` (movie release) vs `rating_year` (when rating was submitted). Never use generic `year`.

Validate the schema module:

```bash
docker exec movielens-app python scripts/validate_schema.py
```

Expected: `Schema validation passed.`

---

## Step 9 — Run sample ETL (Stage 4)

Loads the first **100,000** ratings (configurable via `SAMPLE_RATINGS` in `.env`), transforms them, and writes to all three Elasticsearch indexes.

**Prerequisites:** Steps 1–6 complete, MovieLens CSVs in `data/raw/`, stack running.

```bash
docker exec movielens-app python scripts/run_sample_etl.py
```

### What the pipeline does

1. Read first `SAMPLE_RATINGS` rows from `ratings.csv` (default: 100,000)
2. Validate ratings (0.5–5.0, half-star steps only)
3. Derive `rating_year` from the rating timestamp
4. Join with `movies.csv` — parse `release_year` and `genres`
5. Aggregate normalized tags from `tags.csv` per movie
6. Build three document sets matching the locked schema:
  - `movies` — one doc per movie in the sample
  - `movies_by_release_year` — cohort rollups by release year
  - `movie_ratings_by_rating_year` — stats per `(movie_id, rating_year)`
7. Save parquet files under `data/processed/`
8. Bulk-index into Elasticsearch using document IDs from `src/elastic/schema.py`

### Expected output

```text
Loading ratings sample (limit=100000) from /data/raw
Aggregating tags for 8,227 movies in sample
Built documents: movies=8,227, release_year=102, rating_year=36,447
Saved processed parquet files under /data/processed
Indexed 8,227 documents into movies
Indexed 102 documents into movies_by_release_year
Indexed 36,447 documents into movie_ratings_by_rating_year

Sample ETL complete:
  ratings loaded: 100,000
  ratings valid:  100,000
  movies:         8,227
  release years:  102
  rating years:   36,447
```

Exact movie/year counts vary slightly depending on which ratings appear in the first 100k rows.

### Verify the load

```bash
docker exec movielens-app python scripts/verify_sample_etl.py
```

Expected: non-zero document counts in all three indexes and `Sample ETL verification passed.`

### Optional flags

```bash
# Smaller sample for faster iteration
docker exec movielens-app python scripts/run_sample_etl.py --sample-size 50000

# Build parquet only — skip Elasticsearch write
docker exec movielens-app python scripts/run_sample_etl.py --skip-es

# Skip parquet output — Elasticsearch write only
docker exec movielens-app python scripts/run_sample_etl.py --skip-parquet
```

### Output files


| Location                                                     | Contents                                                                                    |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------- |
| Elasticsearch `movies`                                       | Per-movie stats: title, release_year, genres, average_rating, rating_count, tags, tag_count |
| Elasticsearch `movies_by_release_year`                       | Cohort rollups: movie_count, total_rating_count, weighted average_rating                    |
| Elasticsearch `movie_ratings_by_rating_year`                 | Time-series: rating_count and average_rating per movie per rating year                      |
| `data/processed/sample_movies.parquet`                       | Same as `movies` index                                                                      |
| `data/processed/sample_movies_by_release_year.parquet`       | Same as release-year index                                                                  |
| `data/processed/sample_movie_ratings_by_rating_year.parquet` | Same as rating-year index                                                                   |


### Inspect in Elasticsearch (optional)

From your browser or curl:

```bash
# Document counts
curl http://localhost:9200/movies/_count
curl http://localhost:9200/movies_by_release_year/_count
curl http://localhost:9200/movie_ratings_by_rating_year/_count

# Sample document (Toy Story if in sample)
curl "http://localhost:9200/movies/_doc/1?pretty"
```

Or open **Kibana** at [http://localhost:5601](http://localhost:5601) → Dev Tools → run:

```json
GET movies/_search
{
  "size": 5,
  "sort": [{ "rating_count": "desc" }]
}
```

### Re-run after code changes

The app container mounts `./src` and `./scripts` as volumes — code changes apply without rebuild. Rebuild only after changing `requirements.txt` or `Dockerfile`:

```bash
docker compose up -d --build app
docker exec movielens-app python scripts/run_sample_etl.py
```

Re-running the ETL **upserts** documents (same IDs overwrite existing docs).

### Code locations


| File                           | Purpose                                      |
| ------------------------------ | -------------------------------------------- |
| `src/etl/transforms.py`        | Parsing, validation, timestamp → rating_year |
| `src/etl/sample_pipeline.py`   | Full pipeline logic                          |
| `scripts/run_sample_etl.py`    | CLI entry point                              |
| `scripts/verify_sample_etl.py` | Post-load verification                       |


---

## Step 10 — Stream ratings into Kafka (Stage 5)

Publishes MovieLens rating rows from `ratings.csv` to the `**raw_ratings**` Kafka topic as JSON messages.

**Prerequisites:** Steps 1–6 complete (Kafka topic must exist).

```bash
docker exec movielens-app python scripts/run_producer.py
docker exec movielens-app python scripts/verify_producer.py
```

### What the producer does

1. Read `ratings.csv` from `/data/raw/`
2. Convert each row to JSON (`userId`, `movieId`, `rating`, `timestamp`)
3. Publish to topic `raw_ratings` with `movieId` as the message key
4. In **sample mode** (`DATA_MODE=sample`), send first `SAMPLE_RATINGS` rows (default: 100,000)
5. In **full mode** (`DATA_MODE=full`), send all ~20M ratings

### Expected output

```text
Streaming ratings from /data/raw/ratings.csv to topic `raw_ratings` (mode=sample, limit=100000)
  sent 10,000 messages (12,345/s)
  sent 20,000 messages (12,500/s)
  ...
Finished: 100,000 messages in 8.2s

Producer complete:
  topic:     raw_ratings
  mode:      sample
  rows read: 100,000
  rows sent: 100,000
```

Verify:

```text
Topic message count: 100,000
Consumed 5 sample message(s):

  [1] {"movieId": 2, "rating": 3.5, "timestamp": "2005-04-02 23:53:47", "userId": 1}
  ...

Producer verification passed.
```

### Optional flags

```bash
# Override sample size
docker exec movielens-app python scripts/run_producer.py --sample-size 5000

# Send all 20M ratings (slow — use only when ready)
docker exec movielens-app python scripts/run_producer.py --full

# Read more sample messages during verify
docker exec movielens-app python scripts/verify_producer.py --sample-messages 10
```

**Note:** Re-running the producer **appends** messages to the topic (duplicates are expected). For a clean topic, reset volumes: `docker compose down -v` then re-run setup.

### Code locations


| File                               | Purpose                       |
| ---------------------------------- | ----------------------------- |
| `src/producer/message.py`          | JSON message contract         |
| `src/producer/ratings_producer.py` | CSV → Kafka streaming logic   |
| `scripts/run_producer.py`          | CLI entry point               |
| `scripts/verify_producer.py`       | Consume and validate messages |


---

## Step 11 — Run Spark ETL (Stage 6)

Spark reads ratings from **Kafka**, joins static **movies/tags** CSVs, aggregates, and writes to all three Elasticsearch indexes.

**Prerequisites:** Steps 1–10 complete (ratings must be in Kafka).

Run from the **project root on your host** (not inside `movielens-app`):

```powershell
python scripts/run_spark_etl.py
```

Or submit manually:

```powershell
docker exec movielens-spark-worker /opt/spark/bin/spark-submit `
  --master spark://spark:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0 `
  --driver-memory 512m --executor-memory 768m `
  --conf spark.jars.ivy=/opt/spark-jobs/.ivy2 `
  /opt/spark-jobs/etl_job.py
```

### What the job does

1. Batch-read all messages from Kafka topic `raw_ratings` (earliest → latest offsets)
2. Parse JSON, validate ratings, derive `rating_year`
3. Load `movies.csv` and `tags.csv` from `/data/raw/`
4. Join, aggregate — same logic as Stage 4
5. Write to ES via `elasticsearch-spark-30_2.12` connector (upsert by document ID)

### Expected output

```text
Reading ratings from Kafka topic `raw_ratings`...
  valid ratings: 100,000
Reading static movies and tags...
Built outputs: movies=8,227, release_year=102, rating_year=36,447
Writing to Elasticsearch at elasticsearch:9200...
  wrote index `movies`
  wrote index `movies_by_release_year`
  wrote index `movie_ratings_by_rating_year`

Spark ETL complete:
  ratings processed: 100,000
  movies index:      8,227
  release_year:      102
  rating_year:       36,447
```

**First run** downloads Maven JARs (~60 MB) into `src/spark/.ivy2/` — may take 1–2 minutes. Subsequent runs are faster.

### Verify

```powershell
docker exec movielens-app python scripts/verify_spark_etl.py
```

### Notes

- Spark processes **all messages currently in Kafka**. If you ran the producer twice, counts will be higher (duplicates).
- Run producer **before** Spark ETL: `run_producer.py` → `run_spark_etl.py`
- Monitor job progress: [http://localhost:8080](http://localhost:8080) (Spark master UI)

### Code locations


| File                          | Purpose                        |
| ----------------------------- | ------------------------------ |
| `src/spark/config.py`         | Env-based config for Spark job |
| `src/spark/etl_job.py`        | PySpark ETL pipeline           |
| `scripts/run_spark_etl.py`    | Host-side spark-submit wrapper |
| `scripts/verify_spark_etl.py` | Post-load ES verification      |


---

## Step 12 — Verify Elasticsearch indexes and queries (Stage 7)

Confirms all three indexes exist, mappings match the locked schema, data is loaded, and filters/sorts/aggregations work.

**Prerequisites:** Steps 5–11 complete (indexes created + ETL loaded data).

```powershell
docker exec movielens-app python scripts/verify_es_indexes.py

# Gold queries (Stage 8)
docker exec movielens-app python scripts/verify_gold_queries.py
docker exec movielens-app python scripts/verify_gold_queries.py --id movies_02 --show-hits 3
```

### What it checks (18 checks)

1. **Index existence** — all three indexes present
2. **Mappings** — field names and types match `src/elastic/schema.py`
3. **Document counts** — non-zero docs in each index
4. **Filters** — `genres`, `release_year`, `rating_year`, `tags` exact match
5. **Sorting** — `average_rating`, `movie_count`
6. **Aggregations** — genre averages, release-year cohorts, rating-year trends

### Expected output

```text
18/18 checks passed.
Elasticsearch index verification passed.
```

### Code locations

| File | Purpose |
|---|---|
| `src/elastic/verify_indexes.py` | Verification logic |
| `src/elastic/setup_indexes.py` | Index creation from locked mappings |
| `scripts/verify_es_indexes.py` | CLI entry point |

---

## Step 13 — Verify gold queries (Stage 8)

Runs all 16 manual reference queries from `tests/gold_queries/queries.json` against Elasticsearch.

**Prerequisites:** ETL data loaded (Steps 10–11).

```powershell
docker exec movielens-app python scripts/verify_gold_queries.py
```

### Run a single query

```powershell
docker exec movielens-app python scripts/verify_gold_queries.py --id movies_02 --show-hits 3
docker exec movielens-app python scripts/verify_gold_queries.py --id movies_07 --show-response
```

### Expected output

```text
16/16 queries passed.
Gold query verification passed.
```

### Files

| File | Purpose |
|---|---|
| `tests/gold_queries/catalog.yaml` | Question catalog |
| `tests/gold_queries/queries.json` | Verified DSL bodies |
| `src/elastic/gold_queries.py` | Query runner module |
| `scripts/verify_gold_queries.py` | CLI entry point |

See also `tests/gold_queries/README.md` for Kibana manual testing.

---

## Step 14 — Natural language queries (Stage 9)

Generate Elasticsearch DSL from plain English using Ollama (`llama3.2:3b`).

**Prerequisites:** Ollama model pulled (Step 4), ETL data loaded (Steps 10–11).

### Ask one question

```powershell
docker exec movielens-app python scripts/run_nl_query.py "Show Comedy movies released after 2000." --show-dsl
docker exec movielens-app python scripts/run_nl_query.py "What are the 10 highest-rated Comedy movies with at least 100 ratings?" --show-hits 3
```

### Evaluate against gold questions

```powershell
docker exec movielens-app python scripts/evaluate_ai_queries.py --id movies_01 --show-dsl
docker exec movielens-app python scripts/evaluate_ai_queries.py
```

Full evaluation runs all 16 gold questions (~2–5 minutes).

### Expected output (single question)

```text
Question: Show Comedy movies released after 2000.

Index: movies
Hits: 3,031  agg_buckets=0
```

### Files

| File | Purpose |
|---|---|
| `src/ai/prompt.py` | System prompt + schema |
| `src/ai/ollama_client.py` | Ollama HTTP client |
| `src/ai/parser.py` | JSON / DSL parser |
| `src/ai/generator.py` | Generate + execute queries |
| `scripts/run_nl_query.py` | CLI for ad-hoc questions |
| `scripts/evaluate_ai_queries.py` | Gold-question evaluation |

---

## Step 15 — Verify query validator (Stage 10)

Ensures gold queries pass validation and unsafe DSL is rejected before Elasticsearch execution.

```powershell
docker exec movielens-app python scripts/verify_query_validator.py
```

### Expected output

```text
23/23 validator checks passed.
Query validator verification passed.
```

### Files

| File | Purpose |
|---|---|
| `src/ai/validator.py` | Schema + safety validation |
| `scripts/verify_query_validator.py` | Gold pass + rejection-case tests |

The validator runs automatically in `generate_query()` and `execute_query()` before any ES call.

---

## Step 16 — Streamlit demo UI (Stage 11)

Open the natural-language search interface at **http://localhost:8501**.

**Prerequisites:** Stack running, Ollama model pulled, ETL data loaded (Steps 10–11).

```powershell
docker compose up -d --build app
```

Then open http://localhost:8501 in your browser.

### What the UI shows

1. Your question
2. Target Elasticsearch index
3. Validated generated DSL
4. Result table (hits) or aggregation JSON
5. Optional AI summary (sidebar checkbox; labeled as LLM-generated)

### Files

| File | Purpose |
|---|---|
| `src/app/streamlit_app.py` | Streamlit UI |
| `Dockerfile` | Starts Streamlit on port 8501 |

---

## Useful day-to-day commands

```bash
# Start stack (after first setup)
docker compose up -d

# Stop stack
docker compose down

# Fresh reset (deletes Kafka/ES/Ollama volumes)
docker compose down -v
docker compose up -d --build

# View logs
docker compose logs -f
docker compose logs movielens-elasticsearch
docker compose logs movielens-kafka

# Rebuild app after code changes
docker compose up -d --build app

# Re-run infrastructure setup
docker exec movielens-app python scripts/setup_infrastructure.py

# Run sample ETL (Stage 4)
docker exec movielens-app python scripts/run_sample_etl.py
docker exec movielens-app python scripts/verify_sample_etl.py

# Stream ratings to Kafka (Stage 5)
docker exec movielens-app python scripts/run_producer.py
docker exec movielens-app python scripts/verify_producer.py

# Spark ETL (Stage 6 — run from host)
python scripts/run_spark_etl.py
docker exec movielens-app python scripts/verify_spark_etl.py
docker exec movielens-app python scripts/verify_es_indexes.py

# Gold queries (Stage 8)
docker exec movielens-app python scripts/verify_gold_queries.py
docker exec movielens-app python scripts/verify_gold_queries.py --id movies_02 --show-hits 3

# Check ES index counts from host
curl http://localhost:9200/movies/_count
curl http://localhost:9200/movies_by_release_year/_count
curl http://localhost:9200/movie_ratings_by_rating_year/_count
```

---

## Troubleshooting


| Problem                                   | Cause                               | Fix                                                                             |
| ----------------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------- |
| `Cannot connect to Docker daemon`         | Docker Desktop not running          | Start Docker Desktop, retry                                                     |
| `docker compose up` hangs >5 min          | Waiting for healthchecks            | Run `docker compose ps`; wait for `(healthy)`                                   |
| `movielens-app` stuck at `created`        | Ollama not healthy yet              | Wait or run `docker compose up -d app` after ollama is healthy                  |
| `manifest not found` for Bitnami          | Old compose file                    | Use current `docker-compose.yml` (Apache images)                                |
| Ollama model missing                      | Step 4 not done                     | `docker exec movielens-ollama ollama pull llama3.2:3b`                          |
| Kafka topic missing                       | Step 5 not done                     | `docker exec movielens-app python scripts/setup_infrastructure.py`              |
| ES status `yellow`                        | Single-node cluster                 | Normal — not an error                                                           |
| Out of memory                             | Too many containers/apps            | Close other apps; Ollama needs 2–4 GB                                           |
| Wrong CSV filenames                       | Extracted with wrong names          | Rename to `ratings.csv`, `movies.csv`, `tags.csv`                               |
| `ModuleNotFoundError: pandas` in notebook | No local venv                       | Follow Step 7 Option A                                                          |
| `ModuleNotFoundError: pyarrow` in ETL     | App image not rebuilt after Stage 4 | `docker compose up -d --build app`                                              |
| `invalid literal for int() ... timestamp` | Old ETL code                        | Pull latest; timestamps may be datetime strings or Unix epochs — both supported |
| ES indexes empty after ETL                | Step 9 not run or failed            | Run `run_sample_etl.py` then `verify_sample_etl.py`                             |
| `FileNotFoundError` for ratings.csv       | Data not downloaded                 | Complete Step 2; check `data/raw/`                                              |
| ETL slow on first run                     | Reading 100k rows + tag aggregation | Normal (~30–60 s); use `--sample-size 10000` for quick tests                    |
| Kafka topic empty                         | Step 10 not run                     | `docker exec movielens-app python scripts/run_producer.py`                      |
| Duplicate Kafka messages                  | Producer re-run appends             | Expected; reset with `docker compose down -v` for clean topic                   |


---

## Project structure (current)

```text
bigData/
├── data/
│   ├── raw/              # MovieLens CSV files (not in Git)
│   └── processed/        # ETL outputs
├── docs/
│   ├── plan.md           # This file — full project plan + instructions
│   ├── schema.md         # Locked data model (Stage 2)
│   ├── infrastructure.md # Docker details (Stage 3)
│   └── data_quality_summary.md
├── notebooks/
│   └── 01_data_exploration.ipynb
├── scripts/
│   ├── verify_stack.py
│   ├── setup_infrastructure.py
│   ├── validate_schema.py
│   ├── run_sample_etl.py
│   ├── verify_sample_etl.py
│   ├── run_producer.py
│   └── verify_producer.py
├── src/
│   ├── etl/              # Sample ETL pipeline (Stage 4)
│   ├── producer/         # Kafka producer (Stage 5)
│   ├── spark/            # Spark ETL jobs (Stage 6)
│   ├── elastic/          # Schema + index setup
│   ├── ai/               # NL → Elasticsearch query (Stage 9)
│   ├── app/              # Streamlit demo (Stage 11)
│   └── config.py
├── tests/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## Quick reference — full setup sequence

Run once from the project root (bash):

```bash
cp .env.example .env
# download MovieLens 20M into data/raw/ (ratings.csv, movies.csv, tags.csv)
docker compose up -d --build
docker compose ps                                    # wait for (healthy)
docker exec movielens-ollama ollama pull llama3.2:3b
docker exec movielens-app python scripts/setup_infrastructure.py
docker exec movielens-app python scripts/verify_stack.py
docker exec movielens-app python scripts/run_sample_etl.py      # Stage 4
docker exec movielens-app python scripts/verify_sample_etl.py   # Stage 4
docker exec movielens-app python scripts/run_producer.py        # Stage 5
docker exec movielens-app python scripts/verify_producer.py     # Stage 5
python scripts/run_spark_etl.py                                 # Stage 6 (host)
docker exec movielens-app python scripts/verify_spark_etl.py    # Stage 6
docker exec movielens-app python scripts/verify_es_indexes.py   # Stage 7
docker exec movielens-app python scripts/verify_gold_queries.py # Stage 8
```

Windows PowerShell equivalent:

```powershell
copy .env.example .env
docker compose up -d --build
docker compose ps
docker exec movielens-ollama ollama pull llama3.2:3b
docker exec movielens-app python scripts/setup_infrastructure.py
docker exec movielens-app python scripts/verify_stack.py
docker exec movielens-app python scripts/run_sample_etl.py
docker exec movielens-app python scripts/verify_sample_etl.py
docker exec movielens-app python scripts/run_producer.py
docker exec movielens-app python scripts/verify_producer.py
python scripts/run_spark_etl.py
docker exec movielens-app python scripts/verify_spark_etl.py
docker exec movielens-app python scripts/verify_es_indexes.py

# Gold queries (Stage 8)
docker exec movielens-app python scripts/verify_gold_queries.py
```

When all steps pass, the full sample pipeline and gold queries are ready. **Next:** Stage 9 (AI).

---

## Command cheat sheet — copy/paste restore

Use this section to **restore every manual command** in one place. All commands are run from the project root (`bigData/`) in **Commander or PowerShell** on Windows unless noted.

### Where commands run


| You type on | Command pattern | Runs inside |
|---|---|---|
| Host terminal | `docker exec movielens-app ...` | `movielens-app` container |
| Host terminal | `docker exec movielens-ollama ...` | `ollama` container |
| Host terminal | `python scripts/run_spark_etl.py` | Host script → submits to `movielens-spark-worker` |
| Host terminal | `docker compose ...` | Docker Desktop (orchestrates containers) |
| Host terminal | `curl http://localhost:9200/...` | Your machine → Elasticsearch port |

You never need `docker exec -it ... bash` for normal operation.

---

### A — First-time setup (once per machine / after clone)

```powershell
cd C:\Users\Noa\Desktop\noa\projects\bigData
copy .env.example .env
# Download MovieLens 20M → data/raw/ (ratings.csv, movies.csv, tags.csv)
docker compose up -d --build
docker compose ps
docker exec movielens-ollama ollama pull llama3.2:3b
docker exec movielens-app python scripts/setup_infrastructure.py
docker exec movielens-app python scripts/verify_stack.py
docker exec movielens-app python scripts/validate_schema.py
```

---

### B — Recovery after `docker compose down -v` (wipes all data)

```powershell
docker compose up -d --build
docker compose ps
docker exec movielens-ollama ollama pull llama3.2:3b
docker exec movielens-app python scripts/setup_infrastructure.py
docker exec movielens-app python scripts/verify_stack.py
```

Then re-run the pipeline (section C or D below).

---

### C — Main pipeline: Kafka → Spark → Elasticsearch (Stages 5–6)

**Run in this order.** Do not press Ctrl+C during the producer.

```powershell
docker compose up -d
docker compose ps

# Stage 5 — producer → Kafka
docker exec movielens-app python scripts/run_producer.py
docker exec movielens-app python scripts/verify_producer.py

# Stage 6 — Spark → Elasticsearch (host Python wrapper)
python scripts/run_spark_etl.py
docker exec movielens-app python scripts/verify_spark_etl.py

# Stage 7 — verify mappings + queries
docker exec movielens-app python scripts/verify_es_indexes.py

# Stage 8 — gold queries
docker exec movielens-app python scripts/verify_gold_queries.py

# Gold queries (Stage 8)
docker exec movielens-app python scripts/verify_gold_queries.py
docker exec movielens-app python scripts/verify_gold_queries.py --id movies_02 --show-hits 3
```

If `python` is not found on host, try: `py scripts/run_spark_etl.py`

---

### D — Optional: Stage 4 pandas ETL (legacy / skip if using Spark)

Only needed if you want ES data **without** Kafka + Spark:

```powershell
docker exec movielens-app python scripts/run_sample_etl.py
docker exec movielens-app python scripts/verify_sample_etl.py
```

---

### E — Day-to-day stack control

```powershell
docker compose up -d
docker compose ps
docker compose down
docker compose down -v
docker compose up -d --build app
docker compose logs -f
docker compose logs movielens-kafka
```

---

### F — Verify / inspect

```powershell
docker exec movielens-app python scripts/verify_stack.py
docker exec movielens-app python scripts/verify_producer.py
docker exec movielens-app python scripts/verify_spark_etl.py
docker exec movielens-app python scripts/verify_es_indexes.py

# Gold queries (Stage 8)
docker exec movielens-app python scripts/verify_gold_queries.py
docker exec movielens-app python scripts/verify_gold_queries.py --id movies_02 --show-hits 3
docker exec movielens-ollama ollama list
curl http://localhost:9200/movies/_count
curl http://localhost:9200/movies_by_release_year/_count
curl http://localhost:9200/movie_ratings_by_rating_year/_count
```

Browser URLs: ES http://localhost:9200 · Kibana http://localhost:5601 · Spark UI http://localhost:8080

---

### G — Optional flags (quick tests)

```powershell
docker exec movielens-app python scripts/run_producer.py --sample-size 1000
docker exec movielens-app python scripts/run_producer.py --full
docker exec movielens-app python scripts/run_sample_etl.py --sample-size 50000
docker exec movielens-app python scripts/verify_producer.py --sample-messages 10
```

---

### H — Local notebook (Stage 1, optional)

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt ipykernel
.\.venv\Scripts\python -m ipykernel install --user --name=bigdata --display-name="Python (bigData)"
# Open notebooks/01_data_exploration.ipynb with kernel "Python (bigData)"
```

---

## Checklist — what you should have when done


| Check                  | How to verify                                                  |
| ---------------------- | -------------------------------------------------------------- |
| Docker stack running   | `docker compose ps` — 7 containers, key services `(healthy)`   |
| MovieLens data present | `data/raw/ratings.csv`, `movies.csv`, `tags.csv`               |
| Ollama model pulled    | `docker exec movielens-ollama ollama list` shows `llama3.2:3b` |
| Kafka topic exists     | `verify_stack.py` passes                                       |
| ES indexes exist       | `setup_infrastructure.py` created all three                    |
| Sample data loaded     | `verify_sample_etl.py` passes                                  |
| Ratings in Kafka       | `verify_producer.py` passes                                    |
| Spark ETL loaded ES    | `verify_spark_etl.py` passes                                   |
| ES indexes queryable   | `verify_es_indexes.py` passes (18/18 checks)                   |
| Gold queries verified  | `verify_gold_queries.py` passes (16/16 queries)                |
| Processed files saved  | `data/processed/sample_*.parquet` exist after Step 9           |


---

# Project Stages

## Stage 0 — Project Repository and Development Environment

**Current status:** complete (repo scaffold, Docker stack, config).

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

### How to run (completed)

Follow **Steps 1–3** in [Full Setup and Run Instructions](#full-setup-and-run-instructions) above.

### Parallel work

No. Do this once as a team.

---

## Stage 1 — Explore and Understand MovieLens

**Current status:** complete (exploration notebook + data quality summary).

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

### How to run (completed)

1. Follow **Step 7** in [Full Setup and Run Instructions](#full-setup-and-run-instructions).
2. Open `notebooks/01_data_exploration.ipynb` and run all cells.
3. Read findings in `docs/data_quality_summary.md`.

### Parallel work

Yes.

Suggested split (up to 3 members):

- **Member A:** `ratings.csv`
- **Member B:** `movies.csv`
- **Member C:** `tags.csv`

Then combine conclusions together.

---

## Stage 2 — Define the Final Data Model

**Current status:** complete (locked schema in `docs/schema.md` + `src/elastic/schema.py`).

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


| Field          | Meaning                                | Used in                            |
| -------------- | -------------------------------------- | ---------------------------------- |
| `release_year` | Year the movie was released            | `movies`, `movies_by_release_year` |
| `rating_year`  | Calendar year the rating was submitted | `movie_ratings_by_rating_year`     |


Never use a generic `year` field — always use `release_year` or `rating_year`.

### Decide together

- Exact field names and data types (locked as above).
- Fields that are searchable vs aggregation-only.
- Which Spark outputs map to which index.
- Which Natural Language questions the system must support.
- Spark → Elasticsearch direct-write configuration (index names, id fields).

### End result

A documented schema shared by Spark, Elasticsearch, and the AI layer.

### How to run (completed)

1. Follow **Step 8** in [Full Setup and Run Instructions](#full-setup-and-run-instructions).

### Parallel work

No. This is a synchronization point for the whole team.

---

## Stage 3 — Docker and Infrastructure

**Current status:** complete (healthchecks, setup scripts, infrastructure docs).

All components run in Docker on a single laptop. Minimize container count and memory usage.

### Target stack (~5–6 containers)


| Service           | Image / notes                                         | Memory hint              |
| ----------------- | ----------------------------------------------------- | ------------------------ |
| **kafka**         | Apache Kafka, KRaft mode (no Zookeeper)               | ~512 MB                  |
| **elasticsearch** | Single-node, `discovery.type=single-node`             | 512 MB–1 GB heap         |
| **kibana**        | Matches ES version                                    | ~512 MB                  |
| **spark**         | Apache Spark — 1 master + 1 worker                    | 1–2 GB                   |
| **ollama**        | Official Ollama image                                 | 2–4 GB (model-dependent) |
| **app**           | Python — producer, Streamlit UI, AI client, validator | ~512 MB                  |


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

- Create `docker-compose.yml` with all services above. ✅
- Configure shared Docker network and volume mounts. ✅
- Set memory limits where possible (`ES_JAVA_OPTS`, Spark worker memory). ✅
- Start all services and verify connectivity. ✅
- Create Kafka topic: `raw_ratings`. ✅
- Verify Elasticsearch health (`/_cluster/health`). ✅
- Verify Kibana connects to Elasticsearch. ✅
- Pull Ollama model: `docker exec movielens-ollama ollama pull llama3.2:3b`. ✅
- Document startup order and expected ports. ✅

### How to run (completed)

Follow **Steps 3–6** in [Full Setup and Run Instructions](#full-setup-and-run-instructions) above.

Key scripts:

- `scripts/setup_infrastructure.py` — create Kafka topic + ES indexes
- `scripts/verify_stack.py` — health check all services
- `scripts/validate_schema.py` — validate locked schema module

See also `docs/infrastructure.md` for Docker details and troubleshooting.

### End result

`docker compose up` starts the full local stack on one laptop.

### Parallel work

Yes. Can be done in parallel with Stage 4 after the schema is agreed.

---

## Stage 4 — Build a Small ETL Prototype

**Current status:** complete.

Do **not** start immediately with all 20 million ratings.

Use a sample, for example:

```text
100,000 ratings
```

### Tasks

- Load a small sample. ✅
- Validate rating values. ✅
- Convert timestamps and derive `rating_year`. ✅
- Join ratings with movies. ✅
- Parse genres and release year from title. ✅
- Aggregate ratings by movie. ✅
- Aggregate tags. ✅
- Calculate movie-level: `average_rating`, `rating_count`, `tag_count`. ✅
- Calculate release-year cohort stats for `movies_by_release_year`. ✅
- Calculate `(movie_id, rating_year)` stats for `movie_ratings_by_rating_year`. ✅
- Write sample output directly to Elasticsearch. ✅

### How to run (completed)

1. Follow **Step 9** in [Full Setup and Run Instructions](#full-setup-and-run-instructions).

Also see the detailed Stage 4 section below for options and code locations.

### End result

A small processed dataset loaded into all three Elasticsearch indexes, matching the Stage 2 schema.

### Parallel work

Yes. Can be done in parallel with Stage 3.

---

## Stage 5 — Kafka Producer

**Current status:** complete.

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
  "timestamp": "2005-04-02 23:53:47"
}
```

(`timestamp` may also be a Unix epoch integer depending on the CSV format.)

### Tasks

- Build Python Kafka producer in `src/producer/`. ✅
- Read `ratings.csv` from the mounted volume. ✅
- Convert each row to JSON. ✅
- Send records to `raw_ratings`. ✅
- Add configurable **sample / full** modes via environment variable. ✅
- Verify messages can be consumed. ✅

### How to run (completed)

Follow **Step 10** in [Full Setup and Run Instructions](#full-setup-and-run-instructions).

### End result

Real MovieLens rating events are entering Kafka.

### Parallel work

Yes. Can overlap with Stage 6 once the schema and Kafka contract are fixed.

---

## Stage 6 — Spark ETL Pipeline

**Current status:** complete.

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

- Configure Spark Kafka source. ✅
- Parse Kafka JSON messages. ✅
- Validate and clean fields. ✅
- Load static movie/tag data from volume mount. ✅
- Perform joins. ✅
- Parse genres and extract `release_year` from titles. ✅
- Normalize tags (lowercase, deduplicate). ✅
- Compute all three aggregation outputs. ✅
- Write each output directly to its Elasticsearch index. ✅
- Handle missing/invalid records. ✅
- Support sample/full mode aligned with the Kafka producer. ✅

### How to run (completed)

Follow **Step 11** in [Full Setup and Run Instructions](#full-setup-and-run-instructions).

Pipeline order: `run_producer.py` → `run_spark_etl.py` → `verify_spark_etl.py` → `verify_es_indexes.py`

### End result

The full ETL pipeline produces clean, aggregated data written directly into Elasticsearch.

### Parallel work

Partially. Kafka producer and Spark ETL can be developed by different members after their interface is agreed.

---

## Stage 7 — Elasticsearch Indexes and Mappings

**Current status:** complete.

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

- Define mappings for all three indexes. ✅
- Create indexes (via `src/elastic/setup_indexes.py`). ✅
- Load sample data from ETL pipeline (Stage 4 / Stage 6). ✅
- Verify exact-match filters (`genres`, `tags`, `release_year`, `rating_year`). ✅
- Verify numeric ranges and sorting. ✅
- Verify aggregations (genre averages, release-year cohorts, rating-year trends). ✅
- Verify tag searching. ✅

### How to run (completed)

Follow **Step 12** in [Full Setup and Run Instructions](#full-setup-and-run-instructions).

```powershell
docker exec movielens-app python scripts/verify_es_indexes.py

# Gold queries (Stage 8)
docker exec movielens-app python scripts/verify_gold_queries.py
docker exec movielens-app python scripts/verify_gold_queries.py --id movies_02 --show-hits 3
```

### End result

Elasticsearch contains queryable MovieLens data across all three indexes.

### Parallel work

Yes. Mappings can be developed using Stage 4 sample output while Spark is being completed.

---

## Stage 8 — Build Manual Elasticsearch Queries

**Current status:** complete.

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

- Define 15–20 target natural-language questions covering all categories above. ✅ (16 queries)
- Write the correct Elasticsearch DSL manually for each. ✅
- Specify which index each query targets. ✅
- Verify each query against Elasticsearch. ✅
- Save cases in `tests/gold_queries/` for later evaluation. ✅

### How to run (completed)

Follow **Step 13** in [Full Setup and Run Instructions](#full-setup-and-run-instructions).

```powershell
docker exec movielens-app python scripts/verify_gold_queries.py
```

### End result

A tested query set independent of the LLM.

### Parallel work

Yes. Can be performed while Spark and Elasticsearch integration are being completed.

---

## Stage 9 — AI: Natural Language → Elasticsearch Query

**Current status:** complete.

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

- Integrate Ollama client in `src/ai/`. ✅
- Define system prompt with full schema and field semantics. ✅
- Restrict output to JSON/DSL only. ✅
- Parse model output (strip markdown fences if present). ✅
- Handle malformed output gracefully. ✅
- Test against the Stage 8 gold query set. ✅

### How to run (completed)

Follow **Step 14** in [Full Setup and Run Instructions](#full-setup-and-run-instructions).

```powershell
docker exec movielens-app python scripts/run_nl_query.py "Show Comedy movies released after 2000." --show-dsl
docker exec movielens-app python scripts/evaluate_ai_queries.py
```

### End result

Natural-language questions reliably generate Elasticsearch queries against the correct index.

### Parallel work

Yes. Prompt design and gold queries can begin before the full dataset pipeline is finished.

---

## Stage 10 — Query Validator

**Current status:** complete.

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

- Validate JSON syntax. ✅ (parser + validator)
- Allow only search/query operations (`query`, `aggs`, `sort`, `size`, `_source`). ✅
- Reject update/delete/index-management operations. ✅
- Validate field names against the schema for the target index. ✅
- Reject queries using `year` — require `release_year` or `rating_year`. ✅
- Apply result-size limits (e.g. `size` ≤ 100). ✅
- Return useful errors to the UI. ✅
- Log rejected queries for testing. ✅

### How to run (completed)

Follow **Step 15** in [Full Setup and Run Instructions](#full-setup-and-run-instructions).

```powershell
docker exec movielens-app python scripts/verify_query_validator.py
```

### End result

Only safe, valid search queries reach Elasticsearch.

### Parallel work

Yes. Can be developed in parallel with Stage 9.

---

## Stage 11 — Demo Application

**Current status:** complete.

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

- Build Streamlit UI in `src/app/`. ✅
- Connect to Ollama and Elasticsearch over the Docker network. ✅
- Show generated DSL before execution. ✅
- Execute validated query. ✅
- Display results clearly (table for hits, JSON for aggregations). ✅
- Handle errors (LLM failure, invalid query, ES timeout). ✅

### How to run (completed)

Follow **Step 16** in [Full Setup and Run Instructions](#full-setup-and-run-instructions).

```powershell
docker compose up -d --build app
```

Open http://localhost:8501

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


| Question                           | Index                        | Valid query? | Semantically correct? | Correct result? |
| ---------------------------------- | ---------------------------- | ------------ | --------------------- | --------------- |
| Top Comedy movies                  | movies                       | ✅            | ✅                     | ✅               |
| Movies tagged dark                 | movies                       | ✅            | ✅                     | ✅               |
| Genre avg rating                   | movies                       | ✅            | ✅                     | ✅               |
| Popular by rating activity in 2010 | movie_ratings_by_rating_year | ✅            | ✅                     | ✅               |
| Best movies released in 2010       | movies                       | ✅            | ✅                     | ✅               |
| ...                                | ...                          | ...          | ...                   | ...             |


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


| Member         | Primary Responsibility              | Secondary Responsibility |
| -------------- | ----------------------------------- | ------------------------ |
| **A — Data**   | Kafka + Spark ETL + direct ES write | Data exploration         |
| **B — Search** | Elasticsearch mappings + Kibana     | Manual/reference queries |
| **C — AI/App** | Ollama + Validator + Streamlit UI   | AI evaluation            |


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