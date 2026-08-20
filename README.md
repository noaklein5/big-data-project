# MovieLens Smart Analytics

Big Data pipeline over **MovieLens 20M** with Kafka, Spark, Elasticsearch, and natural-language search via Ollama.

**Full setup guide:** [`docs/plan.md`](docs/plan.md) — step-by-step instructions, troubleshooting, and stage-by-stage plan.

## Status

| Stage | Description | Status |
|---|---|---|
| 0 | Repo, Docker stack, config | ✅ |
| 1 | Data exploration | ✅ |
| 2 | Locked data model | ✅ |
| 3 | Docker infrastructure | ✅ |
| 4 | Sample ETL (100k ratings → ES) | ✅ |
| 5 | Kafka producer | ✅ |
| 6 | Spark ETL (Kafka → ES) | ✅ |
| 7 | ES indexes + query verification | ✅ |
| 8 | Gold queries (16 reference DSL) | ✅ |
| 9 | NL → ES query (Ollama) | ✅ |
| 10 | Query validator | ✅ |
| 11 | Streamlit demo UI | ✅ |
| 12 | Kibana dashboards | ✅ |
| 13+ | Full 20M, evaluation | ⬜ **Next** |

## Prerequisites

- Docker Desktop (must be **running** before any `docker` command)
- ~8–12 GB free RAM
- Git
- Python 3.11+ (optional — for local notebooks only)

## Quick start

### 1. Clone and configure

```bash
git clone <repository-url>
cd bigData
cp .env.example .env
```

Windows PowerShell:

```powershell
git clone <repository-url>
cd bigData
copy .env.example .env
```

### 2. Download MovieLens 20M

Download from [MovieLens 20M](https://grouplens.org/datasets/movielens/20m/) and extract into `data/raw/` with **exact filenames**:

```text
data/raw/
  ratings.csv    ← not rating.csv
  movies.csv     ← not movie.csv
  tags.csv       ← not tag.csv
```

These files are **not** committed to Git.

### 3. Start the stack

```bash
docker compose up -d --build
docker compose ps
```

Wait until key containers show `(healthy)`. First start can take 2–5 minutes.

| Service | URL | Purpose |
|---|---|---|
| Elasticsearch | http://localhost:9200 | Search and analytics store |
| Kibana | http://localhost:5601 | Dashboards |
| Spark master UI | http://localhost:8080 | Spark cluster |
| Ollama | http://localhost:11434 | Local LLM |
| Kafka | `localhost:9092` | Rating event stream |
| App | http://localhost:8501 | Demo UI (Stage 11) |

Services use **Apache** official images for Kafka and Spark.

### 4. Pull the Ollama model

```bash
docker exec movielens-ollama ollama pull llama3.2:3b
```

### 5. Initialize infrastructure

Creates Kafka topic `raw_ratings` and Elasticsearch indexes:

```bash
docker exec movielens-app python scripts/setup_infrastructure.py
```

### 6. Verify connectivity

```bash
docker exec movielens-app python scripts/verify_stack.py
```

### 7. Run sample ETL (Stage 4)

Loads 100,000 ratings into all three Elasticsearch indexes:

```bash
docker exec movielens-app python scripts/run_sample_etl.py
docker exec movielens-app python scripts/verify_sample_etl.py
```

Optional: validate schema module

```bash
docker exec movielens-app python scripts/validate_schema.py
```

When all steps pass, you should have ~8k movies in Elasticsearch and 100k rating messages in Kafka (exact counts depend on sample settings).

### 8. Stream ratings to Kafka (Stage 5)

```bash
docker exec movielens-app python scripts/run_producer.py
docker exec movielens-app python scripts/verify_producer.py
```

Use `--full` to send all 20M ratings (slow). Re-running appends duplicate messages.

### 9. Run Spark ETL (Stage 6)

**Run from project root on your host** (uses `docker exec` internally):

```powershell
python scripts/run_spark_etl.py
docker exec movielens-app python scripts/verify_spark_etl.py
```

**Order matters:** producer first (step 8), then Spark ETL. First Spark run downloads JARs (~1–2 min).

### 10. Verify ES indexes (Stage 7)

```powershell
docker exec movielens-app python scripts/verify_es_indexes.py
```

Expected: `18/18 checks passed` — confirms mappings, filters, sorts, and aggregations work.

### 11. Verify gold queries (Stage 8)

```powershell
docker exec movielens-app python scripts/verify_gold_queries.py
docker exec movielens-app python scripts/verify_gold_queries.py --id movies_02 --show-hits 3
```

Expected: `16/16 queries passed`. See `tests/gold_queries/README.md` for Kibana manual testing.

### 12. Natural language queries (Stage 9)

```powershell
docker exec movielens-app python scripts/run_nl_query.py "Show Comedy movies released after 2000." --show-dsl
docker exec movielens-app python scripts/evaluate_ai_queries.py --id movies_01 --show-dsl
docker exec movielens-app python scripts/evaluate_ai_queries.py
```

The evaluator runs all 16 gold questions through Ollama (~2–5 min). Requires Ollama model from step 4 and ETL data loaded.

### 13. Verify query validator (Stage 10)

```powershell
docker exec movielens-app python scripts/verify_query_validator.py
```

Expected: `23/23 validator checks passed` — gold queries pass; unsafe DSL is rejected.

### 14. Demo UI (Stage 11)

Open **http://localhost:8501** after the stack is up and ETL data is loaded.

```powershell
docker compose up -d --build app
```

Type a question (or pick an example from the sidebar) and click **Search**. The app shows the validated Elasticsearch query and results.

### 15. Kibana dashboards (Stage 12)

```powershell
docker exec movielens-app python scripts/setup_kibana.py
docker exec movielens-app python scripts/verify_kibana.py
```

Open http://localhost:5601 → **Dashboards** → **MovieLens Analytics**. See `kibana/insights.md` for data observations.

See [`docs/plan.md`](docs/plan.md) for expected output, flags, and troubleshooting.

## Project structure

```text
bigData/
├── data/
│   ├── raw/              # MovieLens CSV files (not in Git)
│   └── processed/        # ETL parquet outputs (sample_*.parquet)
├── docs/
│   ├── plan.md                  # Full setup guide + project plan
│   ├── schema.md                # Locked data model (Stage 2)
│   ├── infrastructure.md        # Docker stack details (Stage 3)
│   └── data_quality_summary.md  # Stage 1 findings
├── notebooks/
│   └── 01_data_exploration.ipynb
├── scripts/
│   ├── setup_infrastructure.py  # Kafka topic + ES indexes
│   ├── verify_stack.py          # Health check all services
│   ├── validate_schema.py       # Schema consistency check
│   ├── run_sample_etl.py        # Stage 4 ETL entry point
│   ├── verify_sample_etl.py     # Post-load verification
│   ├── run_producer.py          # Stage 5 Kafka producer
│   ├── verify_producer.py       # Kafka message verification
│   ├── run_spark_etl.py         # Stage 6 Spark submit (host)
│   ├── verify_spark_etl.py      # Spark ETL doc-count check
│   ├── verify_es_indexes.py     # Stage 7 mapping + query checks
│   ├── verify_gold_queries.py   # Stage 8 gold query runner
│   ├── run_nl_query.py          # Stage 9 NL → ES query
│   ├── evaluate_ai_queries.py   # Stage 9 AI evaluation
│   ├── verify_query_validator.py # Stage 10 validator checks
│   ├── setup_kibana.py          # Stage 12 Kibana dashboard setup
│   └── verify_kibana.py         # Stage 12 Kibana verification
├── kibana/
│   ├── README.md                # Stage 12 dashboard guide
│   └── insights.md              # Generated data observations
├── tests/
│   └── gold_queries/            # Stage 8 reference queries
│       ├── catalog.yaml
│       ├── queries.json
│       └── README.md
├── src/
│   ├── etl/              # Sample ETL pipeline (Stage 4)
│   ├── producer/         # Kafka producer (Stage 5) ✅
│   ├── spark/            # Spark ETL jobs (Stage 6) ✅
│   ├── elastic/          # Schema + index setup
│   ├── ai/               # NL → Elasticsearch query (Stage 9)
│   ├── app/              # Streamlit demo (Stage 11)
│   │   └── streamlit_app.py
│   ├── kibana/           # Kibana dashboard setup (Stage 12)
│   └── config.py         # Shared configuration
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

## Configuration

Copy `.env.example` to `.env`. Use **Docker internal hostnames** (`kafka`, `elasticsearch`, etc.) — do not replace with `localhost`.

| Variable | Default | Description |
|---|---|---|
| `DATA_MODE` | `sample` | `sample` or `full` |
| `SAMPLE_RATINGS` | `100000` | Ratings to process in sample mode |
| `OLLAMA_MODEL` | `llama3.2:3b` | Ollama model for query generation |
| `KAFKA_TOPIC_RAW_RATINGS` | `raw_ratings` | Ratings stream topic |

## Elasticsearch indexes

| Index | Purpose | Document ID |
|---|---|---|
| `movies` | All-time stats per movie | `movie_id` |
| `movies_by_release_year` | Release-year cohort rollups | `release_year` |
| `movie_ratings_by_rating_year` | Rating activity by calendar year | `{movie_id}_{rating_year}` |

Key field rule: use `release_year` (when the movie came out) vs `rating_year` (when the rating was submitted). Never use generic `year`.

See [`docs/schema.md`](docs/schema.md) for the full locked schema.

## Useful commands

```bash
# Start / stop stack
docker compose up -d
docker compose down
docker compose down -v          # fresh reset (deletes volumes)

# Rebuild app after requirements.txt changes
docker compose up -d --build app

# View logs
docker compose logs -f

# Re-run sample ETL
docker exec movielens-app python scripts/run_sample_etl.py
docker exec movielens-app python scripts/verify_sample_etl.py

# Stream ratings to Kafka
docker exec movielens-app python scripts/run_producer.py
docker exec movielens-app python scripts/verify_producer.py

# Spark ETL (from project root on host)
python scripts/run_spark_etl.py
docker exec movielens-app python scripts/verify_spark_etl.py
docker exec movielens-app python scripts/verify_es_indexes.py

# Check ES document counts
curl http://localhost:9200/movies/_count
curl http://localhost:9200/movies_by_release_year/_count
curl http://localhost:9200/movie_ratings_by_rating_year/_count
```

## Local notebook (optional)

For Stage 1 data exploration outside Docker:

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt ipykernel
.\.venv\Scripts\python -m ipykernel install --user --name=bigdata --display-name="Python (bigData)"
```

Open `notebooks/01_data_exploration.ipynb` with kernel **Python (bigData)**.

## Team

Up to 3 members. Suggested split: Data (Kafka/Spark) · Search (ES/Kibana) · AI/App (Ollama/UI).

## License

Academic course project. MovieLens dataset © GroupLens Research.
