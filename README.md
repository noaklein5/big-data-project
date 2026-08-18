# MovieLens Smart Analytics

Big Data pipeline over **MovieLens 20M** with Kafka, Spark, Elasticsearch, and natural-language search via Ollama.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- ~8–12 GB free RAM on your laptop
- Git

## Quick start

### 1. Clone and configure

```bash
git clone <repository-url>
cd bigData
cp .env.example .env
```

### 2. Download MovieLens 20M

Download from [MovieLens 20M](https://grouplens.org/datasets/movielens/20m/) and extract into `data/raw/`:

```text
data/raw/
  ratings.csv
  movies.csv
  tags.csv
```

These files are **not** committed to Git.

### 3. Start the stack

```bash
docker compose up -d --build
```

Services use **Apache** official images for Kafka and Spark (Bitnami images are no longer available on Docker Hub).

| Service | URL | Purpose |
|---|---|---|
| Kafka | `localhost:9092` | Rating event stream |
| Elasticsearch | `http://localhost:9200` | Search and analytics store |
| Kibana | `http://localhost:5601` | Dashboards |
| Spark master UI | `http://localhost:8080` | Spark cluster |
| Ollama | `http://localhost:11434` | Local LLM |
| App | `localhost:8501` | Demo UI (Streamlit — coming in Stage 11) |

### 4. Pull the Ollama model

```bash
docker exec movielens-ollama ollama pull llama3.2:3b
```

### 5. Verify connectivity

```bash
docker exec movielens-app python scripts/verify_stack.py
```

### 6. Create Elasticsearch indexes

```bash
docker exec movielens-app python -m src.elastic.setup_indexes
```

## Project structure

```text
bigData/
├── data/
│   ├── raw/              # MovieLens CSV files (not in Git)
│   └── processed/        # Intermediate outputs
├── docs/
│   └── plan.md           # Full project plan
├── notebooks/
│   └── 01_data_exploration.ipynb
├── scripts/
│   └── verify_stack.py   # Health check for core services
├── src/
│   ├── producer/         # Kafka producer
│   ├── spark/            # Spark ETL jobs
│   ├── elastic/          # Index setup and ES utilities
│   ├── ai/               # NL → Elasticsearch query
│   ├── app/              # Streamlit demo
│   └── config.py         # Shared configuration
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

## Configuration

Copy `.env.example` to `.env` and adjust if needed:

| Variable | Default | Description |
|---|---|---|
| `DATA_MODE` | `sample` | `sample` or `full` |
| `SAMPLE_RATINGS` | `100000` | Ratings to process in sample mode |
| `OLLAMA_MODEL` | `llama3.2:3b` | Ollama model for query generation |

## Useful commands

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop all services
docker compose down

# Stop and remove volumes (fresh start)
docker compose down -v

# Rebuild app container after code changes
docker compose up -d --build app
```

## Elasticsearch indexes

| Index | Purpose |
|---|---|
| `movies` | All-time stats per movie |
| `movies_by_release_year` | Release-year cohort rollups |
| `movie_ratings_by_rating_year` | Rating activity by calendar year |

See `docs/plan.md` for full schema and field semantics (`release_year` vs `rating_year`).

## Development stages

This repo follows the staged plan in `docs/plan.md`:

- **Stage 0** — repo, Docker stack, config ✅
- **Stage 1** — data exploration notebook ✅
- **Stage 2** — lock schema (team sync)
- **Stage 3+** — ETL, AI, demo

## Team

Up to 3 members. Suggested split: Data (Kafka/Spark) · Search (ES/Kibana) · AI/App (Ollama/UI).

## License

Academic course project. MovieLens dataset © GroupLens Research.
