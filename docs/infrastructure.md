# Infrastructure Guide (Stage 3)

Local Docker stack for MovieLens Smart Analytics. All services run on a single laptop via `docker compose`.

---

## Stack overview

| Container | Image | Host port | Memory limit |
|---|---|---:|---|
| `movielens-kafka` | `apache/kafka:3.7.0` | 9092 | 768 MB |
| `movielens-elasticsearch` | ES 8.15.0 | 9200 | 1 GB |
| `movielens-kibana` | Kibana 8.15.0 | 5601 | 768 MB |
| `movielens-spark` | Apache Spark 3.5.9 | 7077, 8080 | 768 MB |
| `movielens-spark-worker` | Apache Spark 3.5.9 | — | 1.5 GB |
| `movielens-ollama` | Ollama latest | 11434 | 4 GB |
| `movielens-app` | Python 3.11 (built) | 8501 | 512 MB |

**Note:** Bitnami Kafka/Spark images are unavailable on Docker Hub; this project uses official Apache images.

---

## Startup order

Docker Compose handles dependencies via healthchecks:

```text
1. elasticsearch  (waits until cluster health OK)
2. kafka          (KRaft broker ready)
3. ollama         (API responding)
4. kibana         (after elasticsearch healthy)
5. spark          (master UI up)
6. spark-worker   (after spark healthy)
7. app            (after kafka + elasticsearch + ollama healthy)
```

First boot may take **2–5 minutes** while images pull and Elasticsearch/Kibana initialize.

---

## Quick start

```bash
cp .env.example .env
docker compose up -d --build
docker exec movielens-ollama ollama pull llama3.2:3b
docker exec movielens-app python scripts/setup_infrastructure.py
docker exec movielens-app python scripts/verify_stack.py
```

`setup_infrastructure.py` creates:
- Kafka topic `raw_ratings`
- Elasticsearch indexes (`movies`, `movies_by_release_year`, `movie_ratings_by_rating_year`)

---

## Verification checklist

| Check | Command / URL |
|---|---|
| All containers running | `docker compose ps` |
| Elasticsearch | http://localhost:9200/_cluster/health |
| Kibana | http://localhost:5601 |
| Spark master UI | http://localhost:8080 |
| Ollama | http://localhost:11434/api/tags |
| Full stack (from app) | `docker exec movielens-app python scripts/verify_stack.py` |

---

## Volume mounts

| Host path | Container path | Purpose |
|---|---|---|
| `./data/raw` | `/data/raw` | MovieLens CSV files (read-only in app/spark) |
| `./data/processed` | `/data/processed` | ETL outputs |
| `./src` | `/app/src` | Live Python code in app container |
| `./src/spark` | `/opt/spark-jobs` | Spark job scripts |

Named volumes: `kafka_data`, `es_data`, `ollama_data` (persist Kafka, ES, and model data).

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Docker daemon not running | Start Docker Desktop |
| `manifest not found` for Bitnami | Use current `docker-compose.yml` (Apache images) |
| Ollama model missing | `docker exec movielens-ollama ollama pull llama3.2:3b` |
| Kafka topic missing | `docker exec movielens-app python scripts/setup_infrastructure.py` |
| ES `yellow` status | Normal for single-node cluster |
| Out of memory | Stop other apps; Ollama needs ~2–4 GB when model loaded |

Fresh reset (deletes data volumes):

```bash
docker compose down -v
docker compose up -d --build
```

---

## Scripts

| Script | Purpose |
|---|---|
| `scripts/verify_stack.py` | Health check for all services |
| `scripts/setup_infrastructure.py` | Create Kafka topic + ES indexes |
| `scripts/validate_schema.py` | Validate locked schema module |
