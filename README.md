# dual-mode-order-pipeline

An end-to-end data engineering project that processes e-commerce orders through two pipeline modes — batch and streaming — sharing one validated cleaning layer so the numbers never silently drift apart.

The batch path runs daily via Airflow: extract raw CSV → clean → quality gate → load to Postgres → summarize. The streaming path runs continuously: a Kafka producer replays cleaned orders as live events, Spark Structured Streaming aggregates revenue per category in 1-minute tumbling windows, and writes results back to Postgres. Both paths use the same `clean.py` — you never want your live numbers and your daily report to disagree because two different engineers wrote two different cleaning script.

## Architecture

```
                    raw_orders_messy.csv
                            │
            ┌───────────────┴───────────────┐
            │                               │
      BATCH (daily)                 STREAMING (live)
            │                               │
    extract.py                      producer.py
            │                     (reuses clean.py)
      clean.py                              │
            │                      Kafka: orders_clean
    quality_check                              │
            │                  spark_streaming_job.py
      load.py                         (1-min windows)
            │                               │
    orders_clean               live_category_revenue
            │                               │
    daily_orders_summary          Postgres warehouse
```

## Tech stack

| Component | What | Version |
|---|---|---|
| Python | ETL, producer, DAG tasks | 3.11 |
| Pandas | Data cleaning and transformation | 2.2.2 |
| Apache Kafka | Event streaming (KRaft mode) | 3.7.0 |
| Apache Spark | Structured Streaming consumer | 3.5.1 |
| Apache Airflow | Batch orchestration | 2.9.3 |
| PostgreSQL | Data warehouse | 15 |
| Docker | Containerization | Compose v3.8 |

## Prerequisites

- Docker and Docker Compose (v2+)
- That's it — everything else runs inside containers

## Setup

```bash
# clone the repo
git clone https://github.com/mkhanumer/dual-mode-order-pipeline.git
cd dual-mode-order-pipeline

# copy the example env file
cp .env.example .env

# build all containers
docker compose build

# start Postgres and Kafka first
docker compose up -d postgres kafka

# initialize Airflow (creates admin user, only needed once)
docker compose up airflow-init

# start everything else
docker compose up -d airflow-webserver airflow-scheduler producer spark_streaming
```

## Running

**Batch pipeline (manual run):**

```bash
docker compose run --rm etl
```

**Batch pipeline (via Airflow):**

Open http://localhost:8080 (admin / admin), find `daily_batch_pipeline`, click the trigger button. It also runs automatically at 2 AM.

**Streaming pipeline:**

Already running after `docker compose up`. Check producer logs:

```bash
docker compose logs -f producer
```

**Query results:**

```bash
docker exec -it postgres psql -U de_user -d warehouse

-- cleaned batch orders
SELECT * FROM orders_clean LIMIT 10;

-- live streaming aggregates
SELECT * FROM live_category_revenue ORDER BY window_start DESC LIMIT 10;

-- daily summaries
SELECT * FROM daily_orders_summary ORDER BY summary_date DESC LIMIT 10;
```

**Tear down:**

```bash
docker compose down -v
```

## Project structure

```
├── docker-compose.yml          # wires all services together
├── .env.example                # copy to .env before running
├── data/
│   └── raw_orders_messy.csv    # intentionally messy sample data
├── etl/                        # shared batch+streaming cleaning logic
│   ├── extract.py              # reads raw data from CSV
│   ├── clean.py                # 7-step cleaning pipeline
│   ├── load.py                 # writes to Postgres
│   ├── requirements.txt
│   └── Dockerfile
├── streaming/
│   ├── producer/               # Kafka producer, reuses etl/clean.py
│   │   ├── producer.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── spark_jobs/
│       └── spark_streaming_job.py  # Kafka → Spark → Postgres
├── airflow/
│   ├── docker/Dockerfile
│   └── dags/
│       └── daily_batch_pipeline_dag.py
└── postgres/
    ├── 01-init-multi-db.sh     # creates the airflow metadata DB
    └── 02-create-tables.sql    # creates warehouse tables
```

## Known limitations

- **Simulated streaming**: the producer replays historical CSV rows as fake live events. A real system would read from an actual order source (API, CDC, message queue).
- **Single-node everything**: Kafka, Spark, and Postgres all run as single containers. This is a learning project, not a production deployment.
- **No tests**: the quality gate in the Airflow DAG is the only automated validation. Unit tests for `clean.py` would be a natural next step.
- **Hardcoded Spark packages**: the `--packages` flag in docker-compose downloads JARs from Maven Central on first run, which can be slow on poor connections.

## Cleanup

After cleaning the intentionally messy CSV, the pipeline typically reports:

```
[extract] Read 263 raw rows
[clean] Dropped ~76 rows with missing critical fields
[clean] Removed ~1 duplicate rows
[clean] Removed ~41 rows that broke business rules
```

Exact counts depend on the data. The point is that every row removal is logged and traceable.
