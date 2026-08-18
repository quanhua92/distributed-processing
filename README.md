# Distributed Processing Engine

A demo-grade, high-throughput distributed task processing platform for **arbitrary async jobs** built on **NATS JetStream**, **PostgreSQL 18**, **RustFS** (S3-compatible storage), **OpenObserve**, and **Grafana**.

Submit an arbitrary job payload via the API → it's recorded in PostgreSQL as `PENDING` → published to NATS JetStream → a worker pulls it, executes the registered processor plugin (e.g., Image Blur, Grayscale, Resize, or Statistical Data Aggregation) → stores output artifacts in RustFS S3 → updates PostgreSQL state → streams live SSE events to the dashboard → decoupled audit consumer batches logs to S3.

---

## Quickstart

```bash
docker compose up -d --wait
```

Open:

| URL | What |
|---|---|
| http://localhost:8000/ | Live SSE interactive dashboard (built-in) |
| http://localhost:3000/ | Grafana (auto-provisioned "Distributed Processing Overview" dashboard) |
| http://localhost:5080/ | OpenObserve (raw traces + metrics) — login `admin@local.dev` / `Admin123!@#` |
| http://localhost:9001/ | RustFS S3 console — login `rustfsadmin` / `rustfsadmin123` |
| http://localhost:8222/ | NATS JetStream monitoring |

> [!NOTE]
> When running with `docker-compose.local.yml` to avoid host port collisions, the ports are remapped to:
> - **API & Dashboard**: `http://localhost:18000`
> - **Grafana**: `http://localhost:13000`
> - **OpenObserve**: `http://localhost:15080`
> - **RustFS S3 Console**: `http://localhost:19001`
> - **PostgreSQL 18**: `localhost:15434`
> - **NATS**: `localhost:14222` / `18222`

### Submit a Task via cURL

**1. Data Aggregation & Hash Task:**
```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_type": "data:transform",
    "payload": {
      "operation": "aggregate",
      "data": [10.5, 42.0, 99.1, 150.0, 3.14, 88.0]
    }
  }'
# → {"job_id":"...","job_type":"data:transform","status":"PENDING", ...}
```

**2. Image Blur Task (Pillow):**
```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_type": "image:blur",
    "payload": {
      "source_url": "https://picsum.photos/1200/800",
      "radius": 15
    }
  }'
```

**3. Query Task Result:**
```bash
curl http://localhost:8000/jobs/<job-id>
```

---

## Architecture

```text
[Browser / Client] ──► [FastAPI Gateway]
                              │
                    POST /jobs│ (inject W3C traceparent)
                              ▼
                    ┌─────────────────────────┐
                    │ NATS JetStream (`JOBS`) │
                    │ (work-queue retention)  │
                    └───────────┬─────────────┘
                                │ pull_subscribe (queue: job-workers)
                                ▼
                    ┌─────────────────────────────────────────┐
                    │ Worker × 2 (async, bounded semaphore)   │
                    │  1. Lookup job_type in ProcessorRegistry│
                    │  2. Execute Processor (Image/Data/etc.) │
                    │  3. Store result artifact in RustFS S3  │
                    │  4. Update state in PostgreSQL (`jobs`) │
                    │  5. Publish event to `jobs.events`      │
                    │  6. Emit OTEL metrics & spans           │
                    │  7. Explicit Ack / Nak (DLQ routing)    │
                    └───────────┬─────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌──────────────┐        ┌──────────────┐        ┌──────────────┐
│ RustFS (S3)  │        │PostgreSQL 18 │        │Audit Archiver│
│  data bucket │        │  jobs table  │        │(Batch Gzip)  │
└──────────────┘        └──────────────┘        └───────┬──────┘
                                                        │
                                                        ▼
                                                ┌──────────────┐
                                                │ RustFS (S3)  │
                                                │ logs bucket  │
                                                │(.jsonl.gz)   │
                                                └──────────────┘
```

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the deep dive, [`docs/PLUGINS.md`](./docs/PLUGINS.md) for writing new processor plugins, and [`docs/DECISIONS.md`](./docs/DECISIONS.md) for design trade-offs.

---

## Stack

| Service | Purpose | Port |
|---|---|---|
| **NATS 2.10 (JetStream)** | Job queue (`jobs.request`, work-queue) + broadcast (`jobs.events`) + DLQ (`jobs.dlq`) | `4222`, `8222` |
| **PostgreSQL 18** | Relational metadata tracking `jobs` state machines (`PENDING` $\rightarrow$ `PROCESSING` $\rightarrow$ `COMPLETED`) | `5432` |
| **RustFS** | S3-compatible storage for artifacts (`processing-data`) and audit logs (`processing-logs`) | `9000`, `9001` |
| **OpenObserve** | Unified telemetry ingestion (OTLP HTTP `/v1/traces`, `/v1/metrics`) + PromQL endpoint | `5080` |
| **Grafana** | Auto-provisioned "Distributed Processing Overview" dashboard over OpenObserve | `3000` |
| **API (FastAPI)** | `POST /jobs`, `GET /jobs/{id}`, `GET /processors`, `GET /events` (SSE), `GET /` | `8000` |
| **Worker × 2** | Pull subscribe (queue group `job-workers`), bounded in-flight concurrency | — |
| **Audit Archiver** | Standalone consumer buffering NATS events and flushing Gzip JSONL batches to S3 | — |

---

## Pluggable Processors

The engine uses a pluggable registry pattern. Built-in processors:

| Processor Type | Implementation | Description |
|---|---|---|
| `image:blur` | `ImageBlurProcessor` | Downloads image, applies Gaussian blur via Pillow, stores to S3 |
| `image:grayscale` | `ImageGrayscaleProcessor` | Converts image to grayscale, stores to S3 |
| `image:resize` | `ImageResizeProcessor` | Resizes image to specified `width` and `height`, stores to S3 |
| `data:transform` | `DataTransformProcessor` | Statistical analysis (`avg`, `sum`, `min`, `max`), SHA256 hashing, JSON artifact generation |

To add your own custom processor, see [`docs/PLUGINS.md`](./docs/PLUGINS.md).

---

## Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Submit arbitrary async job: `{"job_type": "...", "payload": {...}}` |
| `POST` | `/jobs/upload` | Upload a raw file to S3 and trigger a processing job |
| `GET` | `/jobs/{job_id}` | Retrieve job state, duration, and output JSON result |
| `GET` | `/jobs` | List recent jobs with optional `job_type` query filter |
| `GET` | `/processors` | List all registered and available processor plugins |
| `GET` | `/events` | Server-Sent Events (SSE) stream of live job lifecycle events |
| `GET` | `/stats` | System connectivity and job statistics |
| `GET` | `/` | Live interactive web dashboard |
| `GET` | `/healthz` | Liveness probe |

---

## How a Task Flows Through the System

1. **Client** → `POST /jobs {"job_type": "data:transform", "payload": {...}}`
2. **API** creates a UUID `job_id`, records `PENDING` in PostgreSQL `jobs` table, opens `api.submit_job` OTEL trace span, and publishes message to NATS JetStream `jobs.request`.
3. **NATS** work-queue stream delivers the message to **one** worker in the `job-workers` queue group (competing consumers).
4. **Worker** extracts trace context, opens `worker.process_job` span, updates DB state to `PROCESSING`, and emits `STARTED` event to `jobs.events`.
5. **Processor** looked up via `ProcessorRegistry` executes the transformation (`data:transform`, `image:blur`, etc.) and stores output artifacts in RustFS S3.
6. **Worker** updates PostgreSQL `jobs` record to `COMPLETED` with execution `duration_ms` and `result` JSONB, increments OTEL metrics (`jobs_completed_total`, `job_duration_ms`), emits `COMPLETED` event, and acknowledges (ACK) the NATS message.
7. **Audit Archiver** buffers event payloads and flushes compressed `s3://processing-logs/logs/YYYY/MM/DD/batch_{uuid}.jsonl.gz` batches every 100 events or 15s, writing the S3 pointer back to PostgreSQL.
8. **Dashboard** streams live events over SSE; **Grafana** plots throughput and latency in real time.

---

## Benchmark & Analytics Scripts

### Run Load Generator (Seed Script)
```bash
# Burst 100 image jobs concurrently across workers
python3 scripts/seed_jobs.py --api-url http://localhost:18000 --count 100 --concurrency 20 --category image

# Burst 50 numeric data transformation jobs
python3 scripts/seed_jobs.py --api-url http://localhost:18000 --count 50 --concurrency 10 --category data

# Burst 1,000 mixed jobs across a scaled worker pool
docker compose up -d --scale worker=4
python3 scripts/seed_jobs.py --api-url http://localhost:18000 --count 1000 --concurrency 30 --category all
```

### Run DuckDB S3 Analytics
Query historical `.jsonl.gz` audit batches directly from S3 without querying PostgreSQL:
```bash
uv run --with duckdb python3 scripts/query_logs_duckdb.py --run
```

---

## Development & Testing

```bash
uv sync                              # install dependencies
uv run ruff check src/ tests/        # linting
uv run mypy src/                     # strict type checking
uv run pytest tests/unit             # run unit test suite
```

---

## Documentation Links

- [docs/TESTING.md](./docs/TESTING.md) — Comprehensive verification and testing guide with step-by-step cURL commands, benchmarks, and DuckDB S3 analytics.
- [docs/FAILURE_MODES.md](./docs/FAILURE_MODES.md) — Failure mode & effects analysis (FMEA), distributed edge cases, and resiliency mitigations.
- [ARCHITECTURE.md](./ARCHITECTURE.md) — In-depth component architecture, lifecycle, and DLQ design.
- [docs/PLUGINS.md](./docs/PLUGINS.md) — Guide to creating and registering custom processor plugins.
- [docs/TELEMETRY.md](./docs/TELEMETRY.md) — OpenTelemetry traces, metrics specification, and OpenObserve guide.
- [docs/DECISIONS.md](./docs/DECISIONS.md) — Architectural decisions and trade-offs.

---

## License

MIT
