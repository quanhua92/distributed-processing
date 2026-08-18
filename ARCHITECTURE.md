# Architecture

Detailed design of the distributed processing platform. Read the [README](./README.md) first for the quickstart and overview.

---

## 1. System Topology

```text
[Client / Dashboard] ──► [FastAPI Gateway] ──► [NATS JetStream (`JOBS` stream)]
                                                     │
                                                     ▼
                                      [Pluggable Workers (competing)]
                                        ├── Read/Write S3 Artifacts (`processing-data`)
                                        ├── State Updates ──► [PostgreSQL 18 (`jobs`)]
                                        └── Emit Events ────► [Audit Archiver]
                                                                   │
                                                                   ▼ (Batch Gzip)
                                                            [RustFS S3 Logs]
                                                            (s3://.../batch.jsonl.gz)
```

---

## 2. Core Components & Responsibilities

### 2.1 FastAPI Gateway (`src/distributed_processing/api/`)
- **`POST /jobs`**: Accepts any arbitrary `job_type` and `payload`. Generates `job_id`, records `PENDING` state in PostgreSQL, injects W3C `traceparent` OpenTelemetry headers, and publishes to NATS JetStream `jobs.request`.
- **`GET /jobs/{job_id}`**: Retrieves real-time execution status and output JSON results from PostgreSQL.
- **`GET /jobs`**: Returns recent jobs, with optional `job_type` query filtering.
- **`GET /processors`**: Returns all currently registered processor plugins.
- **`GET /events`**: Server-Sent Events (SSE) bridge subscribing to core NATS `jobs.events` and streaming live updates to browsers.
- **`GET /`**: Serves the static live web dashboard from `dashboard/index.html`.

### 2.2 Pluggable Workers (`src/distributed_processing/worker/`)
- **`loop.py`**: Competing-consumers pull subscription to `jobs.request` with durable queue group `job-workers`. Uses `asyncio.Semaphore(cfg.worker_concurrency)` to bound in-flight tasks per container. Handles `SIGINT`/`SIGTERM` gracefully.
- **`runner.py`**: Dispatches task to the appropriate processor looked up from `ProcessorRegistry`. Wraps execution in an OpenTelemetry span (`worker.process_job`), writes `COMPLETED`/`FAILED` state to PostgreSQL, and emits lifecycle events to `jobs.events`.
- **DLQ Rerouting**: If delivery count exceeds `worker_max_deliver`, the failed message is rerouted to `jobs.dlq`.

### 2.3 Processor Plugins (`src/distributed_processing/processors/`)
- **`base.py`**: Defines the `JobProcessor` protocol and `JobContext` dataclass (providing async S3, HTTP client, worker metadata).
- Built-in processors: `image:blur`, `image:grayscale`, `image:resize`, and `data:transform`.
- For creating new processors, see [`docs/PLUGINS.md`](./docs/PLUGINS.md).

### 2.4 Decoupled Audit Archiver (`src/distributed_processing/audit/`)
- Subscribes to `jobs.events`.
- Buffers raw event JSON payloads in memory.
- Flush triggers: **100 events** or **15 seconds** timer.
- Compresses buffered events into `s3://processing-logs/logs/YYYY/MM/DD/batch_{uuid}.jsonl.gz`.
- Updates `log_archive_s3_key` in PostgreSQL for all jobs in the flushed batch.

### 2.5 Storage Layer
- **PostgreSQL 18 (`src/distributed_processing/storage/postgres.py`)**: Stores lean job state machine records (`jobs` table).
- **RustFS S3 (`src/distributed_processing/storage/s3.py`)**: S3-compatible storage with two buckets:
  - `processing-data`: Job input/output artifacts (images, generated JSON summaries).
  - `processing-logs`: Compressed Gzip JSONL audit log batches.

---

## 3. Telemetry & Observability

- **OpenTelemetry (`src/distributed_processing/telemetry.py`)**: Both API and Worker replicas send traces (`/v1/traces`) and metrics (`/v1/metrics`) to **OpenObserve** via OTLP HTTP.
- **Grafana**: Pre-configured with OpenObserve Prometheus datasource to visualize job throughput, duration percentiles, and active worker concurrency.
- Detailed metrics and setup: see [`docs/TELEMETRY.md`](./docs/TELEMETRY.md).
