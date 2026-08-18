# Verification & Testing Guide

Comprehensive guide for running unit tests, end-to-end task executions, high-concurrency benchmarks, negative fault-tolerance tests, and telemetry analytics.

---

## 1. Port & Service Reference

| Service | Container Port | Local Host Port (`docker-compose.local.yml`) | Purpose |
| :--- | :--- | :--- | :--- |
| **API & Web UI** | `8000` | `http://localhost:18000` | Job submission, SSE events, Web UI |
| **Grafana** | `3000` | `http://localhost:13000` | Metrics dashboard (`admin` / `admin`) |
| **OpenObserve** | `5080` | `http://localhost:15080` | Logs & OTLP traces (`admin@local.dev` / `Admin123!@#`) |
| **RustFS S3** | `9000` / `9001` | `http://localhost:19000` / `19001` | S3 API / Web Console (`rustfsadmin` / `rustfsadmin123`) |
| **PostgreSQL 18** | `5432` | `localhost:15434` | Relational jobs database (`postgres` / `postgres`) |
| **NATS Core / JetStream** | `4222` / `8222` | `localhost:14222` / `18222` | Messaging & HTTP monitoring |

---

## 2. Unit Testing Suite

Run the isolated unit tests for processor algorithms and dynamic plugin registry:

```bash
uv run pytest tests/unit
```

**Expected Output:**
```text
tests/unit/test_processors.py .. [ 66%]
tests/unit/test_registry.py .    [100%]
=================== 3 passed in 2.62s ====================
```

---

## 3. End-to-End Verification (cURL)

### Step 1: Verify Infrastructure Health & Registered Plugins

```bash
# Check connectivity to PostgreSQL and NATS
curl -s http://localhost:18000/stats

# List available processor plugins
curl -s http://localhost:18000/processors
```

**Expected Response (`/stats`):**
```json
{
  "status": "healthy",
  "nats_connected": true,
  "postgres_connected": true,
  "recent_jobs_count": 0,
  "registered_processors": ["data:transform", "image:blur", "image:grayscale", "image:resize"]
}
```

---

### Step 2: Submit a `data:transform` Job (Numeric Aggregation & SHA256)

```bash
curl -s -X POST http://localhost:18000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_type": "data:transform",
    "payload": {
      "operation": "aggregate",
      "data": [10.5, 42.0, 99.1, 150.0, 3.14, 88.0]
    }
  }'
```

**Expected Initial Response:**
```json
{
  "job_id": "4face4f2-f5bc-4f45-8df4-81c5b8aca4f9",
  "job_type": "data:transform",
  "status": "PENDING",
  "payload": { "operation": "aggregate", "data": [10.5, 42.0, 99.1, 150.0, 3.14, 88.0] }
}
```

---

### Step 3: Query Completed Job by ID

```bash
curl -s http://localhost:18000/jobs/<JOB_ID>
```

**Expected Result:**
```json
{
  "job_id": "4face4f2-f5bc-4f45-8df4-81c5b8aca4f9",
  "job_type": "data:transform",
  "status": "COMPLETED",
  "result": {
    "stats": { "avg": 65.45, "max": 150.0, "min": 3.14, "sum": 392.74, "count": 6 },
    "sha256": "1056262ba112d967c48f2d37ac8b01f4a3226138ce803b18c1349abde0c6013d",
    "artifact_s3_url": "s3://processing-data/results/4face4f2-f5bc-4f45-8df4-81c5b8aca4f9_summary.json"
  },
  "worker_id": "e0ac383f7ade-1",
  "duration_ms": 37,
  "error_message": null
}
```

---

### Step 4: Submit Image Processing Jobs

#### A. Gaussian Blur (`image:blur`)
```bash
curl -s -X POST http://localhost:18000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_type": "image:blur",
    "payload": {
      "source_url": "https://picsum.photos/600/400",
      "radius": 10
    }
  }'
```

#### B. Grayscale Conversion (`image:grayscale`)
```bash
curl -s -X POST http://localhost:18000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_type": "image:grayscale",
    "payload": {
      "source_url": "https://picsum.photos/600/400"
    }
  }'
```

#### C. Smart Thumbnail Resize (`image:resize`)
```bash
curl -s -X POST http://localhost:18000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_type": "image:resize",
    "payload": {
      "source_url": "https://picsum.photos/800/600",
      "width": 300,
      "height": 200
    }
  }'
```

---

## 4. Live Server-Sent Events (SSE) Stream

Subscribe to live lifecycle events in real time (`STARTED`, `COMPLETED`, `FAILED`):

```bash
curl -N http://localhost:18000/events
```

**Stream Output:**
```text
data: {"job_id":"7d5c7da7...","job_type":"image:blur","event":"STARTED","worker_id":"00daf86a0280-1","timestamp":1787080233.1}

data: {"job_id":"7d5c7da7...","job_type":"image:blur","event":"COMPLETED","worker_id":"00daf86a0280-1","duration_ms":1226,"result":{"output_url":"s3://processing-data/processed/7d5c7da7..._blur.jpg"},"timestamp":1787080234.3}
```

---

## 5. High-Concurrency Benchmark Testing

Generate mixed-load bursts across all processor plugins:

```bash
# Seed 50 jobs with 8 concurrent workers
python3 scripts/seed_jobs.py --api-url http://localhost:18000 --count 50 --concurrency 8

# Seed 100 jobs with 16 concurrent workers
python3 scripts/seed_jobs.py --api-url http://localhost:18000 --count 100 --concurrency 16
```

**Expected Metric:** Seeding speed between **400 - 750+ jobs/sec**.

---

## 6. Fault-Tolerance & Negative Testing

### Scenario 1: Invalid URL / Network Host Failure
```bash
curl -s -X POST http://localhost:18000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_type": "image:blur",
    "payload": {
      "source_url": "https://invalid-host-404-domain.xyz/nonexistent.jpg",
      "radius": 5
    }
  }'
```
- **Result**: State transitions to `FAILED`.
- **Database record**: `error_message: "[Errno -2] Name or service not known"`.

### Scenario 2: Invalid Transformation Dimensions
```bash
curl -s -X POST http://localhost:18000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_type": "image:resize",
    "payload": {
      "source_url": "https://picsum.photos/400/300",
      "width": -50,
      "height": -50
    }
  }'
```
- **Result**: State transitions to `FAILED`.
- **Database record**: `error_message: "height and width must be > 0"`.

### Scenario 3: Unknown Job Type (API Validation Guard)
```bash
curl -s -X POST http://localhost:18000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"job_type": "unknown:processor", "payload": {}}'
```
- **Result**: **HTTP 400 Bad Request** immediately returned at API layer without polluting queue.

---

## 7. PostgreSQL Database State Verification

Query the PostgreSQL 18 container directly:

```bash
docker exec -it distributed-processing-postgres-1 psql -U postgres -d processing -c "
SELECT 
    status, 
    job_type, 
    COUNT(*) as count, 
    ROUND(AVG(duration_ms)::numeric, 1) as avg_duration_ms 
FROM jobs 
GROUP BY status, job_type 
ORDER BY status, count DESC;
"
```

---

## 8. S3 Audit Analytics (DuckDB Direct Query)

Query batched `.jsonl.gz` audit files stored in RustFS S3 without downloading them:

```bash
uv run --with duckdb python3 scripts/query_logs_duckdb.py --run
```

**Sample Output:**
```text
┌───────────┬─────────────────┬──────────────┬─────────────────┬─────────────────┬─────────────────┐
│   event   │    job_type     │ total_events │ avg_duration_ms │ max_duration_ms │ min_duration_ms │
├───────────┼─────────────────┼──────────────┼─────────────────┼─────────────────┼─────────────────┤
│ STARTED   │ data:transform  │           81 │            NULL │            NULL │            NULL │
│ COMPLETED │ data:transform  │           81 │            20.3 │             120 │               1 │
│ STARTED   │ image:blur      │           52 │            NULL │            NULL │            NULL │
│ STARTED   │ image:resize    │           50 │            NULL │            NULL │            NULL │
│ STARTED   │ image:grayscale │           48 │            NULL │            NULL │            NULL │
│ COMPLETED │ image:grayscale │           48 │           359.3 │            1038 │             262 │
│ COMPLETED │ image:resize    │           47 │           557.4 │            1840 │             278 │
│ COMPLETED │ image:blur      │           46 │           359.9 │            1226 │             269 │
│ FAILED    │ image:blur      │            6 │           297.0 │             774 │               4 │
│ FAILED    │ image:resize    │            3 │           495.7 │             638 │             319 │
└───────────┴─────────────────┴──────────────┴─────────────────┴─────────────────┴─────────────────┘
```

---

## 9. Observability & Dashboard Verification

1. **Grafana Dashboard**: Open [http://localhost:13000](http://localhost:13000)
   - View live counters: `Jobs Submitted`, `Jobs Completed`, `Jobs Failed / DLQ`, `Audit Batches Flushed`.
   - Monitor real-time throughput graphs (`ops/sec`) and p95 latency curves.
2. **OpenObserve Metrics / Traces**: Open [http://localhost:15080](http://localhost:15080)
   - Login: `admin@local.dev` / `Admin123!@#`
   - Explore traces tagged under `worker.process_job` and `api.submit_job`.
3. **RustFS S3 Web Console**: Open [http://localhost:19001](http://localhost:19001)
   - Login: `rustfsadmin` / `rustfsadmin123`
   - Browse buckets `processing-data` (results) and `processing-logs` (gzipped JSONL audit logs).
