# Telemetry & Observability

This document details the OpenTelemetry (OTEL) instrumentation, OpenObserve configuration, and Grafana monitoring dashboard.

---

## 1. OpenTelemetry Setup

Both the FastAPI gateway (`api`) and the worker pool (`worker`) initialize OpenTelemetry via `setup_telemetry(component)` on startup:

```
[FastAPI / Workers]
       │
       ├── OTLP HTTP Traces  ──► http://openobserve:5080/api/default/v1/traces
       └── OTLP HTTP Metrics ──► http://openobserve:5080/api/default/v1/metrics
```

### Authentication
OpenObserve requires HTTP Basic Authentication headers for OTLP HTTP ingestion even when running locally:
- Default credentials: `admin@local.dev` / `Admin123!@#`.

---

## 2. Distributed Tracing Spans

| Span Name | Source | Attributes |
|---|---|---|
| `api.submit_job` | FastAPI | `job.id`, `job.type` |
| `worker.process_job` | Worker | `job.id`, `job.type`, `worker.id` |
| `HTTP GET / POST ...` | FastAPI | Auto-instrumented by `FastAPIInstrumentor` |

---

## 3. Prometheus-Compatible Metrics

| Metric Name | Type | Description | Dimensions / Labels |
|---|---|---|---|
| `jobs_submitted_total` | Counter | Total submitted tasks | `job_type` |
| `jobs_completed_total` | Counter | Successfully processed tasks | `worker_id`, `job_type` |
| `jobs_failed_total` | Counter | Failed tasks | `worker_id`, `job_type` |
| `job_duration_ms` | Histogram | Latency distribution (p50, p95, p99) | `worker_id`, `job_type` |
| `bytes_processed_total` | Counter | Raw bytes processed | - |
| `active_workers_gauge` | UpDownCounter | Currently in-flight tasks | - |
| `audit_batches_flushed_total` | Counter | Batches uploaded to S3 | - |

---

## 4. Grafana Dashboards & Provisioning

- **Datasource Provisioning (`grafana/provisioning/datasources/o2.yml`)**: Configures the OpenObserve Prometheus proxy at `http://openobserve:5080/api/default/prometheus`.
- **Prebuilt Dashboard (`grafana/dashboards/processing_overview.json`)**: Visualizes job submission rate, worker throughput, duration percentiles, active worker concurrency, and S3 batch archival rate.

Access Grafana at [http://localhost:3000](http://localhost:3000) (Anonymous Admin enabled).
