"""OpenTelemetry setup — sends traces + metrics to OpenObserve via OTLP HTTP."""

from __future__ import annotations

import base64
import logging
from typing import Any, Final

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

log: Final = logging.getLogger(__name__)

_initialized: bool = False
_meter: metrics.Meter | None = None
_tracer: trace.Tracer | None = None

# Metric instruments
_jobs_submitted = None
_jobs_completed = None
_jobs_failed = None
_job_duration = None
_bytes_processed = None
_active_workers = None
_audit_batches = None


def setup_telemetry(
    component: str,
    o2_endpoint: str,
    *,
    o2_user: str = "",
    o2_password: str = "",
) -> metrics.Meter | None:
    """Initialize OpenTelemetry OTLP exporters for traces and metrics."""
    global _initialized, _meter, _tracer
    global _jobs_submitted, _jobs_completed, _jobs_failed, _job_duration, _bytes_processed, _active_workers, _audit_batches

    if _initialized:
        return _meter
    _initialized = True

    if not o2_endpoint:
        log.info("telemetry.disabled (O2_OTLP_ENDPOINT empty)")
        return None

    headers = _auth_headers(o2_user, o2_password)

    resource = Resource.create(
        {
            "service.name": "distributed-processing",
            "service.component": component,
            "deployment.environment": "poc",
        }
    )

    # ── Traces ────────────────────────────────────────────────────────────
    tp = TracerProvider(resource=resource)
    tp.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=f"{o2_endpoint}/v1/traces",
                timeout=10,
                headers=headers,
            )
        )
    )
    trace.set_tracer_provider(tp)
    _tracer = trace.get_tracer("distributed-processing", "0.1.0")

    # ── Metrics ───────────────────────────────────────────────────────────
    metric_reader = PeriodicExportingMetricReader(
        exporter=OTLPMetricExporter(
            endpoint=f"{o2_endpoint}/v1/metrics",
            timeout=10,
            headers=headers,
        ),
        export_interval_millis=2_000,  # 2s flush interval for fast updates
    )
    mp = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(mp)
    _meter = metrics.get_meter("distributed-processing", "0.1.0")

    _jobs_submitted = _meter.create_counter(
        "jobs_submitted",
        description="Total number of jobs submitted",
        unit="1",
    )
    _jobs_completed = _meter.create_counter(
        "jobs_completed",
        description="Total number of jobs completed successfully",
        unit="1",
    )
    _jobs_failed = _meter.create_counter(
        "jobs_failed",
        description="Total number of jobs failed",
        unit="1",
    )
    _job_duration = _meter.create_histogram(
        "job_duration_ms",
        description="Execution duration of jobs in milliseconds",
        unit="ms",
    )
    _bytes_processed = _meter.create_counter(
        "bytes_processed",
        description="Total bytes processed",
        unit="bytes",
    )
    _active_workers = _meter.create_up_down_counter(
        "active_workers",
        description="Number of currently in-flight worker executions",
        unit="1",
    )
    _audit_batches = _meter.create_counter(
        "audit_batches_flushed",
        description="Total number of audit event batches uploaded to S3",
        unit="1",
    )

    log.info("telemetry.ready component=%s endpoint=%s", component, o2_endpoint)
    return _meter


def _auth_headers(user: str, password: str) -> dict[str, str]:
    if not user or not password:
        return {}
    token = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def get_tracer() -> trace.Tracer | None:
    return _tracer


# Metric recording helper functions
def record_job_submitted(job_type: str) -> None:
    if _jobs_submitted:
        _jobs_submitted.add(1, {"job_type": job_type})


def record_job_completed(worker_id: str, job_type: str, duration_ms: int, bytes_count: int = 0) -> None:
    if _jobs_completed:
        _jobs_completed.add(1, {"worker_id": worker_id, "job_type": job_type})
    if _job_duration:
        _job_duration.record(duration_ms, {"worker_id": worker_id, "job_type": job_type})
    if _bytes_processed and bytes_count > 0:
        _bytes_processed.add(bytes_count, {"job_type": job_type})


def record_job_failed(worker_id: str, job_type: str) -> None:
    if _jobs_failed:
        _jobs_failed.add(1, {"worker_id": worker_id, "job_type": job_type})


def record_active_worker(delta: int) -> None:
    if _active_workers:
        _active_workers.add(delta)


def record_audit_batch_flushed() -> None:
    if _audit_batches:
        _audit_batches.add(1)
