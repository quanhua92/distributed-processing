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
jobs_submitted_counter: metrics.Counter | None = None
jobs_completed_counter: metrics.Counter | None = None
jobs_failed_counter: metrics.Counter | None = None
job_duration_histogram: metrics.Histogram | None = None
bytes_processed_counter: metrics.Counter | None = None
active_workers_gauge: metrics.UpDownCounter | None = None
audit_batches_counter: metrics.Counter | None = None


def setup_telemetry(
    component: str,
    o2_endpoint: str,
    *,
    o2_user: str = "",
    o2_password: str = "",
) -> metrics.Meter | None:
    """Initialize OpenTelemetry OTLP exporters for traces and metrics.

    Safe to call multiple times. Subsequent calls return the existing meter.
    """
    global _initialized, _meter, _tracer
    global jobs_submitted_counter, jobs_completed_counter, jobs_failed_counter
    global job_duration_histogram, bytes_processed_counter, active_workers_gauge, audit_batches_counter

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
    _tracer = trace.get_tracer("distributed-processing", version="0.1.0")

    # ── Metrics ───────────────────────────────────────────────────────────
    metric_reader = PeriodicExportingMetricReader(
        exporter=OTLPMetricExporter(
            endpoint=f"{o2_endpoint}/v1/metrics",
            timeout=10,
            headers=headers,
        ),
        export_interval_millis=5_000,  # 5s flush interval
    )
    mp = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(mp)
    _meter = metrics.get_meter("distributed-processing", version="0.1.0")

    # Initialize instruments
    jobs_submitted_counter = _meter.create_counter(
        "jobs_submitted_total",
        description="Total number of image processing jobs submitted",
        unit="1",
    )
    jobs_completed_counter = _meter.create_counter(
        "jobs_completed_total",
        description="Total number of image processing jobs completed successfully",
        unit="1",
    )
    jobs_failed_counter = _meter.create_counter(
        "jobs_failed_total",
        description="Total number of image processing jobs failed",
        unit="1",
    )
    job_duration_histogram = _meter.create_histogram(
        "job_duration_ms",
        description="Execution duration of image processing jobs in milliseconds",
        unit="ms",
    )
    bytes_processed_counter = _meter.create_counter(
        "bytes_processed_total",
        description="Total bytes of images processed",
        unit="bytes",
    )
    active_workers_gauge = _meter.create_up_down_counter(
        "active_workers_gauge",
        description="Number of currently in-flight worker executions",
        unit="1",
    )
    audit_batches_counter = _meter.create_counter(
        "audit_batches_flushed_total",
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


def get_meter() -> metrics.Meter | None:
    return _meter


def get_tracer() -> trace.Tracer | None:
    return _tracer
