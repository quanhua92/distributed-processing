"""Worker job execution pipeline using pluggable processors."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Final

import httpx

from distributed_processing.processors.base import JobContext
from distributed_processing.processors.registry import ProcessorRegistry
from distributed_processing.settings import Settings
from distributed_processing.storage.postgres import PostgresDatabase
from distributed_processing.storage.s3 import S3Storage
from distributed_processing.telemetry import (
    active_workers_gauge,
    bytes_processed_counter,
    get_tracer,
    job_duration_histogram,
    jobs_completed_counter,
    jobs_failed_counter,
)

log: Final = logging.getLogger(__name__)


class JobRunner:
    def __init__(
        self,
        worker_id: str,
        cfg: Settings,
        db: PostgresDatabase,
        s3: S3Storage,
        registry: ProcessorRegistry,
        nc_publish_event: Any,
    ) -> None:
        self.worker_id = worker_id
        self.cfg = cfg
        self.db = db
        self.s3 = s3
        self.registry = registry
        self.publish_event = nc_publish_event
        self.http_client = httpx.AsyncClient(timeout=15.0)

    async def close(self) -> None:
        await self.http_client.aclose()

    async def execute(self, payload_msg: dict[str, Any]) -> bool:
        job_id = payload_msg["job_id"]
        job_type = payload_msg.get("job_type", "image:blur")
        payload = payload_msg.get("payload", {})

        tracer = get_tracer()
        span_ctx = tracer.start_as_current_span(
            "worker.process_job",
            attributes={
                "job.id": job_id,
                "job.type": job_type,
                "worker.id": self.worker_id,
            },
        ) if tracer else None

        if active_workers_gauge:
            active_workers_gauge.add(1)

        start_time = time.monotonic()
        try:
            # 1. Emit STARTED event and update DB
            await self._emit_event(job_id, job_type, "STARTED")
            await self.db.update_job_status(job_id, "PROCESSING", worker_id=self.worker_id)

            # 2. Lookup Processor
            processor = self.registry.get(job_type)
            if not processor:
                raise ValueError(f"No processor registered for job_type '{job_type}'")

            ctx = JobContext(
                job_id=job_id,
                worker_id=self.worker_id,
                cfg=self.cfg,
                s3=self.s3,
                http_client=self.http_client,
            )

            # 3. Execute processing
            result = await processor.process(payload, ctx)

            duration_ms = int((time.monotonic() - start_time) * 1000)

            # 4. Update Postgres state
            await self.db.update_job_status(
                job_id,
                "COMPLETED",
                worker_id=self.worker_id,
                result=result,
                duration_ms=duration_ms,
            )

            # 5. Emit COMPLETED event
            await self._emit_event(
                job_id,
                job_type,
                "COMPLETED",
                extra={
                    "duration_ms": duration_ms,
                    "result": result,
                },
            )

            # 6. Record OTEL metrics
            if jobs_completed_counter:
                jobs_completed_counter.add(1, {"worker_id": self.worker_id, "job_type": job_type})
            if job_duration_histogram:
                job_duration_histogram.record(duration_ms, {"worker_id": self.worker_id, "job_type": job_type})
            if bytes_processed_counter and "bytes_processed" in result:
                bytes_processed_counter.add(int(result["bytes_processed"]))

            log.info("job.completed job_id=%s type=%s duration_ms=%d", job_id, job_type, duration_ms)
            return True

        except Exception as exc:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            error_msg = str(exc)
            log.exception("job.failed job_id=%s type=%s error=%s", job_id, job_type, error_msg)

            await self.db.update_job_status(
                job_id,
                "FAILED",
                worker_id=self.worker_id,
                duration_ms=duration_ms,
                error_message=error_msg,
            )
            await self._emit_event(
                job_id,
                job_type,
                "FAILED",
                extra={"error": error_msg, "duration_ms": duration_ms},
            )

            if jobs_failed_counter:
                jobs_failed_counter.add(1, {"worker_id": self.worker_id, "job_type": job_type})

            return False

        finally:
            if active_workers_gauge:
                active_workers_gauge.add(-1)
            if span_ctx:
                span_ctx.__exit__(None, None, None)

    async def _emit_event(self, job_id: str, job_type: str, event_type: str, extra: dict[str, Any] | None = None) -> None:
        evt: dict[str, Any] = {
            "job_id": job_id,
            "job_type": job_type,
            "event": event_type,
            "worker_id": self.worker_id,
            "timestamp": time.time(),
        }
        if extra:
            evt.update(extra)
        await self.publish_event(self.cfg.nats_subject_events, json.dumps(evt).encode())
