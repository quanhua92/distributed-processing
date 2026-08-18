"""Decoupled audit consumer — buffers NATS events, compresses to Gzip JSONL, and flushes to S3."""

from __future__ import annotations

import asyncio
import datetime
import gzip
import json
import logging
import signal
import uuid
from typing import Any, Final

import nats
from nats.aio.msg import Msg

from distributed_processing.nats_setup import ensure_nats_streams
from distributed_processing.settings import Settings
from distributed_processing.storage.postgres import PostgresDatabase
from distributed_processing.storage.s3 import S3Storage
from distributed_processing.telemetry import audit_batches_counter

log: Final = logging.getLogger(__name__)


class AuditArchiver:
    def __init__(self, cfg: Settings, db: PostgresDatabase, s3: S3Storage) -> None:
        self.cfg = cfg
        self.db = db
        self.s3 = s3
        self.buffer: list[dict[str, Any]] = []
        self.lock = asyncio.Lock()

    async def add_event(self, event: dict[str, Any]) -> None:
        async with self.lock:
            self.buffer.append(event)
            if len(self.buffer) >= self.cfg.audit_batch_size:
                await self._flush_locked()

    async def flush_timer(self) -> None:
        """Periodic timer to flush any lingering events."""
        while True:
            await asyncio.sleep(self.cfg.audit_flush_interval_seconds)
            async with self.lock:
                if self.buffer:
                    await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self.buffer:
            return

        events_to_flush = list(self.buffer)
        self.buffer.clear()

        batch_id = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc)
        s3_key = f"logs/{now.strftime('%Y/%m/%d')}/batch_{batch_id}.jsonl.gz"

        # Compress to JSONL Gzip
        jsonl_lines = [json.dumps(e) for e in events_to_flush]
        raw_data = "\n".join(jsonl_lines).encode("utf-8")
        compressed = gzip.compress(raw_data)

        try:
            await self.s3.put_object(
                self.cfg.s3_bucket_logs,
                s3_key,
                compressed,
                content_type="application/gzip",
            )

            # Extract unique job_ids to update Postgres pointer
            job_ids = list({e["job_id"] for e in events_to_flush if "job_id" in e})
            if job_ids:
                await self.db.update_log_archive_key(job_ids, f"s3://{self.cfg.s3_bucket_logs}/{s3_key}")

            if audit_batches_counter:
                audit_batches_counter.add(1)

            log.info(
                "audit.flushed events=%d s3_key=%s bytes=%d",
                len(events_to_flush),
                s3_key,
                len(compressed),
            )
        except Exception:
            log.exception("audit.flush_failed batch_id=%s", batch_id)


async def run_audit_consumer(cfg: Settings) -> None:
    log.info("audit_consumer.starting")
    await ensure_nats_streams(cfg)

    db = PostgresDatabase(cfg)
    await db.connect()

    s3 = S3Storage(cfg)
    await s3.ensure_buckets()

    archiver = AuditArchiver(cfg, db, s3)
    nc = await nats.connect(cfg.nats_url)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    async def on_event(msg: Msg) -> None:
        try:
            data = json.loads(msg.data.decode())
            await archiver.add_event(data)
        except Exception:
            log.exception("audit.event_parse_error")

    sub = await nc.subscribe(cfg.nats_subject_events, cb=on_event)
    timer_task = asyncio.create_task(archiver.flush_timer())

    log.info("audit_consumer.ready subscribed=%s", cfg.nats_subject_events)

    try:
        await stop_event.wait()
    finally:
        log.info("audit_consumer.shutting_down")
        timer_task.cancel()
        await sub.unsubscribe()
        async with archiver.lock:
            await archiver._flush_locked()
        await nc.drain()
        await db.close()
        log.info("audit_consumer.stopped")
