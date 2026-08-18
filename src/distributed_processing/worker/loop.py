"""Worker subscription loop with JetStream competing consumers."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
from typing import Final

import nats
from nats.aio.msg import Msg

from distributed_processing.nats_setup import ensure_nats_streams
from distributed_processing.processors.registry import create_default_registry
from distributed_processing.settings import Settings
from distributed_processing.storage.postgres import PostgresDatabase
from distributed_processing.storage.s3 import S3Storage
from distributed_processing.worker.runner import JobRunner

log: Final = logging.getLogger(__name__)


async def run_worker(cfg: Settings) -> None:
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    log.info("worker.starting id=%s concurrency=%d", worker_id, cfg.worker_concurrency)

    # Initialize streams and infra
    await ensure_nats_streams(cfg)

    db = PostgresDatabase(cfg)
    await db.connect()

    s3 = S3Storage(cfg)
    await s3.ensure_buckets()

    registry = create_default_registry()

    nc = await nats.connect(cfg.nats_url)
    js = nc.jetstream()

    runner = JobRunner(worker_id, cfg, db, s3, registry, nc.publish)
    semaphore = asyncio.Semaphore(cfg.worker_concurrency)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    async def message_handler(msg: Msg) -> None:
        async with semaphore:
            try:
                payload_msg = json.loads(msg.data.decode())
                success = await runner.execute(payload_msg)
                if success:
                    await msg.ack()
                else:
                    # Check delivery count for DLQ routing
                    meta = msg.metadata
                    if meta and meta.num_delivered >= cfg.worker_max_deliver:
                        log.warning("job.routed_to_dlq job_id=%s num_delivered=%d", payload_msg.get("job_id"), meta.num_delivered)
                        await js.publish(cfg.nats_subject_dlq, msg.data)
                        await msg.ack()
                    else:
                        await msg.nak(delay=2.0)
            except Exception:
                log.exception("worker.unhandled_exception")
                await msg.nak(delay=2.0)

    # Push subscription with durable queue group for competing consumers
    sub = await js.subscribe(
        subject=cfg.nats_subject_request,
        queue=cfg.nats_consumer_group,
        durable=cfg.nats_consumer_group,
        cb=message_handler,
        manual_ack=True,
    )
    log.info("worker.subscribed subject=%s queue=%s", cfg.nats_subject_request, cfg.nats_consumer_group)

    try:
        await stop_event.wait()
    finally:
        log.info("worker.shutting_down id=%s", worker_id)
        await sub.unsubscribe()
        await runner.close()
        await nc.drain()
        await db.close()
        log.info("worker.stopped id=%s", worker_id)
