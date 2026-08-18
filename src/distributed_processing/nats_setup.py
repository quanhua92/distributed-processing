"""NATS JetStream stream and consumer setup."""

from __future__ import annotations

import logging
from typing import Final

import nats
from nats.js.api import RetentionPolicy, StorageType, StreamConfig

from distributed_processing.settings import Settings

log: Final = logging.getLogger(__name__)


async def ensure_nats_streams(cfg: Settings) -> None:
    """Ensure the NATS JetStream stream and required subjects exist."""
    nc = await nats.connect(cfg.nats_url)
    js = nc.jetstream()

    stream_config = StreamConfig(
        name=cfg.nats_stream,
        subjects=[
            cfg.nats_subject_request,
            cfg.nats_subject_events,
            cfg.nats_subject_dlq,
        ],
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        duplicate_window=120.0,
    )

    try:
        await js.add_stream(stream_config)
        log.info("nats.stream_created name=%s", cfg.nats_stream)
    except Exception:
        # If exists, update stream config
        try:
            await js.update_stream(stream_config)
            log.info("nats.stream_updated name=%s", cfg.nats_stream)
        except Exception as err:
            log.warning("nats.stream_setup_notice: %s", err)

    await nc.close()
