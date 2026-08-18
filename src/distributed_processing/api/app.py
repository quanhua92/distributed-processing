"""FastAPI application factory and lifecycle management."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Final

import nats
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from distributed_processing.api.routes import router
from distributed_processing.nats_setup import ensure_nats_streams
from distributed_processing.settings import Settings
from distributed_processing.storage.postgres import PostgresDatabase
from distributed_processing.storage.s3 import S3Storage

log: Final = logging.getLogger(__name__)


def create_app(cfg: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        log.info("api.starting")

        # 1. Ensure NATS streams
        await ensure_nats_streams(cfg)

        # 2. Database pool
        db = PostgresDatabase(cfg)
        await db.connect()
        app.state.db = db

        # 3. S3 Storage
        s3 = S3Storage(cfg)
        await s3.ensure_buckets()
        app.state.s3 = s3

        # 4. NATS Client
        nc = await nats.connect(cfg.nats_url)
        js = nc.jetstream()
        app.state.nc = nc
        app.state.js = js
        app.state.cfg = cfg

        log.info("api.ready")
        try:
            yield
        finally:
            log.info("api.shutting_down")
            await nc.drain()
            await db.close()
            log.info("api.stopped")

    app = FastAPI(
        title="Distributed Image Processing API",
        description="POC for educational distributed image processing with FastAPI, NATS JetStream, Postgres 18, RustFS, and OpenObserve.",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router)

    # Instrument FastAPI for automatic OpenTelemetry tracing
    FastAPIInstrumentor.instrument_app(app)

    return app
