"""Main CLI entrypoint for distributed-processing."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

import uvicorn

from distributed_processing.api.app import create_app
from distributed_processing.audit.consumer import run_audit_consumer
from distributed_processing.nats_setup import ensure_nats_streams
from distributed_processing.settings import settings
from distributed_processing.storage.postgres import PostgresDatabase
from distributed_processing.storage.s3 import S3Storage
from distributed_processing.telemetry import setup_telemetry
from distributed_processing.worker.loop import run_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("distributed_processing")


async def init_infrastructure() -> None:
    """Initialize NATS streams, S3 buckets, and verify Postgres."""
    log.info("infra.initializing")
    await ensure_nats_streams(settings)
    s3 = S3Storage(settings)
    await s3.ensure_buckets()
    db = PostgresDatabase(settings)
    await db.connect()
    await db.close()
    log.info("infra.ready")


def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed Image Processing Engine")
    parser.add_argument(
        "--role",
        choices=["api", "worker", "audit", "init-infra"],
        default="api",
        help="Service role to run",
    )
    args = parser.parse_args()

    # 1. Setup Telemetry for OpenObserve
    setup_telemetry(
        component=args.role,
        o2_endpoint=settings.o2_otlp_endpoint,
        o2_user=settings.o2_user,
        o2_password=settings.o2_password,
    )

    if args.role == "api":
        app = create_app(settings)
        uvicorn.run(
            app,
            host=settings.api_host,
            port=settings.api_port,
            log_level="info",
        )
    elif args.role == "worker":
        asyncio.run(run_worker(settings))
    elif args.role == "audit":
        asyncio.run(run_audit_consumer(settings))
    elif args.role == "init-infra":
        asyncio.run(init_infrastructure())
    else:
        sys.exit(f"Unknown role: {args.role}")


if __name__ == "__main__":
    main()
