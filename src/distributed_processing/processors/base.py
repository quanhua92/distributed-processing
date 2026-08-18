"""Pluggable processor interfaces and context definitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from distributed_processing.settings import Settings
from distributed_processing.storage.s3 import S3Storage


@dataclass
class JobContext:
    job_id: str
    worker_id: str
    cfg: Settings
    s3: S3Storage
    http_client: httpx.AsyncClient


class JobProcessor(Protocol):
    """Protocol for arbitrary async job processors."""

    async def process(self, payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        """Execute the job transformation and return a result dictionary."""
        ...
