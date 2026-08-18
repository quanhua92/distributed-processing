"""Pydantic schemas for generic async jobs API."""

from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class JobSubmitRequest(BaseModel):
    job_type: str = Field(..., description="Registered processor type, e.g. 'image:blur', 'image:grayscale', 'data:transform'")
    payload: dict[str, Any] = Field(default_factory=dict, description="Arbitrary task payload parameters")


class JobResponse(BaseModel):
    job_id: str
    job_type: str
    status: str
    payload: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    worker_id: str | None = None
    duration_ms: int | None = None
    error_message: str | None = None
    log_archive_s3_key: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class ProcessorsListResponse(BaseModel):
    available_processors: list[str]


class StatsResponse(BaseModel):
    status: str
    nats_connected: bool
    postgres_connected: bool
    recent_jobs_count: int
    registered_processors: list[str]
