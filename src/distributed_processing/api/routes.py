"""FastAPI router endpoints for generic async jobs, SSE, and queries."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from pathlib import Path
from typing import Any, AsyncGenerator, Final

import nats
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse

from distributed_processing.api.schemas import JobResponse, JobSubmitRequest, ProcessorsListResponse, StatsResponse
from distributed_processing.processors.registry import create_default_registry
from distributed_processing.telemetry import get_tracer, record_job_submitted

log: Final = logging.getLogger(__name__)
router = APIRouter()
registry = create_default_registry()


@router.get("/", response_class=HTMLResponse)
async def serve_dashboard() -> str:
    """Serve the static HTML dashboard."""
    dashboard_path = Path(__file__).resolve().parent.parent.parent.parent / "dashboard" / "index.html"
    if dashboard_path.exists():
        return dashboard_path.read_text(encoding="utf-8")
    return "<h1>Distributed Processing API</h1><p>Dashboard file not found.</p>"


@router.get("/processors", response_model=ProcessorsListResponse)
async def list_processors() -> dict[str, Any]:
    """List all registered and available job processors."""
    return {"available_processors": registry.list_types()}


@router.post("/jobs", response_model=JobResponse)
async def submit_job(req: JobSubmitRequest, request: Request) -> dict[str, Any]:
    """Submit an arbitrary async job for distributed processing."""
    if not registry.get(req.job_type):
        raise HTTPException(
            status_code=400,
            detail=f"Unknown job_type '{req.job_type}'. Available: {registry.list_types()}",
        )

    job_id = str(uuid.uuid4())
    tracer = get_tracer()

    span = tracer.start_span(
        "api.submit_job",
        attributes={"job.id": job_id, "job.type": req.job_type},
    ) if tracer else None

    try:
        db = request.app.state.db
        js = request.app.state.js
        cfg = request.app.state.cfg

        # 1. Insert PENDING into PostgreSQL
        await db.create_job(job_id, req.job_type, req.payload)

        # 2. Publish to NATS JetStream
        msg_payload = {
            "job_id": job_id,
            "job_type": req.job_type,
            "payload": req.payload,
        }
        await js.publish(cfg.nats_subject_request, json.dumps(msg_payload).encode())

        # 3. Increment OTEL metric
        record_job_submitted(req.job_type)

        log.info("job.submitted id=%s type=%s", job_id, req.job_type)

        return {
            "job_id": job_id,
            "job_type": req.job_type,
            "status": "PENDING",
            "payload": req.payload,
        }
    finally:
        if span:
            span.end()


@router.post("/jobs/image/blur", response_model=JobResponse)
async def submit_image_blur(
    request: Request,
    source_url: str = Form("https://picsum.photos/800/600"),
    radius: int = Form(5),
) -> dict[str, Any]:
    """Convenience endpoint to submit an image blur task."""
    req = JobSubmitRequest(
        job_type="image:blur",
        payload={"source_url": source_url, "radius": radius},
    )
    return await submit_job(req, request)


@router.post("/jobs/upload", response_model=JobResponse)
async def upload_and_process(
    request: Request,
    file: UploadFile = File(...),
    job_type: str = Form("image:blur"),
    params_json: str = Form("{}"),
) -> dict[str, Any]:
    """Upload a raw file to S3 and enqueue a processing job."""
    job_id = str(uuid.uuid4())
    s3 = request.app.state.s3
    cfg = request.app.state.cfg

    contents = await file.read()
    raw_key = f"uploads/{job_id}_{file.filename}"
    await s3.put_object(cfg.s3_bucket_data, raw_key, contents, content_type=file.content_type or "application/octet-stream")

    source_url = f"s3://{cfg.s3_bucket_data}/{raw_key}"
    payload = json.loads(params_json) if params_json else {}
    payload["source_url"] = source_url

    req = JobSubmitRequest(job_type=job_type, payload=payload)
    return await submit_job(req, request)


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job_status(job_id: str, request: Request) -> dict[str, Any]:
    """Retrieve the real-time processing status and result of a specific job."""
    db = request.app.state.db
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    job_data = dict(job)
    if isinstance(job_data.get("payload"), str):
        job_data["payload"] = json.loads(job_data["payload"])
    if isinstance(job_data.get("result"), str):
        job_data["result"] = json.loads(job_data["result"])
    if job_data.get("created_at"):
        job_data["created_at"] = str(job_data["created_at"])
    if job_data.get("updated_at"):
        job_data["updated_at"] = str(job_data["updated_at"])
    return job_data


@router.get("/jobs")
async def list_recent_jobs(request: Request, limit: int = 25, job_type: str | None = None) -> list[dict[str, Any]]:
    """List most recently submitted jobs, optionally filtered by job_type."""
    db = request.app.state.db
    jobs = await db.get_recent_jobs(limit=min(limit, 100), job_type=job_type)
    formatted = []
    for j in jobs:
        row = dict(j)
        if isinstance(row.get("payload"), str):
            row["payload"] = json.loads(row["payload"])
        if isinstance(row.get("result"), str):
            row["result"] = json.loads(row["result"])
        if row.get("created_at"):
            row["created_at"] = str(row["created_at"])
        if row.get("updated_at"):
            row["updated_at"] = str(row["updated_at"])
        formatted.append(row)
    return formatted


@router.get("/events")
async def sse_events(request: Request) -> StreamingResponse:
    """Server-Sent Events (SSE) stream forwarding real-time NATS events to UI."""
    cfg = request.app.state.cfg

    async def event_generator() -> AsyncGenerator[str, None]:
        nc = await nats.connect(cfg.nats_url)
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=256)

        async def on_nats_msg(msg: Any) -> None:
            try:
                data = msg.data.decode("utf-8")
                if queue.full():
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                await queue.put(data)
            except Exception:
                pass

        sub = await nc.subscribe(cfg.nats_subject_events, cb=on_nats_msg)

        try:
            yield f"data: {json.dumps({'event': 'CONNECTED'})}\n\n"
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=10.0)
                    yield f"data: {data}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            await sub.unsubscribe()
            await nc.drain()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/stats", response_model=StatsResponse)
async def get_stats(request: Request) -> dict[str, Any]:
    """Health and basic connectivity statistics."""
    db = request.app.state.db
    nc = request.app.state.nc

    recent_jobs = await db.get_recent_jobs(limit=10)

    return {
        "status": "healthy",
        "nats_connected": nc.is_connected if nc else False,
        "postgres_connected": db.pool is not None,
        "recent_jobs_count": len(recent_jobs),
        "registered_processors": registry.list_types(),
    }


@router.get("/healthz")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}
