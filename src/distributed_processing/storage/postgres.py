"""Async PostgreSQL 18 connection pool and generic jobs repository."""

from __future__ import annotations

import json
import logging
from typing import Any, Final

import asyncpg

from distributed_processing.settings import Settings

log: Final = logging.getLogger(__name__)


class PostgresDatabase:
    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self.pool is not None:
            return
        self.pool = await asyncpg.create_pool(
            dsn=self.cfg.postgres_dsn,
            min_size=2,
            max_size=10,
        )
        log.info("postgres.pool_connected host=%s db=%s", self.cfg.postgres_host, self.cfg.postgres_db)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None
            log.info("postgres.pool_closed")

    async def create_job(
        self,
        job_id: str,
        job_type: str,
        payload: dict[str, Any],
    ) -> None:
        assert self.pool is not None
        query = """
        INSERT INTO jobs (job_id, job_type, status, payload, created_at, updated_at)
        VALUES ($1, $2, 'PENDING', $3, NOW(), NOW())
        ON CONFLICT (job_id) DO NOTHING;
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, job_id, job_type, json.dumps(payload))

    async def update_job_status(
        self,
        job_id: str,
        status: str,
        *,
        worker_id: str | None = None,
        result: dict[str, Any] | None = None,
        duration_ms: int | None = None,
        error_message: str | None = None,
    ) -> None:
        assert self.pool is not None
        query = """
        UPDATE jobs
        SET
            status = $2,
            worker_id = COALESCE($3, worker_id),
            result = CASE WHEN $4::text IS NOT NULL THEN $4::jsonb ELSE result END,
            duration_ms = COALESCE($5, duration_ms),
            error_message = COALESCE($6, error_message),
            updated_at = NOW()
        WHERE job_id = $1;
        """
        result_json = json.dumps(result) if result is not None else None
        async with self.pool.acquire() as conn:
            await conn.execute(query, job_id, status, worker_id, result_json, duration_ms, error_message)

    async def update_log_archive_key(self, job_ids: list[str], s3_log_key: str) -> None:
        assert self.pool is not None
        query = """
        UPDATE jobs
        SET log_archive_s3_key = $2, updated_at = NOW()
        WHERE job_id = ANY($1::uuid[]);
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, job_ids, s3_log_key)

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        assert self.pool is not None
        query = """
        SELECT
            job_id::text,
            job_type,
            status,
            payload,
            result,
            worker_id,
            duration_ms,
            error_message,
            log_archive_s3_key,
            retry_count,
            created_at,
            updated_at
        FROM jobs
        WHERE job_id = $1;
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, job_id)
            if not row:
                return None
            return dict(row)

    async def get_recent_jobs(self, limit: int = 25, job_type: str | None = None) -> list[dict[str, Any]]:
        assert self.pool is not None
        if job_type:
            query = """
            SELECT
                job_id::text,
                job_type,
                status,
                payload,
                result,
                worker_id,
                duration_ms,
                error_message,
                log_archive_s3_key,
                created_at,
                updated_at
            FROM jobs
            WHERE job_type = $2
            ORDER BY created_at DESC
            LIMIT $1;
            """
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, limit, job_type)
                return [dict(r) for r in rows]
        else:
            query = """
            SELECT
                job_id::text,
                job_type,
                status,
                payload,
                result,
                worker_id,
                duration_ms,
                error_message,
                log_archive_s3_key,
                created_at,
                updated_at
            FROM jobs
            ORDER BY created_at DESC
            LIMIT $1;
            """
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, limit)
                return [dict(r) for r in rows]

    async def get_status_counts(self) -> dict[str, int]:
        assert self.pool is not None
        query = "SELECT status, count(*) as count FROM jobs GROUP BY status;"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            return {r["status"]: int(r["count"]) for r in rows}
