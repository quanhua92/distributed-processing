"""Load generator and benchmark seed script for distributed processing."""

from __future__ import annotations

import argparse
import asyncio
import random
import time
import httpx

IMAGE_JOBS = [
    ("image:blur", {"source_url": "https://picsum.photos/600/400", "radius": 10}),
    ("image:grayscale", {"source_url": "https://picsum.photos/600/400"}),
    ("image:resize", {"source_url": "https://picsum.photos/600/400", "width": 300, "height": 200}),
]

DATA_JOBS = [
    ("data:transform", {"operation": "aggregate", "data": [random.uniform(1.0, 500.0) for _ in range(25)]}),
]

ALL_JOBS = IMAGE_JOBS + DATA_JOBS


async def submit_single(client: httpx.AsyncClient, api_url: str, job_type: str, payload: dict) -> bool:
    try:
        resp = await client.post(
            f"{api_url}/jobs",
            json={"job_type": job_type, "payload": payload},
            timeout=15.0,
        )
        return resp.status_code == 200
    except Exception as e:
        print(f"Error submitting {job_type}: {e}")
        return False


async def seed_jobs(api_url: str, total_count: int, concurrency: int, job_category: str = "image") -> None:
    job_pool = IMAGE_JOBS if job_category == "image" else (DATA_JOBS if job_category == "data" else ALL_JOBS)
    print(f"Seeding {total_count} '{job_category}' jobs to {api_url} (concurrency={concurrency})...")
    start = time.monotonic()
    semaphore = asyncio.Semaphore(concurrency)

    limits = httpx.Limits(max_keepalive_connections=concurrency, max_connections=concurrency * 2)
    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        async def worker():
            async with semaphore:
                job_type, payload = random.choice(job_pool)
                return await submit_single(client, api_url, job_type, payload)

        tasks = [asyncio.create_task(worker()) for _ in range(total_count)]
        results = await asyncio.gather(*tasks)

    duration = time.monotonic() - start
    success_count = sum(1 for r in results if r)
    ops_per_sec = total_count / duration if duration > 0 else 0

    print(f"Seeding completed in {duration:.2f}s!")
    print(f"Success: {success_count}/{total_count} ({ops_per_sec:.2f} jobs/sec)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed distributed processing jobs")
    parser.add_argument("--api-url", default="http://localhost:8000", help="FastAPI gateway URL")
    parser.add_argument("--count", type=int, default=50, help="Total number of jobs to submit")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent submit requests")
    parser.add_argument("--category", choices=["image", "data", "all"], default="image", help="Category of jobs to submit")
    args = parser.parse_args()

    asyncio.run(seed_jobs(args.api_url, args.count, args.concurrency, args.category))


if __name__ == "__main__":
    main()
