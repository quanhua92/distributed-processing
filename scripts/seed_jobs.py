"""Load generator and benchmark seed script for distributed processing."""

from __future__ import annotations

import argparse
import asyncio
import random
import time
import httpx

JOB_TYPES = [
    ("image:blur", {"source_url": "https://picsum.photos/800/600", "radius": 10}),
    ("image:grayscale", {"source_url": "https://picsum.photos/800/600"}),
    ("image:resize", {"source_url": "https://picsum.photos/1200/800", "width": 300, "height": 200}),
    ("data:transform", {"operation": "aggregate", "data": [random.uniform(1.0, 500.0) for _ in range(25)]}),
]


async def submit_single(client: httpx.AsyncClient, api_url: str, job_type: str, payload: dict) -> bool:
    try:
        resp = await client.post(
            f"{api_url}/jobs",
            json={"job_type": job_type, "payload": payload},
            timeout=10.0,
        )
        return resp.status_code == 200
    except Exception as e:
        print(f"Error submitting {job_type}: {e}")
        return False


async def seed_jobs(api_url: str, total_count: int, concurrency: int) -> None:
    print(f"Seeding {total_count} arbitrary jobs to {api_url} (concurrency={concurrency})...")
    start = time.monotonic()
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as client:
        async def worker():
            async with semaphore:
                job_type, payload = random.choice(JOB_TYPES)
                return await submit_single(client, api_url, job_type, payload)

        tasks = [asyncio.create_task(worker()) for _ in range(total_count)]
        results = await asyncio.gather(*tasks)

    duration = time.monotonic() - start
    success_count = sum(1 for r in results if r)
    ops_per_sec = total_count / duration if duration > 0 else 0

    print(f"Seeding completed in {duration:.2f}s!")
    print(f"Success: {success_count}/{total_count} ({ops_per_sec:.2f} jobs/sec)")


def main():
    parser = argparse.ArgumentParser(description="Seed distributed processing jobs")
    parser.add_argument("--api-url", default="http://localhost:8000", help="FastAPI gateway URL")
    parser.add_argument("--count", type=int, default=50, help="Total number of jobs to submit")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent submit requests")
    args = parser.parse_args()

    asyncio.run(seed_jobs(args.api_url, args.count, args.concurrency))


if __name__ == "__main__":
    main()
