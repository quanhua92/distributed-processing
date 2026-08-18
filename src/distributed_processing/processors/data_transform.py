"""Arbitrary Data Transformation processor plugin."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from distributed_processing.processors.base import JobContext


class DataTransformProcessor:
    """Processes arbitrary JSON / numeric / text data payloads."""

    async def process(self, payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        data = payload.get("data", [])
        operation = payload.get("operation", "aggregate")

        if operation == "aggregate":
            # Compute statistical summary on numeric arrays or list of dicts
            numbers = [float(x) for x in data if isinstance(x, (int, float))]
            if not numbers:
                count = len(data)
                result_stats = {"count": count, "type": "empty_or_non_numeric"}
            else:
                result_stats = {
                    "count": len(numbers),
                    "sum": sum(numbers),
                    "avg": sum(numbers) / len(numbers),
                    "min": min(numbers),
                    "max": max(numbers),
                }

            # Generate SHA256 integrity hash
            payload_str = json.dumps(payload, sort_keys=True)
            sha256_hash = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()

            # Store result artifact in S3
            result_key = f"results/{ctx.job_id}_summary.json"
            result_bytes = json.dumps({"stats": result_stats, "hash": sha256_hash}, indent=2).encode("utf-8")
            await ctx.s3.put_object(ctx.cfg.s3_bucket_data, result_key, result_bytes, content_type="application/json")

            return {
                "operation": "aggregate",
                "stats": result_stats,
                "sha256": sha256_hash,
                "artifact_s3_url": f"s3://{ctx.cfg.s3_bucket_data}/{result_key}",
            }

        elif operation == "hash":
            text = str(payload.get("text", ""))
            return {
                "operation": "hash",
                "length": len(text),
                "md5": hashlib.md5(text.encode("utf-8")).hexdigest(),
                "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            }

        else:
            return {
                "operation": operation,
                "echo": payload,
                "status": "processed",
            }
