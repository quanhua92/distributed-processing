"""Image Grayscale processor plugin."""

from __future__ import annotations

import io
from typing import Any
from PIL import Image, ImageOps

from distributed_processing.processors.base import JobContext


class ImageGrayscaleProcessor:
    """Converts images to grayscale."""

    async def process(self, payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        source_url = payload["source_url"]
        output_key = payload.get("output_key", f"processed/{ctx.job_id}_gray.jpg")

        # 1. Fetch raw image
        if source_url.startswith("s3://") or not (source_url.startswith("http://") or source_url.startswith("https://")):
            key = source_url.replace(f"s3://{ctx.cfg.s3_bucket_data}/", "").lstrip("/")
            raw_bytes = await ctx.s3.get_object(ctx.cfg.s3_bucket_data, key)
        else:
            resp = await ctx.http_client.get(source_url)
            resp.raise_for_status()
            raw_bytes = resp.content

        # 2. Convert to Grayscale
        with Image.open(io.BytesIO(raw_bytes)) as img:
            gray = ImageOps.grayscale(img)
            out = io.BytesIO()
            gray.save(out, format="JPEG")
            processed_bytes = out.getvalue()

        # 3. Store processed image in RustFS
        saved_key = await ctx.s3.put_object(
            ctx.cfg.s3_bucket_data,
            output_key,
            processed_bytes,
            content_type="image/jpeg",
        )

        return {
            "output_url": f"s3://{ctx.cfg.s3_bucket_data}/{saved_key}",
            "bytes_processed": len(raw_bytes),
            "output_bytes": len(processed_bytes),
            "mode": "grayscale",
        }
