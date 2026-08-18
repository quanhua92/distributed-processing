"""Image Resize processor plugin."""

from __future__ import annotations

import io
from typing import Any
from PIL import Image

from distributed_processing.processors.base import JobContext


class ImageResizeProcessor:
    """Resizes or creates thumbnails from images."""

    async def process(self, payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        source_url = payload["source_url"]
        width = int(payload.get("width", 256))
        height = int(payload.get("height", 256))
        output_key = payload.get("output_key", f"processed/{ctx.job_id}_{width}x{height}.jpg")

        # 1. Fetch raw image
        if source_url.startswith("s3://") or not (source_url.startswith("http://") or source_url.startswith("https://")):
            key = source_url.replace(f"s3://{ctx.cfg.s3_bucket_data}/", "").lstrip("/")
            raw_bytes = await ctx.s3.get_object(ctx.cfg.s3_bucket_data, key)
        else:
            resp = await ctx.http_client.get(source_url)
            resp.raise_for_status()
            raw_bytes = resp.content

        # 2. Resize
        with Image.open(io.BytesIO(raw_bytes)) as img:
            if img.mode in ("P", "1"):
                img = img.convert("RGB")
            resized = img.resize((width, height), Image.Resampling.LANCZOS)
            out = io.BytesIO()
            resized.save(out, format="JPEG")
            processed_bytes = out.getvalue()

        # 3. Store in RustFS
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
            "width": width,
            "height": height,
        }
