"""Image Blur processor plugin."""

from __future__ import annotations

import io
from typing import Any
from PIL import Image, ImageFilter

from distributed_processing.processors.base import JobContext


class ImageBlurProcessor:
    """Processes images with Gaussian blur."""

    async def process(self, payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        source_url = payload["source_url"]
        radius = int(payload.get("radius", 5))
        output_key = payload.get("output_key", f"processed/{ctx.job_id}_blur.jpg")

        # 1. Fetch raw image
        if source_url.startswith("s3://") or not (source_url.startswith("http://") or source_url.startswith("https://")):
            key = source_url.replace(f"s3://{ctx.cfg.s3_bucket_data}/", "").lstrip("/")
            raw_bytes = await ctx.s3.get_object(ctx.cfg.s3_bucket_data, key)
        else:
            resp = await ctx.http_client.get(source_url)
            resp.raise_for_status()
            raw_bytes = resp.content

        # 2. Apply Gaussian blur with Pillow
        with Image.open(io.BytesIO(raw_bytes)) as img:
            if img.mode in ("P", "1"):
                img = img.convert("RGB")
            format_to_save = img.format or "JPEG"
            blurred = img.filter(ImageFilter.GaussianBlur(radius))
            out = io.BytesIO()
            blurred.save(out, format=format_to_save)
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
            "radius": radius,
        }
