"""Pillow-based Gaussian Blur processor."""

from __future__ import annotations

import io
from PIL import Image, ImageFilter


class PillowBlurProcessor:
    """Fast, lightweight image blur processor using Pillow."""

    def process(self, image_data: bytes, params: dict[str, int | float | str]) -> bytes:
        radius = int(params.get("radius", 5))
        with Image.open(io.BytesIO(image_data)) as img:
            # Convert palette/alpha modes to RGB/RGBA for consistent saving
            if img.mode in ("P", "1"):
                img = img.convert("RGB")
            format_to_save = img.format or "JPEG"
            blurred = img.filter(ImageFilter.GaussianBlur(radius))
            out = io.BytesIO()
            blurred.save(out, format=format_to_save)
            return out.getvalue()
