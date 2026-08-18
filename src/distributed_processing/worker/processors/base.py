"""Image processor protocol definition."""

from __future__ import annotations

from typing import Protocol


class ImageProcessor(Protocol):
    def process(self, image_data: bytes, params: dict[str, int | float | str]) -> bytes:
        """Transform raw input image bytes and return transformed image bytes."""
        ...
