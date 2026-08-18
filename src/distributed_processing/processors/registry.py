"""Processor Registry mapping job types to processor instances."""

from __future__ import annotations

import logging
from typing import Final

from distributed_processing.processors.base import JobProcessor
from distributed_processing.processors.data_transform import DataTransformProcessor
from distributed_processing.processors.image_blur import ImageBlurProcessor
from distributed_processing.processors.image_grayscale import ImageGrayscaleProcessor
from distributed_processing.processors.image_resize import ImageResizeProcessor

log: Final = logging.getLogger(__name__)


class ProcessorRegistry:
    def __init__(self) -> None:
        self._processors: dict[str, JobProcessor] = {}

    def register(self, job_type: str, processor: JobProcessor) -> None:
        self._processors[job_type] = processor
        log.info("processor.registered type=%s", job_type)

    def get(self, job_type: str) -> JobProcessor | None:
        return self._processors.get(job_type)

    def list_types(self) -> list[str]:
        return sorted(self._processors.keys())


def create_default_registry() -> ProcessorRegistry:
    reg = ProcessorRegistry()
    reg.register("image:blur", ImageBlurProcessor())
    reg.register("image:grayscale", ImageGrayscaleProcessor())
    reg.register("image:resize", ImageResizeProcessor())
    reg.register("data:transform", DataTransformProcessor())
    return reg
