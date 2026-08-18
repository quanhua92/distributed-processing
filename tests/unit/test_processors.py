"""Unit tests for pluggable job processors."""

import io
import pytest
from PIL import Image
from distributed_processing.processors.data_transform import DataTransformProcessor
from distributed_processing.processors.image_blur import ImageBlurProcessor
from distributed_processing.processors.image_grayscale import ImageGrayscaleProcessor
from distributed_processing.processors.image_resize import ImageResizeProcessor
from distributed_processing.processors.base import JobContext
from distributed_processing.settings import Settings


class FakeS3:
    def __init__(self):
        self.objects = {}

    async def put_object(self, bucket, key, data, content_type="application/octet-stream"):
        self.objects[(bucket, key)] = data
        return key

    async def get_object(self, bucket, key):
        return self.objects[(bucket, key)]


def create_sample_image_bytes() -> bytes:
    img = Image.new("RGB", (100, 100), color="red")
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


@pytest.mark.asyncio
async def test_data_transform_processor():
    processor = DataTransformProcessor()
    fake_s3 = FakeS3()
    ctx = JobContext(
        job_id="test-job-1",
        worker_id="test-worker",
        cfg=Settings(),
        s3=fake_s3,
        http_client=None,
    )

    payload = {
        "operation": "aggregate",
        "data": [10.0, 20.0, 30.0, 40.0],
    }

    result = await processor.process(payload, ctx)
    assert result["operation"] == "aggregate"
    assert result["stats"]["count"] == 4
    assert result["stats"]["sum"] == 100.0
    assert result["stats"]["avg"] == 25.0
    assert "sha256" in result
    assert ("processing-data", "results/test-job-1_summary.json") in fake_s3.objects


@pytest.mark.asyncio
async def test_image_blur_processor():
    processor = ImageBlurProcessor()
    fake_s3 = FakeS3()
    sample_bytes = create_sample_image_bytes()
    fake_s3.objects[("processing-data", "raw/test.jpg")] = sample_bytes

    ctx = JobContext(
        job_id="test-job-2",
        worker_id="test-worker",
        cfg=Settings(),
        s3=fake_s3,
        http_client=None,
    )

    payload = {
        "source_url": "s3://processing-data/raw/test.jpg",
        "radius": 5,
    }

    result = await processor.process(payload, ctx)
    assert "s3://processing-data/" in result["output_url"]
    assert result["radius"] == 5
    assert result["output_bytes"] > 0
