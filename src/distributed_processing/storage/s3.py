"""Async S3 client integration using aioboto3 for RustFS."""

from __future__ import annotations

import logging
from typing import Final

import aioboto3

from distributed_processing.settings import Settings

log: Final = logging.getLogger(__name__)


class S3Storage:
    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self.session = aioboto3.Session()

    def _client_kwargs(self) -> dict[str, str]:
        return {
            "endpoint_url": self.cfg.s3_endpoint,
            "aws_access_key_id": self.cfg.s3_access_key,
            "aws_secret_access_key": self.cfg.s3_secret_key,
            "region_name": self.cfg.s3_region,
        }

    async def ensure_buckets(self) -> None:
        """Create configured S3 buckets if they do not exist."""
        async with self.session.client("s3", **self._client_kwargs()) as s3:
            for bucket in [self.cfg.s3_bucket_data, self.cfg.s3_bucket_logs]:
                try:
                    await s3.head_bucket(Bucket=bucket)
                except Exception:
                    try:
                        await s3.create_bucket(Bucket=bucket)
                        log.info("s3.bucket_created bucket=%s", bucket)
                    except Exception as err:
                        log.warning("s3.bucket_creation_error bucket=%s: %s", bucket, err)

    async def put_object(self, bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        """Upload raw bytes to S3 and return the key."""
        async with self.session.client("s3", **self._client_kwargs()) as s3:
            await s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )
            return key

    async def get_object(self, bucket: str, key: str) -> bytes:
        """Download raw bytes from S3."""
        async with self.session.client("s3", **self._client_kwargs()) as s3:
            resp = await s3.get_object(Bucket=bucket, Key=key)
            async with resp["Body"] as stream:
                return await stream.read()
