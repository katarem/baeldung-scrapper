from __future__ import annotations

from typing import Any

from baeldung_scrapper.config.settings import AppSettings
from baeldung_scrapper.infrastructure.cloud_storage.factory import build_cloud_storage_provider
from baeldung_scrapper.infrastructure.cloud_storage.s3 import S3Object


class _NoopS3Client:
    def get_object(self, *, bucket: str, object_key: str) -> bytes | None:
        _ = (bucket, object_key)
        return None

    def head_object(self, *, bucket: str, object_key: str) -> S3Object | None:
        _ = (bucket, object_key)
        return None

    def put_object(
        self,
        *,
        bucket: str,
        object_key: str,
        mime_type: str,
        payload: bytes,
        checksum_sha256: str,
        acl: str | None = None,
    ) -> S3Object:
        _ = (bucket, object_key, mime_type, payload, checksum_sha256, acl)
        return S3Object(object_key=object_key, checksum_sha256=checksum_sha256)


def _minimal_settings() -> dict[str, Any]:
    return {
        "s3_endpoint": "https://bucket.up.railway.app",
        "s3_region": "us-east-1",
        "s3_bucket": "railway-bucket",
        "s3_access_key_id": "access-key",
        "s3_secret_access_key": "secret-key",
    }


def test_resolve_destination_root_id_returns_bucket() -> None:
    settings = AppSettings(**_minimal_settings())

    assert settings.resolve_destination_root_id() == "railway-bucket"


def test_factory_builds_s3_runtime_binding() -> None:
    settings = AppSettings(**_minimal_settings())
    binding = build_cloud_storage_provider(settings, s3_client=_NoopS3Client())

    assert binding.provider.provider_name == "s3"
    assert binding.destination_root_id == "railway-bucket"


def test_factory_builds_local_runtime_binding() -> None:
    settings = AppSettings(storage_backend="local", local_base_directory="./tmp/baeldung-local")
    binding = build_cloud_storage_provider(settings)

    assert binding.provider.provider_name == "local"
    assert binding.destination_root_id == "local-filesystem"
