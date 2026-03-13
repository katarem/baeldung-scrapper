from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal
from typing import Any, Protocol

from baeldung_scrapper.domain.models.storage_layout import normalize_relative_path, normalize_storage_root
from baeldung_scrapper.domain.ports.cloud_storage import ArtifactObject, ArtifactWriteResult, CloudStorageProvider


class CloudStorageAdapterError(RuntimeError):
    """Raised when the cloud provider adapter cannot complete an operation."""


@dataclass(frozen=True)
class S3Object:
    object_key: str
    checksum_sha256: str | None = None


class S3Client(Protocol):
    def get_object(self, *, bucket: str, object_key: str) -> bytes | None:
        ...

    def head_object(self, *, bucket: str, object_key: str) -> S3Object | None:
        ...

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
        ...


class Boto3S3ClientAdapter(S3Client):
    def __init__(self, *, client: Any) -> None:
        self._client = client

    def get_object(self, *, bucket: str, object_key: str) -> bytes | None:
        try:
            response = self._client.get_object(Bucket=bucket, Key=object_key)
        except Exception as exc:
            if _is_not_found_error(exc):
                return None
            raise

        body = response["Body"]
        return body.read()

    def head_object(self, *, bucket: str, object_key: str) -> S3Object | None:
        try:
            response = self._client.head_object(Bucket=bucket, Key=object_key)
        except Exception as exc:
            if _is_not_found_error(exc):
                return None
            raise

        metadata = response.get("Metadata") or {}
        checksum = metadata.get("checksum_sha256")
        return S3Object(object_key=object_key, checksum_sha256=checksum)

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
        put_args: dict[str, Any] = {
            "Bucket": bucket,
            "Key": object_key,
            "Body": payload,
            "ContentType": mime_type,
            "Metadata": {"checksum_sha256": checksum_sha256},
        }
        if acl is not None:
            put_args["ACL"] = acl

        self._client.put_object(
            **put_args,
        )
        return S3Object(object_key=object_key, checksum_sha256=checksum_sha256)


class S3StorageAdapter(CloudStorageProvider):
    provider_name = "s3"

    def __init__(
        self,
        *,
        client: S3Client,
        destination_folder_path: str,
        object_acl: Literal["private", "public-read"] = "private",
    ) -> None:
        self._client = client
        self._destination_folder_path = normalize_storage_root(destination_folder_path)
        self._object_acl = object_acl

    def upsert(self, *, destination_root_id: str, item: ArtifactObject) -> ArtifactWriteResult:
        bucket, object_key = self._resolve_bucket_and_key(
            destination_root_id=destination_root_id,
            object_path=item.object_path,
        )
        checksum = hashlib.sha256(item.payload).hexdigest()

        try:
            existing = self._client.head_object(bucket=bucket, object_key=object_key)
            if existing is not None:
                existing_checksum = existing.checksum_sha256
                if existing_checksum is None:
                    existing_payload = self._client.get_object(bucket=bucket, object_key=object_key)
                    if existing_payload is None:
                        raise CloudStorageAdapterError(
                            "Object disappeared between head and get operations"
                        )
                    existing_checksum = hashlib.sha256(existing_payload).hexdigest()

                if existing_checksum == checksum:
                    return ArtifactWriteResult(
                        provider_object_id=f"{bucket}/{object_key}",
                        checksum_sha256=checksum,
                    )

            written = self._client.put_object(
                bucket=bucket,
                object_key=object_key,
                mime_type=item.mime_type,
                payload=item.payload,
                checksum_sha256=checksum,
                acl=self._object_acl,
            )
        except CloudStorageAdapterError:
            raise
        except Exception as exc:
            raise CloudStorageAdapterError("Failed to upsert object to S3-compatible storage") from exc

        return ArtifactWriteResult(
            provider_object_id=f"{bucket}/{written.object_key}",
            checksum_sha256=checksum,
        )

    def exists(self, *, destination_root_id: str, object_path: str) -> bool:
        bucket, object_key = self._resolve_bucket_and_key(
            destination_root_id=destination_root_id,
            object_path=object_path,
        )
        try:
            return self._client.head_object(bucket=bucket, object_key=object_key) is not None
        except Exception as exc:
            raise CloudStorageAdapterError("Failed to check object existence in S3-compatible storage") from exc

    def read(self, *, destination_root_id: str, object_path: str) -> bytes | None:
        bucket, object_key = self._resolve_bucket_and_key(
            destination_root_id=destination_root_id,
            object_path=object_path,
        )
        try:
            return self._client.get_object(bucket=bucket, object_key=object_key)
        except Exception as exc:
            raise CloudStorageAdapterError("Failed to read object from S3-compatible storage") from exc

    def _resolve_bucket_and_key(self, *, destination_root_id: str, object_path: str) -> tuple[str, str]:
        bucket = destination_root_id.strip()
        if not bucket:
            raise ValueError("destination_root_id must not be empty")

        normalized_path = normalize_relative_path(object_path)
        prefix = self._destination_folder_path
        if normalized_path == prefix:
            raise ValueError("object_path must include a file segment")

        prefixed = f"{prefix}/"
        if not normalized_path.startswith(prefixed):
            raise CloudStorageAdapterError(
                f"object_path '{normalized_path}' must start with destination_folder_path '{prefix}'"
            )
        return bucket, normalized_path


def build_boto3_s3_client(
    *,
    endpoint: str,
    region: str,
    access_key_id: str,
    secret_access_key: str,
    force_path_style: bool,
) -> S3Client:
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise RuntimeError("boto3 is required for runtime S3 storage. Install project dependencies.") from exc

    addressing_style = "path" if force_path_style else "virtual"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=Config(s3={"addressing_style": addressing_style}),
    )
    return Boto3S3ClientAdapter(client=client)


def _is_not_found_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    error = response.get("Error")
    if not isinstance(error, dict):
        return False
    code = str(error.get("Code", "")).strip()
    return code in {"404", "NoSuchKey", "NotFound"}
