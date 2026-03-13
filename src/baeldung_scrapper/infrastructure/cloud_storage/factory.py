from __future__ import annotations

from dataclasses import dataclass

from baeldung_scrapper.config.settings import AppSettings
from baeldung_scrapper.domain.ports.cloud_storage import CloudStorageProvider
from baeldung_scrapper.infrastructure.cloud_storage.local_filesystem import LocalFilesystemStorageAdapter
from baeldung_scrapper.infrastructure.cloud_storage.s3 import S3Client, S3StorageAdapter, build_boto3_s3_client


@dataclass(frozen=True)
class ProviderRuntimeBinding:
    provider: CloudStorageProvider
    destination_root_id: str


def build_cloud_storage_provider(
    settings: AppSettings,
    *,
    s3_client: S3Client | None = None,
) -> ProviderRuntimeBinding:
    destination_root_id = settings.resolve_destination_root_id()

    if settings.storage_backend == "local":
        if settings.local_base_directory is None:
            raise ValueError("local_base_directory is required when storage_backend=local")

        provider = LocalFilesystemStorageAdapter(
            base_directory=settings.local_base_directory,
            destination_folder_path=settings.destination_folder_path,
        )
        return ProviderRuntimeBinding(provider=provider, destination_root_id=destination_root_id)

    provider = S3StorageAdapter(
        client=s3_client
        or build_boto3_s3_client(
            endpoint=_required_setting(settings.s3_endpoint, "s3_endpoint"),
            region=_required_setting(settings.s3_region, "s3_region"),
            access_key_id=_required_setting(settings.s3_access_key_id, "s3_access_key_id"),
            secret_access_key=_required_setting(settings.s3_secret_access_key, "s3_secret_access_key"),
            force_path_style=settings.s3_force_path_style,
        ),
        destination_folder_path=settings.destination_folder_path,
        object_acl=settings.s3_object_acl,
    )
    return ProviderRuntimeBinding(provider=provider, destination_root_id=destination_root_id)


def _required_setting(value: str | None, name: str) -> str:
    if value is None:
        raise ValueError(f"{name} is required")
    return value
