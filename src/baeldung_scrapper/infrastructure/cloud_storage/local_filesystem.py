from __future__ import annotations

import hashlib
from pathlib import Path

from baeldung_scrapper.domain.models.storage_layout import normalize_relative_path, normalize_storage_root
from baeldung_scrapper.domain.ports.cloud_storage import ArtifactObject, ArtifactWriteResult, CloudStorageProvider


class LocalStorageAdapterError(RuntimeError):
    """Raised when the local filesystem adapter cannot complete an operation."""


class LocalFilesystemStorageAdapter(CloudStorageProvider):
    provider_name = "local"

    def __init__(self, *, base_directory: str, destination_folder_path: str) -> None:
        normalized_base = base_directory.strip()
        if not normalized_base:
            raise ValueError("base_directory must not be empty")

        self._base_directory = Path(normalized_base).expanduser().resolve()
        self._destination_folder_path = normalize_storage_root(destination_folder_path)

    def upsert(self, *, destination_root_id: str, item: ArtifactObject) -> ArtifactWriteResult:
        object_path, target = self._resolve_target_path(
            destination_root_id=destination_root_id,
            object_path=item.object_path,
        )
        checksum = hashlib.sha256(item.payload).hexdigest()

        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                existing_payload = target.read_bytes()
                if hashlib.sha256(existing_payload).hexdigest() == checksum:
                    return ArtifactWriteResult(
                        provider_object_id=str(target),
                        checksum_sha256=checksum,
                    )

            target.write_bytes(item.payload)
        except Exception as exc:
            raise LocalStorageAdapterError("Failed to upsert object to local filesystem") from exc

        return ArtifactWriteResult(provider_object_id=str(target), checksum_sha256=checksum)

    def exists(self, *, destination_root_id: str, object_path: str) -> bool:
        _, target = self._resolve_target_path(
            destination_root_id=destination_root_id,
            object_path=object_path,
        )
        return target.is_file()

    def read(self, *, destination_root_id: str, object_path: str) -> bytes | None:
        _, target = self._resolve_target_path(
            destination_root_id=destination_root_id,
            object_path=object_path,
        )
        try:
            if not target.is_file():
                return None
            return target.read_bytes()
        except Exception as exc:
            raise LocalStorageAdapterError("Failed to read object from local filesystem") from exc

    def _resolve_target_path(self, *, destination_root_id: str, object_path: str) -> tuple[str, Path]:
        if not destination_root_id.strip():
            raise ValueError("destination_root_id must not be empty")

        normalized_path = normalize_relative_path(object_path)
        prefix = self._destination_folder_path
        if normalized_path == prefix:
            raise ValueError("object_path must include a file segment")

        prefixed = f"{prefix}/"
        if not normalized_path.startswith(prefixed):
            raise LocalStorageAdapterError(
                f"object_path '{normalized_path}' must start with destination_folder_path '{prefix}'"
            )

        target = (self._base_directory / normalized_path).resolve()
        if self._base_directory not in target.parents:
            raise LocalStorageAdapterError("object_path resolves outside local base directory")
        return normalized_path, target
