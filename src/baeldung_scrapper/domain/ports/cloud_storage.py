from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol


class ArtifactKind(str, Enum):
    ARTICLE = "article"
    INDEX = "index"
    MANIFEST = "manifest"


@dataclass(frozen=True)
class ArtifactObject:
    object_path: str
    kind: ArtifactKind
    mime_type: str
    payload: bytes
    modified_at: datetime

    def __post_init__(self) -> None:
        if not self.object_path.strip():
            raise ValueError("object_path must not be empty")
        if self.object_path.startswith("/"):
            raise ValueError("object_path must be relative")
        if not self.mime_type.strip():
            raise ValueError("mime_type must not be empty")
        if not self.payload:
            raise ValueError("payload must not be empty")


@dataclass(frozen=True)
class ArtifactWriteResult:
    provider_object_id: str
    checksum_sha256: str

    def __post_init__(self) -> None:
        if not self.provider_object_id.strip():
            raise ValueError("provider_object_id must not be empty")
        if not self.checksum_sha256.strip():
            raise ValueError("checksum_sha256 must not be empty")


class CloudStorageProvider(Protocol):
    provider_name: str

    def upsert(self, *, destination_root_id: str, item: ArtifactObject) -> ArtifactWriteResult:
        """Create or update a storage object in provider cloud storage."""
        ...

    def exists(self, *, destination_root_id: str, object_path: str) -> bool:
        """Return true when object exists in provider."""
        ...

    def read(self, *, destination_root_id: str, object_path: str) -> bytes | None:
        """Read object payload or return None when object does not exist."""
        ...
