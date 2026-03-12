from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class ManifestEntry:
    source_id: str
    article_url: str
    article_path: str
    index_path: str
    content_sha256: str
    last_seen_at: datetime

    def __post_init__(self) -> None:
        if not self.source_id.strip():
            raise ValueError("source_id must not be empty")
        if not self.article_url.startswith(("http://", "https://")):
            raise ValueError("article_url must be an absolute http(s) URL")
        if not self.article_path.strip():
            raise ValueError("article_path must not be empty")
        if not self.index_path.strip():
            raise ValueError("index_path must not be empty")
        if len(self.content_sha256.strip()) != 64:
            raise ValueError("content_sha256 must be a 64-char sha256 hex digest")

        normalized_last_seen = self._to_utc(self.last_seen_at)
        object.__setattr__(self, "last_seen_at", normalized_last_seen)

    @staticmethod
    def _to_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class ArtifactManifest:
    generated_at: datetime
    entries: tuple[ManifestEntry, ...]
    schema_version: str = "1.0"

    def __post_init__(self) -> None:
        normalized_generated_at = self._to_utc(self.generated_at)
        object.__setattr__(self, "generated_at", normalized_generated_at)

        sorted_entries = tuple(sorted(self.entries, key=lambda entry: entry.article_url))
        if len({entry.article_url for entry in sorted_entries}) != len(sorted_entries):
            raise ValueError("manifest entries must have unique article_url")
        object.__setattr__(self, "entries", sorted_entries)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "entries": [
                {
                    "source_id": entry.source_id,
                    "article_url": entry.article_url,
                    "article_path": entry.article_path,
                    "index_path": entry.index_path,
                    "content_sha256": entry.content_sha256,
                    "last_seen_at": entry.last_seen_at.isoformat(),
                }
                for entry in self.entries
            ],
        }

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True).encode("utf-8")

    @staticmethod
    def _to_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
