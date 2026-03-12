from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class Article:
    source_id: str
    title: str
    url: str
    category: str
    published_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        if not self.source_id.strip():
            raise ValueError("source_id must not be empty")
        if not self.title.strip():
            raise ValueError("title must not be empty")
        if not self.url.startswith(("http://", "https://")):
            raise ValueError("url must be an absolute http(s) URL")
        if not self.category.strip():
            raise ValueError("category must not be empty")

        normalized_published = self._to_utc(self.published_at)
        normalized_updated = self._to_utc(self.updated_at)

        if normalized_published and normalized_updated:
            if normalized_updated < normalized_published:
                raise ValueError("updated_at must be >= published_at")

        object.__setattr__(self, "published_at", normalized_published)
        object.__setattr__(self, "updated_at", normalized_updated)

    @staticmethod
    def _to_utc(value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
