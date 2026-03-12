from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class NormalizedBodyBlock:
    kind: str
    text: Optional[str] = None
    level: Optional[int] = None
    items: tuple[str, ...] = ()
    html: Optional[str] = None
    language: Optional[str] = None
    src: Optional[str] = None
    alt: Optional[str] = None
    caption: Optional[str] = None


@dataclass(frozen=True)
class ValidationPayload:
    source_code_blocks: int
    extracted_code_blocks: int
    source_images: int
    extracted_images: int


@dataclass(frozen=True)
class ValidationResult:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0


@dataclass(frozen=True)
class NormalizedArticleArtifact:
    source_url: str
    title: str
    body_blocks: tuple[NormalizedBodyBlock, ...]
    published_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    author: Optional[str] = None
    reviewer: Optional[str] = None
    tags: tuple[str, ...] = ()
    validation_payload: ValidationPayload = ValidationPayload(
        source_code_blocks=0,
        extracted_code_blocks=0,
        source_images=0,
        extracted_images=0,
    )

    def __post_init__(self) -> None:
        if not self.source_url.startswith(("http://", "https://")):
            raise ValueError("source_url must be an absolute http(s) URL")

        object.__setattr__(self, "published_at", _to_utc(self.published_at))
        object.__setattr__(self, "updated_at", _to_utc(self.updated_at))


def _to_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
