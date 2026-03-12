from __future__ import annotations

import json
from datetime import datetime, timezone

from baeldung_scrapper.domain.models.normalized_article import NormalizedArticleArtifact, NormalizedBodyBlock


def serialize_normalized_article(artifact: NormalizedArticleArtifact) -> bytes:
    payload = {
        "schema_version": "1.0",
        "source_url": artifact.source_url,
        "title": artifact.title,
        "published_at": _serialize_datetime(artifact.published_at),
        "updated_at": _serialize_datetime(artifact.updated_at),
        "author": artifact.author,
        "reviewer": artifact.reviewer,
        "tags": list(artifact.tags),
        "body_blocks": [_serialize_body_block(block) for block in artifact.body_blocks],
        "validation_payload": {
            "source_code_blocks": artifact.validation_payload.source_code_blocks,
            "extracted_code_blocks": artifact.validation_payload.extracted_code_blocks,
            "source_images": artifact.validation_payload.source_images,
            "extracted_images": artifact.validation_payload.extracted_images,
        },
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def serialize_article_index(*, source_id: str, article_url: str, article_path: str, generated_at: datetime) -> bytes:
    payload = {
        "schema_version": "1.0",
        "source_id": source_id,
        "article_url": article_url,
        "article_path": article_path,
        "generated_at": _serialize_datetime(generated_at),
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _serialize_body_block(block: NormalizedBodyBlock) -> dict[str, object]:
    return {
        "kind": block.kind,
        "text": block.text,
        "level": block.level,
        "items": list(block.items),
        "html": block.html,
        "language": block.language,
        "src": block.src,
        "alt": block.alt,
        "caption": block.caption,
    }


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()
