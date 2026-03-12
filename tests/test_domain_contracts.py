from __future__ import annotations

from datetime import datetime, timezone

import pytest

from baeldung_scrapper.domain.models.article import Article
from baeldung_scrapper.domain.ports.cloud_storage import (
    ArtifactKind,
    ArtifactObject,
    ArtifactWriteResult,
    CloudStorageProvider,
)


def test_article_rejects_updated_before_published() -> None:
    with pytest.raises(ValueError, match="updated_at"):
        Article(
            source_id="a-1",
            title="Article",
            url="https://www.baeldung.com/java-streams",
            category="java",
            published_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            updated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


def test_article_normalizes_naive_datetimes_to_utc() -> None:
    article = Article(
        source_id="a-1",
        title="Article",
        url="https://www.baeldung.com/java-streams",
        category="java",
        published_at=datetime(2026, 1, 2, 3, 0, 0),
    )
    assert article.published_at is not None
    assert article.published_at.tzinfo == timezone.utc


def test_artifact_object_rejects_empty_payload() -> None:
    with pytest.raises(ValueError, match="payload"):
        ArtifactObject(
            object_path="java/article-1.json",
            kind=ArtifactKind.ARTICLE,
            mime_type="application/json",
            payload=b"",
            modified_at=datetime.now(timezone.utc),
        )


def test_cloud_storage_provider_protocol_shape() -> None:
    class FakeProvider:
        provider_name = "s3"

        def upsert(self, *, destination_root_id: str, item: ArtifactObject) -> ArtifactWriteResult:
            return ArtifactWriteResult(provider_object_id="obj-1", checksum_sha256="abc123")

        def exists(self, *, destination_root_id: str, object_path: str) -> bool:
            return False

        def read(self, *, destination_root_id: str, object_path: str) -> bytes | None:
            return None

    provider: CloudStorageProvider = FakeProvider()
    result = provider.upsert(
        destination_root_id="folder-id",
        item=ArtifactObject(
            object_path="java/article-1.json",
            kind=ArtifactKind.ARTICLE,
            mime_type="application/json",
            payload=b"{}",
            modified_at=datetime.now(timezone.utc),
        ),
    )
    assert result.provider_object_id == "obj-1"
