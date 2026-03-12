from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from baeldung_scrapper.domain.models.artifact_manifest import ArtifactManifest, ManifestEntry


def _entry(*, source_id: str, url: str) -> ManifestEntry:
    return ManifestEntry(
        source_id=source_id,
        article_url=url,
        article_path=f"team/baeldung/articles/java/{source_id}.json",
        index_path=f"team/baeldung/indexes/by-url/{source_id}.json",
        content_sha256="a" * 64,
        last_seen_at=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
    )


def test_manifest_sorts_entries_for_deterministic_output() -> None:
    manifest = ArtifactManifest(
        generated_at=datetime(2026, 3, 12, 12, 0, tzinfo=timezone.utc),
        entries=(
            _entry(source_id="2", url="https://www.baeldung.com/zeta"),
            _entry(source_id="1", url="https://www.baeldung.com/alpha"),
        ),
    )

    data = json.loads(manifest.to_json_bytes().decode("utf-8"))

    assert [entry["article_url"] for entry in data["entries"]] == [
        "https://www.baeldung.com/alpha",
        "https://www.baeldung.com/zeta",
    ]


def test_manifest_rejects_duplicate_article_urls() -> None:
    with pytest.raises(ValueError, match="unique article_url"):
        ArtifactManifest(
            generated_at=datetime(2026, 3, 12, 12, 0, tzinfo=timezone.utc),
            entries=(
                _entry(source_id="1", url="https://www.baeldung.com/alpha"),
                _entry(source_id="2", url="https://www.baeldung.com/alpha"),
            ),
        )
