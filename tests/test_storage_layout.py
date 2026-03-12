from __future__ import annotations

import hashlib

import pytest

from baeldung_scrapper.domain.models.article import Article
from baeldung_scrapper.domain.models.storage_layout import (
    build_article_artifact_path,
    build_article_index_path,
    build_manifest_path,
)


def test_build_article_artifact_path_is_deterministic() -> None:
    article = Article(
        source_id="Core Java 17",
        title="Article",
        url="https://www.baeldung.com/java-streams",
        category="Java Core",
    )

    path = build_article_artifact_path(root_prefix="team/baeldung", article=article)

    assert path == "team/baeldung/articles/java-core/core-java-17.json"


def test_build_article_index_path_hashes_article_url() -> None:
    article_url = "https://www.baeldung.com/java-streams"

    path = build_article_index_path(root_prefix="team/baeldung", article_url=article_url)

    assert path.endswith(f"{hashlib.sha256(article_url.encode('utf-8')).hexdigest()}.json")
    assert path.startswith("team/baeldung/indexes/by-url/")


def test_build_manifest_path_rejects_parent_segments() -> None:
    with pytest.raises(ValueError, match=r"\.\."):
        build_manifest_path(root_prefix="team/baeldung", manifest_relative_path="../manifest.json")


def test_build_paths_reject_absolute_destination_root() -> None:
    article = Article(
        source_id="Core Java 17",
        title="Article",
        url="https://www.baeldung.com/java-streams",
        category="Java Core",
    )

    with pytest.raises(ValueError, match="relative path"):
        build_article_artifact_path(root_prefix="/team/baeldung", article=article)


def test_build_manifest_path_rejects_absolute_manifest_path() -> None:
    with pytest.raises(ValueError, match="relative"):
        build_manifest_path(root_prefix="team/baeldung", manifest_relative_path="/manifests/latest.json")
