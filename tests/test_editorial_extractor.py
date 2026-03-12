from __future__ import annotations

from datetime import timezone
from pathlib import Path

from baeldung_scrapper.extraction.editorial_extractor import extract_article_from_html


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "articles"


def _fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def test_standard_article_extracts_editorial_blocks_and_metadata() -> None:
    result = extract_article_from_html(
        html=_fixture("standard_article.html"),
        source_url="https://www.baeldung.com/java-streams",
    )

    artifact = result.artifact

    assert artifact.title == "Understanding Java Streams"
    assert artifact.published_at is not None
    assert artifact.updated_at is not None
    assert artifact.published_at.tzinfo == timezone.utc
    assert artifact.updated_at.tzinfo == timezone.utc
    assert artifact.author == "Ana Writer"
    assert artifact.reviewer == "Leo Reviewer"
    assert artifact.tags == ("Java", "Streams", "Baeldung")
    assert any(block.kind == "code" and "Stream.of" in (block.text or "") for block in artifact.body_blocks)
    assert any(block.kind == "image" and block.src for block in artifact.body_blocks)
    assert result.validation.is_valid


def test_noisy_article_excludes_ad_like_nodes_and_keeps_protected_blocks() -> None:
    result = extract_article_from_html(
        html=_fixture("noisy_article.html"),
        source_url="https://www.baeldung.com/noisy-layout",
    )

    all_text = " ".join((block.text or "") for block in result.artifact.body_blocks)

    assert "Buy our premium plan" not in all_text
    assert "Subscribe for updates" not in all_text
    assert any(block.kind == "code" and "int sum = 1 + 2" in (block.text or "") for block in result.artifact.body_blocks)
    assert result.validation.is_valid


def test_missing_optional_metadata_produces_warnings_not_errors() -> None:
    result = extract_article_from_html(
        html=_fixture("missing_metadata_article.html"),
        source_url="https://www.baeldung.com/metadata-free",
    )

    warnings = set(result.validation.warnings)

    assert result.validation.is_valid
    assert "published_at missing" in warnings
    assert "updated_at missing" in warnings
    assert "author missing" in warnings
    assert "reviewer missing" in warnings
    assert "tags missing" in warnings


def test_edge_case_flags_code_and_image_preservation_failures() -> None:
    result = extract_article_from_html(
        html=_fixture("preservation_edge_case.html"),
        source_url="https://www.baeldung.com/preservation-edge",
    )

    errors = set(result.validation.errors)

    assert not result.validation.is_valid
    assert "code block preservation check failed" in errors
    assert "image preservation check failed" in errors
