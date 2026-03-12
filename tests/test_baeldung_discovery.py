from __future__ import annotations

import logging
from pathlib import Path

from baeldung_scrapper.fetching.baeldung_discovery import discover_java_article_urls
from baeldung_scrapper.fetching.http_client import FetchError


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "discovery"


def _fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


class FakeHttpClient:
    def __init__(self, payloads: dict[str, str]) -> None:
        self._payloads = payloads

    def get_text(self, *, url: str, timeout_seconds: float) -> str:
        _ = timeout_seconds
        payload = self._payloads.get(url)
        if payload is None:
            raise FetchError(f"failed to fetch url: {url}")
        return payload


def test_discover_java_article_urls_uses_sitemaps_hubs_and_pagination() -> None:
    base_url = "https://www.baeldung.com"
    client = FakeHttpClient(
        {
            "https://www.baeldung.com/sitemap.xml": _fixture("sitemap_index.xml"),
            "https://www.baeldung.com/post-sitemap.xml": _fixture("post_sitemap.xml"),
            "https://www.baeldung.com/page-sitemap.xml": _fixture("page_sitemap.xml"),
            "https://www.baeldung.com/java-tutorial": _fixture("java_tutorial_page_1.html"),
            "https://www.baeldung.com/java-tutorial/page/2": _fixture("java_tutorial_page_2.html"),
            "https://www.baeldung.com/core-java": _fixture("core_java_page_1.html"),
            "https://www.baeldung.com/core-java/page/2": _fixture("core_java_page_2.html"),
            "https://www.baeldung.com/spring-tutorial": _fixture("spring_tutorial_page_1.html"),
        }
    )

    discovered = discover_java_article_urls(
        http_client=client,
        base_url=base_url,
        timeout_seconds=5,
    )

    assert discovered == (
        "https://www.baeldung.com/hibernate-criteria-queries",
        "https://www.baeldung.com/java-lambdas",
        "https://www.baeldung.com/java-streams",
        "https://www.baeldung.com/junit-5-migration",
        "https://www.baeldung.com/spring-beans",
        "https://www.baeldung.com/spring-resttemplate",
    )


def test_discovery_ignores_unavailable_optional_sources_and_deduplicates() -> None:
    client = FakeHttpClient(
        {
            "https://www.baeldung.com/java-tutorial": """
            <html><body>
              <a href=\"/java-zeta/\">zeta</a>
              <a href=\"https://www.baeldung.com/java-alpha?utm=test\">alpha</a>
              <a href=\"https://www.baeldung.com/java-zeta#toc\">duplicate</a>
              <a href=\"https://www.baeldung.com/courses/learn-spring-course\">course-noise</a>
              <a href=\"/kotlin-guide\">non-java</a>
            </body></html>
            """,
        }
    )

    discovered = discover_java_article_urls(
        http_client=client,
        base_url="https://www.baeldung.com",
        timeout_seconds=5,
    )

    assert discovered == (
        "https://www.baeldung.com/java-alpha",
        "https://www.baeldung.com/java-zeta",
    )


def test_discovery_emits_milestone_logs(caplog) -> None:
    client = FakeHttpClient(
        {
            "https://www.baeldung.com/sitemap.xml": _fixture("sitemap_index.xml"),
            "https://www.baeldung.com/post-sitemap.xml": _fixture("post_sitemap.xml"),
            "https://www.baeldung.com/page-sitemap.xml": _fixture("page_sitemap.xml"),
            "https://www.baeldung.com/java-tutorial": _fixture("java_tutorial_page_1.html"),
            "https://www.baeldung.com/core-java": _fixture("core_java_page_1.html"),
            "https://www.baeldung.com/spring-tutorial": _fixture("spring_tutorial_page_1.html"),
        }
    )

    caplog.set_level(logging.INFO)

    discover_java_article_urls(
        http_client=client,
        base_url="https://www.baeldung.com",
        timeout_seconds=5,
    )

    assert "discovery_start" in caplog.text
    assert "discovery_source_scanned" in caplog.text
    assert "discovery_milestone" in caplog.text
    assert "dedup_total=" in caplog.text
    assert "discovery_complete" in caplog.text
