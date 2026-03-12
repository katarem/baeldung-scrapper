from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from baeldung_scrapper.config.settings import AppSettings
from baeldung_scrapper.domain.models.article import Article
from baeldung_scrapper.domain.models.artifact_serialization import serialize_normalized_article
from baeldung_scrapper.domain.models.storage_layout import build_article_artifact_path
from baeldung_scrapper.domain.ports.cloud_storage import ArtifactKind, ArtifactObject, ArtifactWriteResult
from baeldung_scrapper.extraction.editorial_extractor import extract_article_from_html
from baeldung_scrapper.fetching.http_client import FetchError
from baeldung_scrapper.infrastructure.cloud_storage.factory import ProviderRuntimeBinding
from baeldung_scrapper.pipeline.daily_run import run_daily_scrape


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "articles"


def _fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


@dataclass
class _StoredItem:
    destination_root_id: str
    item: ArtifactObject


class FakeCloudProvider:
    provider_name = "fake-s3"

    def __init__(self) -> None:
        self.items: dict[str, _StoredItem] = {}

    def upsert(self, *, destination_root_id: str, item: ArtifactObject) -> ArtifactWriteResult:
        self.items[item.object_path] = _StoredItem(destination_root_id=destination_root_id, item=item)
        return ArtifactWriteResult(
            provider_object_id=f"{destination_root_id}/{item.object_path}",
            checksum_sha256=hashlib.sha256(item.payload).hexdigest(),
        )

    def exists(self, *, destination_root_id: str, object_path: str) -> bool:
        _ = destination_root_id
        return object_path in self.items

    def read(self, *, destination_root_id: str, object_path: str) -> bytes | None:
        _ = destination_root_id
        stored = self.items.get(object_path)
        if stored is None:
            return None
        return stored.item.payload


class FakeHttpClient:
    def __init__(self, payloads: dict[str, str], failing_urls: set[str] | None = None) -> None:
        self._payloads = payloads
        self._failing_urls = failing_urls or set()
        self.requested_urls: list[str] = []

    def get_text(self, *, url: str, timeout_seconds: float) -> str:
        _ = timeout_seconds
        self.requested_urls.append(url)
        if url in self._failing_urls:
            raise FetchError(f"failed to fetch url: {url}")
        payload = self._payloads.get(url)
        if payload is None:
            raise FetchError(f"failed to fetch url: {url}")
        return payload


def _settings() -> AppSettings:
    return AppSettings(
        destination_folder_path="team/baeldung-java-daily",
        storage_manifest_path="manifests/latest.json",
        s3_endpoint="https://bucket.up.railway.app",
        s3_region="us-east-1",
        s3_bucket="daily-bucket",
        s3_access_key_id="access",
        s3_secret_access_key="secret",
    )


def test_run_daily_scrape_happy_path_writes_articles_indexes_and_manifest() -> None:
    discovery_url = "https://www.baeldung.com/java-tutorial"
    article_a = "https://www.baeldung.com/java-streams"
    article_b = "https://www.baeldung.com/java-noisy-layout"
    discovery_html = f"""
    <html><body>
      <a href=\"{article_a}\">A</a>
      <a href=\"{article_b}/\">B</a>
      <a href=\"/kotlin-guide\">Skip</a>
    </body></html>
    """

    client = FakeHttpClient(
        {
            discovery_url: discovery_html,
            article_a: _fixture("standard_article.html"),
            article_b: _fixture("noisy_article.html"),
        }
    )
    provider = FakeCloudProvider()

    report = run_daily_scrape(
        _settings(),
        http_client=client,
        storage_binding=ProviderRuntimeBinding(provider=provider, destination_root_id="daily-bucket"),
        run_at=datetime(2026, 3, 12, 15, 30, tzinfo=timezone.utc),
    )

    assert report.discovered_count == 2
    assert report.succeeded_count == 2
    assert report.failed_count == 0
    assert report.manifest_path == "team/baeldung-java-daily/manifests/latest.json"
    assert len(report.artifact_paths) == 2
    assert len(provider.items) == 5

    manifest_payload = provider.read(
        destination_root_id="daily-bucket",
        object_path="team/baeldung-java-daily/manifests/latest.json",
    )
    assert manifest_payload is not None
    manifest_data = json.loads(manifest_payload.decode("utf-8"))
    assert len(manifest_data["entries"]) == 2
    assert [entry["article_url"] for entry in manifest_data["entries"]] == sorted([article_a, article_b])


def test_run_daily_scrape_records_partial_success_and_keeps_manifest_for_successes() -> None:
    discovery_url = "https://www.baeldung.com/java-tutorial"
    article_ok = "https://www.baeldung.com/java-streams"
    article_invalid = "https://www.baeldung.com/java-preservation-edge"
    article_fetch_error = "https://www.baeldung.com/java-missing"
    discovery_html = f"""
    <html><body>
      <a href=\"{article_ok}\">OK</a>
      <a href=\"{article_invalid}\">Invalid</a>
      <a href=\"{article_fetch_error}\">FetchFail</a>
    </body></html>
    """

    client = FakeHttpClient(
        {
            discovery_url: discovery_html,
            article_ok: _fixture("standard_article.html"),
            article_invalid: _fixture("preservation_edge_case.html"),
        },
        failing_urls={article_fetch_error},
    )
    provider = FakeCloudProvider()

    report = run_daily_scrape(
        _settings(),
        http_client=client,
        storage_binding=ProviderRuntimeBinding(provider=provider, destination_root_id="daily-bucket"),
        run_at=datetime(2026, 3, 12, 15, 30, tzinfo=timezone.utc),
    )

    assert report.discovered_count == 3
    assert report.succeeded_count == 1
    assert report.failed_count == 2
    assert len(report.failures) == 2

    manifest_payload = provider.read(
        destination_root_id="daily-bucket",
        object_path="team/baeldung-java-daily/manifests/latest.json",
    )
    assert manifest_payload is not None
    manifest_data = json.loads(manifest_payload.decode("utf-8"))
    assert len(manifest_data["entries"]) == 1
    assert manifest_data["entries"][0]["article_url"] == article_ok


def test_run_daily_scrape_prioritizes_new_articles_before_existing_articles() -> None:
    discovery_url = "https://www.baeldung.com/java-tutorial"
    article_new_b = "https://www.baeldung.com/java-zeta-priority"
    article_existing = "https://www.baeldung.com/java-streams"
    article_new_a = "https://www.baeldung.com/java-alpha-priority"
    discovery_html = f"""
    <html><body>
      <a href=\"{article_new_b}\">New B</a>
      <a href=\"{article_existing}\">Existing</a>
      <a href=\"{article_new_a}\">New A</a>
    </body></html>
    """

    client = FakeHttpClient(
        {
            discovery_url: discovery_html,
            article_new_b: _fixture("standard_article.html"),
            article_existing: _fixture("standard_article.html"),
            article_new_a: _fixture("standard_article.html"),
        }
    )
    provider = FakeCloudProvider()
    existing_source_id = "java-streams"
    existing_path = build_article_artifact_path(
        root_prefix="team/baeldung-java-daily",
        article=Article(
            source_id=existing_source_id,
            title=existing_source_id,
            url=article_existing,
            category="java",
        ),
    )
    provider.upsert(
        destination_root_id="daily-bucket",
        item=ArtifactObject(
            object_path=existing_path,
            kind=ArtifactKind.ARTICLE,
            mime_type="application/json",
            payload=b"{\"preexisting\": true}",
            modified_at=datetime(2026, 3, 11, 15, 30, tzinfo=timezone.utc),
        ),
    )

    report = run_daily_scrape(
        _settings(),
        http_client=client,
        storage_binding=ProviderRuntimeBinding(provider=provider, destination_root_id="daily-bucket"),
        run_at=datetime(2026, 3, 12, 15, 30, tzinfo=timezone.utc),
    )

    assert report.discovered_count == 3
    assert report.succeeded_count == 3
    article_urls = {article_new_a, article_new_b, article_existing}
    article_fetch_sequence = [url for url in client.requested_urls if url in article_urls]
    assert article_fetch_sequence == [article_new_a, article_new_b, article_existing]


def test_serialize_normalized_article_is_deterministic() -> None:
    artifact = extract_article_from_html(
        html=_fixture("standard_article.html"),
        source_url="https://www.baeldung.com/java-streams",
    ).artifact

    first = serialize_normalized_article(artifact)
    second = serialize_normalized_article(artifact)

    assert first == second


def test_run_daily_scrape_builds_fetch_client_from_settings_when_not_injected(
    monkeypatch,
) -> None:
    settings = AppSettings(
        storage_backend="local",
        local_base_directory=".tmp/baeldung-artifacts",
        fetch_backend="httpx",
    )
    provider = FakeCloudProvider()
    observed: dict[str, object] = {}

    class _FakeClient:
        def get_text(self, *, url: str, timeout_seconds: float) -> str:
            _ = (url, timeout_seconds)
            raise AssertionError("no article fetch expected")

    def _fake_build_fetch_client(passed_settings: AppSettings) -> _FakeClient:
        observed["fetch_backend"] = passed_settings.fetch_backend
        return _FakeClient()

    monkeypatch.setattr("baeldung_scrapper.pipeline.daily_run.build_fetch_client", _fake_build_fetch_client)
    monkeypatch.setattr("baeldung_scrapper.pipeline.daily_run.discover_java_article_urls", lambda **_: ())

    report = run_daily_scrape(
        settings,
        storage_binding=ProviderRuntimeBinding(provider=provider, destination_root_id="local-filesystem"),
        run_at=datetime(2026, 3, 12, 15, 30, tzinfo=timezone.utc),
    )

    assert observed["fetch_backend"] == "httpx"
    assert report.discovered_count == 0


def test_run_daily_scrape_emits_run_summary_logs(caplog) -> None:
    discovery_url = "https://www.baeldung.com/java-tutorial"
    article_url = "https://www.baeldung.com/java-streams"
    client = FakeHttpClient(
        {
            discovery_url: f"<html><body><a href=\"{article_url}\">A</a></body></html>",
            article_url: _fixture("standard_article.html"),
        }
    )
    provider = FakeCloudProvider()

    caplog.set_level(logging.INFO)

    report = run_daily_scrape(
        _settings(),
        http_client=client,
        storage_binding=ProviderRuntimeBinding(provider=provider, destination_root_id="daily-bucket"),
        run_at=datetime(2026, 3, 12, 15, 30, tzinfo=timezone.utc),
    )

    assert report.discovered_count == 1
    assert report.succeeded_count == 1
    assert report.failed_count == 0
    assert "run_start" in caplog.text
    assert "run_discovery_complete" in caplog.text
    assert "run_prioritization_active" in caplog.text
    assert "article_processing_start" in caplog.text
    assert "article_processing_succeeded" in caplog.text
    assert "run_complete" in caplog.text
    assert "discovered_count=1" in caplog.text
    assert "new_articles=1" in caplog.text
    assert "existing_articles=0" in caplog.text
    assert "succeeded_count=1" in caplog.text
    assert "failed_count=0" in caplog.text
