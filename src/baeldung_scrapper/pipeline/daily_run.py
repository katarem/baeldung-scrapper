from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from urllib.parse import urlparse

from baeldung_scrapper.config.logging_setup import format_log_fields
from baeldung_scrapper.config.settings import AppSettings
from baeldung_scrapper.domain.models.article import Article
from baeldung_scrapper.domain.models.artifact_manifest import ArtifactManifest, ManifestEntry
from baeldung_scrapper.domain.models.artifact_serialization import (
    serialize_article_index,
    serialize_normalized_article,
)
from baeldung_scrapper.domain.models.storage_layout import (
    build_article_artifact_path,
    build_article_index_path,
    build_manifest_path,
)
from baeldung_scrapper.domain.ports.cloud_storage import ArtifactKind, ArtifactObject, CloudStorageProvider
from baeldung_scrapper.extraction.editorial_extractor import extract_article_from_html
from baeldung_scrapper.fetching.baeldung_discovery import discover_java_article_urls
from baeldung_scrapper.fetching.http_client import FetchError, HttpClient, build_fetch_client
from baeldung_scrapper.infrastructure.cloud_storage.factory import ProviderRuntimeBinding, build_cloud_storage_provider


@dataclass(frozen=True)
class RunFailure:
    article_url: str
    reason: str


@dataclass(frozen=True)
class RunReport:
    discovered_count: int
    succeeded_count: int
    failed_count: int
    manifest_path: str
    artifact_paths: tuple[str, ...]
    failures: tuple[RunFailure, ...]

    @property
    def has_failures(self) -> bool:
        return self.failed_count > 0

    def to_dict(self) -> dict[str, object]:
        return {
            "discovered_count": self.discovered_count,
            "succeeded_count": self.succeeded_count,
            "failed_count": self.failed_count,
            "manifest_path": self.manifest_path,
            "artifact_paths": list(self.artifact_paths),
            "failures": [
                {
                    "article_url": failure.article_url,
                    "reason": failure.reason,
                }
                for failure in self.failures
            ],
        }


def run_daily_scrape(
    settings: AppSettings,
    *,
    http_client: HttpClient | None = None,
    storage_binding: ProviderRuntimeBinding | None = None,
    run_at: datetime | None = None,
    logger: logging.Logger | None = None,
) -> RunReport:
    active_logger = logger or logging.getLogger(__name__)
    client = http_client or build_fetch_client(settings)
    binding = storage_binding or build_cloud_storage_provider(settings)
    now = run_at.astimezone(timezone.utc) if run_at is not None else datetime.now(timezone.utc)

    active_logger.info(
        "run_start"
        + format_log_fields(
            fetch_backend=settings.fetch_backend,
            source_base_url=settings.source_base_url,
            storage_backend=settings.storage_backend,
        )
    )
    active_logger.info("run_discovery_start" + format_log_fields(source_base_url=settings.source_base_url))

    discovered_article_urls = discover_java_article_urls(
        http_client=client,
        base_url=settings.source_base_url,
        timeout_seconds=settings.source_timeout_seconds,
        logger=active_logger,
    )
    active_logger.info("run_discovery_complete" + format_log_fields(discovered_count=len(discovered_article_urls)))

    article_urls, new_article_count, existing_article_count = _prioritize_article_urls(
        article_urls=discovered_article_urls,
        destination_root_id=binding.destination_root_id,
        destination_folder_path=settings.destination_folder_path,
        storage_provider=binding.provider,
    )
    active_logger.info(
        "run_prioritization_active"
        + format_log_fields(
            total_articles=len(article_urls),
            new_articles=new_article_count,
            existing_articles=existing_article_count,
            order="new_first",
        )
    )

    manifest_entries: list[ManifestEntry] = []
    failures: list[RunFailure] = []
    artifact_paths: list[str] = []

    total_articles = len(article_urls)
    for index, article_url in enumerate(article_urls, start=1):
        active_logger.info(
            "article_processing_start"
            + format_log_fields(position=index, total=total_articles, article_url=article_url)
        )
        try:
            html = client.get_text(url=article_url, timeout_seconds=settings.source_timeout_seconds)
            extraction_result = extract_article_from_html(html=html, source_url=article_url)
            if not extraction_result.validation.is_valid:
                reason = ", ".join(extraction_result.validation.errors)
                failures.append(
                    RunFailure(
                        article_url=article_url,
                        reason=reason,
                    )
                )
                active_logger.warning(
                    "article_processing_failed"
                    + format_log_fields(
                        position=index,
                        total=total_articles,
                        article_url=article_url,
                        reason=reason,
                    )
                )
                continue

            source_id = _build_source_id(article_url)
            title = extraction_result.artifact.title.strip() or source_id
            article = Article(
                source_id=source_id,
                title=title,
                url=article_url,
                category="java",
                published_at=extraction_result.artifact.published_at,
                updated_at=extraction_result.artifact.updated_at,
            )

            artifact_path = build_article_artifact_path(
                root_prefix=settings.destination_folder_path,
                article=article,
            )
            index_path = build_article_index_path(
                root_prefix=settings.destination_folder_path,
                article_url=article_url,
            )

            artifact_payload = serialize_normalized_article(extraction_result.artifact)
            index_payload = serialize_article_index(
                source_id=article.source_id,
                article_url=article_url,
                article_path=artifact_path,
                generated_at=now,
            )

            write_result = binding.provider.upsert(
                destination_root_id=binding.destination_root_id,
                item=ArtifactObject(
                    object_path=artifact_path,
                    kind=ArtifactKind.ARTICLE,
                    mime_type="application/json",
                    payload=artifact_payload,
                    modified_at=now,
                ),
            )
            binding.provider.upsert(
                destination_root_id=binding.destination_root_id,
                item=ArtifactObject(
                    object_path=index_path,
                    kind=ArtifactKind.INDEX,
                    mime_type="application/json",
                    payload=index_payload,
                    modified_at=now,
                ),
            )

            manifest_entries.append(
                ManifestEntry(
                    source_id=article.source_id,
                    article_url=article_url,
                    article_path=artifact_path,
                    index_path=index_path,
                    content_sha256=write_result.checksum_sha256,
                    last_seen_at=now,
                )
            )
            artifact_paths.append(artifact_path)
            active_logger.info(
                "article_processing_succeeded"
                + format_log_fields(
                    position=index,
                    total=total_articles,
                    article_url=article_url,
                    artifact_path=artifact_path,
                )
            )
        except FetchError as exc:
            failures.append(RunFailure(article_url=article_url, reason=str(exc)))
            active_logger.warning(
                "article_processing_failed"
                + format_log_fields(
                    position=index,
                    total=total_articles,
                    article_url=article_url,
                    reason=str(exc),
                )
            )
        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"
            failures.append(RunFailure(article_url=article_url, reason=reason))
            active_logger.warning(
                "article_processing_failed"
                + format_log_fields(
                    position=index,
                    total=total_articles,
                    article_url=article_url,
                    reason=reason,
                )
            )

    manifest = ArtifactManifest(generated_at=now, entries=tuple(manifest_entries))
    manifest_path = build_manifest_path(
        root_prefix=settings.destination_folder_path,
        manifest_relative_path=settings.storage_manifest_path,
    )
    binding.provider.upsert(
        destination_root_id=binding.destination_root_id,
        item=ArtifactObject(
            object_path=manifest_path,
            kind=ArtifactKind.MANIFEST,
            mime_type="application/json",
            payload=manifest.to_json_bytes(),
            modified_at=now,
        ),
    )

    active_logger.info(
        "run_complete"
        + format_log_fields(
            discovered_count=len(discovered_article_urls),
            succeeded_count=len(manifest_entries),
            failed_count=len(failures),
            manifest_path=manifest_path,
        )
    )

    return RunReport(
        discovered_count=len(discovered_article_urls),
        succeeded_count=len(manifest_entries),
        failed_count=len(failures),
        manifest_path=manifest_path,
        artifact_paths=tuple(sorted(artifact_paths)),
        failures=tuple(failures),
    )


def _build_source_id(article_url: str) -> str:
    parsed = urlparse(article_url)
    slug = parsed.path.strip("/")
    if not slug:
        return "article"
    return slug.replace("/", "-")


def _prioritize_article_urls(
    *,
    article_urls: tuple[str, ...],
    destination_root_id: str,
    destination_folder_path: str,
    storage_provider: CloudStorageProvider,
) -> tuple[tuple[str, ...], int, int]:
    new_urls: list[str] = []
    existing_urls: list[str] = []

    for article_url in article_urls:
        artifact_path = _build_expected_artifact_path(
            article_url=article_url,
            destination_folder_path=destination_folder_path,
        )
        if storage_provider.exists(destination_root_id=destination_root_id, object_path=artifact_path):
            existing_urls.append(article_url)
        else:
            new_urls.append(article_url)

    ordered_urls = tuple(sorted(new_urls) + sorted(existing_urls))
    return ordered_urls, len(new_urls), len(existing_urls)


def _build_expected_artifact_path(*, article_url: str, destination_folder_path: str) -> str:
    source_id = _build_source_id(article_url)
    expected_article = Article(
        source_id=source_id,
        title=source_id,
        url=article_url,
        category="java",
    )
    return build_article_artifact_path(root_prefix=destination_folder_path, article=expected_article)
