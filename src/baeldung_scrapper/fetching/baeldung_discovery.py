from __future__ import annotations

from collections import deque
import logging
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from baeldung_scrapper.fetching.http_client import FetchError
from baeldung_scrapper.fetching.http_client import HttpClient


_DEFAULT_DISCOVERY_HUBS: tuple[str, ...] = (
    "/java-tutorial",
    "/core-java",
    "/spring-tutorial",
)

_DEFAULT_SITEMAP_PATHS: tuple[str, ...] = (
    "/sitemap.xml",
    "/sitemap_index.xml",
)

_MAX_HUB_PAGES = 5
_MAX_SITEMAP_DOCS = 12

_NON_ARTICLE_PATH_PREFIXES: tuple[str, ...] = (
    "/courses",
    "/category",
    "/tag",
    "/author",
    "/about",
    "/feed",
    "/wp-json",
    "/search",
)

_NON_ARTICLE_SUFFIXES: tuple[str, ...] = (
    ".xml",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".svg",
    ".css",
    ".js",
    ".pdf",
)

_JAVA_RELEVANT_KEYWORDS: tuple[str, ...] = (
    "java",
    "spring",
    "junit",
    "maven",
    "gradle",
    "jakarta",
    "hibernate",
    "jvm",
)


_LOGGER = logging.getLogger(__name__)


def discover_java_article_urls(
    *,
    http_client: HttpClient,
    base_url: str,
    timeout_seconds: float,
    logger: logging.Logger | None = None,
) -> tuple[str, ...]:
    active_logger = logger or _LOGGER
    parsed_base = urlparse(base_url)
    source_scheme = parsed_base.scheme or "https"
    source_host = parsed_base.netloc.lower()
    source_origin = f"{source_scheme}://{source_host}"
    discovered: set[str] = set()

    active_logger.info(
        "discovery_start source_origin=%s timeout_seconds=%.2f",
        source_origin,
        timeout_seconds,
    )

    for article_url in _discover_from_sitemaps(
        http_client=http_client,
        source_origin=source_origin,
        source_scheme=source_scheme,
        source_host=source_host,
        timeout_seconds=timeout_seconds,
        logger=active_logger,
    ):
        discovered.add(article_url)

    active_logger.info("discovery_milestone source=sitemaps dedup_total=%d", len(discovered))

    for hub_path in _DEFAULT_DISCOVERY_HUBS:
        hub_url = f"{source_origin}{hub_path}"
        before = len(discovered)
        for article_url in _discover_from_hub_with_pagination(
            http_client=http_client,
            hub_url=hub_url,
            hub_path=hub_path,
            source_origin=source_origin,
            source_scheme=source_scheme,
            source_host=source_host,
            timeout_seconds=timeout_seconds,
            logger=active_logger,
        ):
            discovered.add(article_url)

        active_logger.info(
            "discovery_milestone source=%s found=%d dedup_total=%d",
            hub_path,
            len(discovered) - before,
            len(discovered),
        )

    active_logger.info("discovery_complete discovered_count=%d", len(discovered))

    return tuple(sorted(discovered))


def _discover_from_sitemaps(
    *,
    http_client: HttpClient,
    source_origin: str,
    source_scheme: str,
    source_host: str,
    timeout_seconds: float,
    logger: logging.Logger,
) -> set[str]:
    discovered: set[str] = set()
    queue = deque(f"{source_origin}{path}" for path in _DEFAULT_SITEMAP_PATHS)
    visited: set[str] = set()

    while queue and len(visited) < _MAX_SITEMAP_DOCS:
        sitemap_url = queue.popleft()
        if sitemap_url in visited:
            continue
        visited.add(sitemap_url)

        try:
            payload = _safe_fetch_text(
                http_client=http_client,
                url=sitemap_url,
                timeout_seconds=timeout_seconds,
            )
        except FetchError as exc:
            logger.warning("discovery_source_unavailable source=%s reason=%s", sitemap_url, str(exc))
            continue

        locations = _extract_sitemap_locations(payload)
        nested_sitemap_count = 0
        article_count = 0
        for loc_value in locations:
            canonical = _canonicalize_url(
                candidate_url=loc_value,
                source_scheme=source_scheme,
                source_host=source_host,
            )
            if canonical is None:
                continue

            parsed = urlparse(canonical)
            if parsed.path.endswith(".xml"):
                if canonical not in visited:
                    queue.append(canonical)
                    nested_sitemap_count += 1
                continue

            if _is_relevant_article_path(parsed.path):
                discovered.add(canonical)
                article_count += 1

        logger.info(
            "discovery_source_scanned source=%s type=sitemap locations=%d nested=%d articles=%d dedup_total=%d",
            sitemap_url,
            len(locations),
            nested_sitemap_count,
            article_count,
            len(discovered),
        )

    return discovered


def _extract_sitemap_locations(payload: str) -> tuple[str, ...]:
    matches = re.findall(r"<loc>\s*([^<]+?)\s*</loc>", payload, flags=re.IGNORECASE)
    return tuple(match.strip() for match in matches if match.strip())


def _discover_from_hub_with_pagination(
    *,
    http_client: HttpClient,
    hub_url: str,
    hub_path: str,
    source_origin: str,
    source_scheme: str,
    source_host: str,
    timeout_seconds: float,
    logger: logging.Logger,
) -> set[str]:
    discovered: set[str] = set()
    queue = deque([hub_url])
    visited_pages: set[str] = set()

    while queue and len(visited_pages) < _MAX_HUB_PAGES:
        page_url = queue.popleft()
        if page_url in visited_pages:
            continue
        visited_pages.add(page_url)

        try:
            payload = _safe_fetch_text(
                http_client=http_client,
                url=page_url,
                timeout_seconds=timeout_seconds,
            )
        except FetchError as exc:
            logger.warning("discovery_source_unavailable source=%s reason=%s", page_url, str(exc))
            continue

        soup = BeautifulSoup(payload, "html.parser")
        page_article_count = 0
        page_pagination_count = 0
        for anchor in soup.select("a[href]"):
            href = anchor.get("href")
            if not isinstance(href, str) or not href.strip():
                continue

            canonical = _canonicalize_url(
                candidate_url=urljoin(source_origin, href.strip()),
                source_scheme=source_scheme,
                source_host=source_host,
            )
            if canonical is None:
                continue

            parsed = urlparse(canonical)
            if _is_hub_pagination_url(path=parsed.path, hub_path=hub_path):
                if canonical not in visited_pages:
                    queue.append(canonical)
                    page_pagination_count += 1
                continue

            if _is_relevant_article_path(parsed.path):
                discovered.add(canonical)
                page_article_count += 1

        logger.info(
            "discovery_source_scanned source=%s type=hub_page articles=%d queued_pages=%d dedup_total=%d",
            page_url,
            page_article_count,
            page_pagination_count,
            len(discovered),
        )

    return discovered


def _safe_fetch_text(*, http_client: HttpClient, url: str, timeout_seconds: float) -> str:
    return http_client.get_text(url=url, timeout_seconds=timeout_seconds)


def _canonicalize_url(*, candidate_url: str, source_scheme: str, source_host: str) -> str | None:
    parsed = urlparse(candidate_url)
    scheme = parsed.scheme.lower()
    host = parsed.netloc.lower()

    if scheme not in {"http", "https"}:
        return None
    if host != source_host:
        return None

    path_parts = [part for part in parsed.path.split("/") if part]
    normalized_path = "/" + "/".join(path_parts)
    if normalized_path != "/":
        normalized_path = normalized_path.rstrip("/")

    return f"{source_scheme}://{source_host}{normalized_path}"


def _is_hub_pagination_url(*, path: str, hub_path: str) -> bool:
    normalized = path.rstrip("/")
    if not normalized.startswith(hub_path):
        return False

    suffix = normalized[len(hub_path) :]
    return suffix.startswith("/page/")


def _is_relevant_article_path(path: str) -> bool:
    normalized = path.rstrip("/")
    if not normalized:
        return False

    if normalized in _DEFAULT_DISCOVERY_HUBS:
        return False

    for prefix in _NON_ARTICLE_PATH_PREFIXES:
        if normalized == prefix or normalized.startswith(f"{prefix}/"):
            return False

    for suffix in _NON_ARTICLE_SUFFIXES:
        if normalized.endswith(suffix):
            return False

    slug = normalized.split("/")[-1]
    if not slug:
        return False

    for keyword in _JAVA_RELEVANT_KEYWORDS:
        if slug == keyword:
            return True
        if slug.startswith(f"{keyword}-"):
            return True
        if f"-{keyword}-" in slug:
            return True

    return False
