from __future__ import annotations

import hashlib
import re

from baeldung_scrapper.domain.models.article import Article

_SAFE_SEGMENT_PATTERN = re.compile(r"[^a-z0-9._-]+")


def normalize_storage_root(root_prefix: str) -> str:
    raw = root_prefix.strip()
    if raw.startswith("/"):
        raise ValueError("storage_root_prefix must be a relative path")
    if "\\" in raw:
        raise ValueError("storage_root_prefix must use '/' separators")

    normalized = "/".join(segment for segment in raw.split("/") if segment)
    if not normalized:
        raise ValueError("storage_root_prefix must not be empty")
    if ".." in normalized.split("/") or "." in normalized.split("/"):
        raise ValueError("storage_root_prefix must not contain '.' or '..' segments")
    return normalized


def normalize_relative_path(path: str) -> str:
    raw = path.strip()
    if raw.startswith("/"):
        raise ValueError("path must be relative")
    if "\\" in raw:
        raise ValueError("path must use '/' separators")

    normalized = "/".join(segment for segment in raw.split("/") if segment)
    if not normalized:
        raise ValueError("path must not be empty")
    if ".." in normalized.split("/") or "." in normalized.split("/"):
        raise ValueError("path must not contain '.' or '..' segments")
    return normalized


def build_article_artifact_path(*, root_prefix: str, article: Article) -> str:
    root = normalize_storage_root(root_prefix)
    category = _normalize_segment(article.category)
    source_id = _normalize_segment(article.source_id)
    return f"{root}/articles/{category}/{source_id}.json"


def build_article_index_path(*, root_prefix: str, article_url: str) -> str:
    root = normalize_storage_root(root_prefix)
    url_fingerprint = hashlib.sha256(article_url.encode("utf-8")).hexdigest()
    return f"{root}/indexes/by-url/{url_fingerprint}.json"


def build_manifest_path(*, root_prefix: str, manifest_relative_path: str) -> str:
    root = normalize_storage_root(root_prefix)
    relative_path = normalize_relative_path(manifest_relative_path)
    return f"{root}/{relative_path}"


def _normalize_segment(value: str) -> str:
    lowered = value.strip().lower()
    normalized = _SAFE_SEGMENT_PATTERN.sub("-", lowered).strip("-._")
    if not normalized:
        raise ValueError("storage segment cannot be empty after normalization")
    return normalized
