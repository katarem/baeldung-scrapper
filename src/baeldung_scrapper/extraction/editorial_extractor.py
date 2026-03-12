from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from bs4 import BeautifulSoup, Tag

from baeldung_scrapper.domain.models.normalized_article import (
    NormalizedArticleArtifact,
    NormalizedBodyBlock,
    ValidationPayload,
    ValidationResult,
)

_EDITORIAL_SELECTORS = (
    "article [itemprop='articleBody']",
    "article[itemprop='articleBody']",
    "article .post-content",
    "article .entry-content",
    "article",
    "main article",
    "main",
)

_EXCLUSION_PATTERN = re.compile(
    r"(ad|ads|promo|sponsor|widget|newsletter|sidebar|related|share|social|cookie)", re.IGNORECASE
)

_PROTECTED_TAGS = {"p", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol", "blockquote", "table", "pre", "code", "figure", "img"}

_SUPPORTED_BLOCK_TAGS = {"p", "h2", "h3", "h4", "h5", "h6", "ul", "ol", "blockquote", "table", "pre", "code", "figure", "img"}


@dataclass(frozen=True)
class ExtractionResult:
    artifact: NormalizedArticleArtifact
    validation: ValidationResult


def extract_article_from_html(*, html: str, source_url: str) -> ExtractionResult:
    soup = BeautifulSoup(html, "html.parser")
    editorial_root = _isolate_editorial_root(soup)
    source_code_blocks = _count_source_code_blocks(editorial_root)
    source_images = len(editorial_root.find_all("img"))

    cleaned_root = _clean_editorial_root(editorial_root)
    body_blocks = _normalize_body_blocks(cleaned_root)
    metadata = _extract_metadata(soup, cleaned_root)

    extracted_code_blocks = sum(1 for block in body_blocks if block.kind == "code")
    extracted_images = sum(1 for block in body_blocks if block.kind == "image")

    artifact = NormalizedArticleArtifact(
        source_url=source_url,
        title=metadata["title"],
        body_blocks=tuple(body_blocks),
        published_at=metadata["published_at"],
        updated_at=metadata["updated_at"],
        author=metadata["author"],
        reviewer=metadata["reviewer"],
        tags=tuple(metadata["tags"]),
        validation_payload=ValidationPayload(
            source_code_blocks=source_code_blocks,
            extracted_code_blocks=extracted_code_blocks,
            source_images=source_images,
            extracted_images=extracted_images,
        ),
    )
    validation = validate_artifact(artifact)
    return ExtractionResult(artifact=artifact, validation=validation)


def validate_artifact(artifact: NormalizedArticleArtifact) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    if not artifact.title.strip():
        errors.append("title is required")
    if len(artifact.body_blocks) == 0:
        errors.append("body_blocks must contain at least one block")

    payload = artifact.validation_payload
    if payload.source_code_blocks > 0 and payload.extracted_code_blocks == 0:
        errors.append("code block preservation check failed")
    if payload.source_images > 0 and payload.extracted_images == 0:
        errors.append("image preservation check failed")

    if artifact.published_at is None:
        warnings.append("published_at missing")
    if artifact.updated_at is None:
        warnings.append("updated_at missing")
    if artifact.author is None:
        warnings.append("author missing")
    if artifact.reviewer is None:
        warnings.append("reviewer missing")
    if len(artifact.tags) == 0:
        warnings.append("tags missing")

    return ValidationResult(errors=tuple(errors), warnings=tuple(warnings))


def _isolate_editorial_root(soup: BeautifulSoup) -> Tag:
    for selector in _EDITORIAL_SELECTORS:
        candidate = soup.select_one(selector)
        if isinstance(candidate, Tag) and candidate.get_text(strip=True):
            return candidate

    fallback = soup.body if isinstance(soup.body, Tag) else soup
    return fallback


def _clean_editorial_root(editorial_root: Tag) -> Tag:
    cleaned = BeautifulSoup(str(editorial_root), "html.parser")
    root = cleaned.find()
    if not isinstance(root, Tag):
        raise ValueError("unable to parse editorial root")

    for node in list(root.find_all(True)):
        if node.name in _PROTECTED_TAGS:
            continue
        if _is_excluded_node(node):
            if _contains_protected_descendant(node):
                node.unwrap()
            else:
                node.decompose()

    for noisy in root.find_all({"script", "style", "noscript", "iframe", "form"}):
        noisy.decompose()

    return root


def _normalize_body_blocks(root: Tag) -> list[NormalizedBodyBlock]:
    blocks: list[NormalizedBodyBlock] = []

    for node in root.find_all(_SUPPORTED_BLOCK_TAGS):
        if _has_supported_ancestor(node):
            continue

        if node.name == "p":
            text = node.get_text(" ", strip=True)
            if text:
                blocks.append(NormalizedBodyBlock(kind="paragraph", text=text))
        elif node.name in {"h2", "h3", "h4", "h5", "h6"}:
            text = node.get_text(" ", strip=True)
            if text:
                blocks.append(NormalizedBodyBlock(kind="heading", text=text, level=int(node.name[1])))
        elif node.name in {"ul", "ol"}:
            items = tuple(
                item.get_text(" ", strip=True)
                for item in node.find_all("li", recursive=False)
                if item.get_text(" ", strip=True)
            )
            if items:
                blocks.append(NormalizedBodyBlock(kind="list", items=items))
        elif node.name == "blockquote":
            text = node.get_text(" ", strip=True)
            if text:
                blocks.append(NormalizedBodyBlock(kind="blockquote", text=text))
        elif node.name == "table":
            blocks.append(NormalizedBodyBlock(kind="table", html=str(node)))
        elif node.name == "pre" or node.name == "code":
            code_text = node.get_text("\n", strip=False).strip("\n")
            if code_text:
                language = _extract_code_language(node)
                blocks.append(NormalizedBodyBlock(kind="code", text=code_text, language=language))
        elif node.name == "figure":
            image = node.find("img")
            image_src = _tag_get(image, "src") if isinstance(image, Tag) else None
            if isinstance(image_src, str) and image_src:
                blocks.append(
                    NormalizedBodyBlock(
                        kind="image",
                        src=image_src,
                        alt=_coerce_optional_str(_tag_get(image, "alt")) if isinstance(image, Tag) else None,
                        caption=_extract_figure_caption(node),
                    )
                )
        elif node.name == "img":
            src = _tag_get(node, "src")
            if isinstance(src, str) and src:
                blocks.append(
                    NormalizedBodyBlock(kind="image", src=src, alt=_coerce_optional_str(_tag_get(node, "alt")))
                )

    return blocks


def _extract_metadata(soup: BeautifulSoup, root: Tag) -> dict[str, Any]:
    title = _extract_title(soup, root)
    published_at = _extract_datetime(soup, [
        "meta[property='article:published_time']",
        "meta[name='published_time']",
        "time[itemprop='datePublished']",
    ])
    updated_at = _extract_datetime(soup, [
        "meta[property='article:modified_time']",
        "meta[name='modified_time']",
        "time[itemprop='dateModified']",
    ])
    author = _extract_text(soup, [
        "meta[name='author']",
        "[itemprop='author'] [itemprop='name']",
        "a[rel='author']",
        ".author-name",
    ])
    reviewer = _extract_text(soup, [
        "meta[name='reviewer']",
        "[data-reviewer]",
        ".reviewer-name",
    ])
    tags = _extract_tags(soup)

    return {
        "title": title,
        "published_at": published_at,
        "updated_at": updated_at,
        "author": author,
        "reviewer": reviewer,
        "tags": tags,
    }


def _extract_title(soup: BeautifulSoup, root: Tag) -> str:
    text_title = _extract_text(root, ["h1", ".entry-title"])
    if text_title:
        return text_title

    meta = soup.select_one("meta[property='og:title']")
    if isinstance(meta, Tag):
        content = (_coerce_optional_str(meta.get("content")) or "").strip()
        if content:
            return content

    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return ""


def _extract_datetime(soup: BeautifulSoup, selectors: list[str]) -> Optional[datetime]:
    for selector in selectors:
        node = soup.select_one(selector)
        if not isinstance(node, Tag):
            continue

        raw = (
            _coerce_optional_str(node.get("datetime"))
            or _coerce_optional_str(node.get("content"))
            or node.get_text(strip=True)
        ).strip()
        parsed = _parse_datetime(raw)
        if parsed is not None:
            return parsed
    return None


def _extract_text(root: Tag | BeautifulSoup, selectors: list[str]) -> Optional[str]:
    for selector in selectors:
        node = root.select_one(selector)
        if not isinstance(node, Tag):
            continue
        value = (
            _coerce_optional_str(node.get("content"))
            or _coerce_optional_str(node.get("data-reviewer"))
            or node.get_text(" ", strip=True)
        ).strip()
        if value:
            return value
    return None


def _extract_tags(soup: BeautifulSoup) -> tuple[str, ...]:
    values: list[str] = []

    keywords = soup.select_one("meta[name='keywords']")
    if isinstance(keywords, Tag):
        keyword_value = (_coerce_optional_str(keywords.get("content")) or "").strip()
        if keyword_value:
            values.extend(token.strip() for token in keyword_value.split(",") if token.strip())

    for node in soup.select("a[rel='tag'], .post-tags a, .tags a"):
        if isinstance(node, Tag):
            text = node.get_text(" ", strip=True)
            if text:
                values.append(text)

    deduped = []
    seen: set[str] = set()
    for value in values:
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(value)
    return tuple(deduped)


def _extract_figure_caption(node: Tag) -> Optional[str]:
    caption = node.find("figcaption")
    if isinstance(caption, Tag):
        text = caption.get_text(" ", strip=True)
        return text or None
    return None


def _extract_code_language(node: Tag) -> Optional[str]:
    classes = _coerce_string_list(_tag_get(node, "class", []))

    for class_name in classes:
        if class_name.startswith("language-"):
            return class_name.split("language-", 1)[1] or None

    child_code = node.find("code")
    if isinstance(child_code, Tag):
        child_classes = _coerce_string_list(_tag_get(child_code, "class", []))
        for class_name in child_classes:
            if class_name.startswith("language-"):
                return class_name.split("language-", 1)[1] or None
    return None


def _is_excluded_node(node: Tag) -> bool:
    if not isinstance(getattr(node, "attrs", None), dict):
        return False

    values: list[str] = [node.name]

    node_id = _tag_get(node, "id")
    if node_id:
        values.append(node_id)

    classes = _coerce_string_list(_tag_get(node, "class", []))
    values.extend(classes)

    for attr_name in ("role", "aria-label", "data-testid"):
        attr_value = _tag_get(node, attr_name)
        if attr_value:
            values.append(str(attr_value))

    serialized = " ".join(values)
    return bool(_EXCLUSION_PATTERN.search(serialized))


def _contains_protected_descendant(node: Tag) -> bool:
    return node.find(_PROTECTED_TAGS) is not None


def _has_supported_ancestor(node: Tag) -> bool:
    parent = node.parent
    while isinstance(parent, Tag):
        if parent.name in _SUPPORTED_BLOCK_TAGS:
            return True
        parent = parent.parent
    return False


def _count_source_code_blocks(root: Tag) -> int:
    pre_blocks = root.find_all("pre")
    standalone_code = [code for code in root.find_all("code") if code.find_parent("pre") is None]
    return len(pre_blocks) + len(standalone_code)


def _parse_datetime(value: str) -> Optional[datetime]:
    candidate = value.strip()
    if not candidate:
        return None

    normalized = candidate.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _tag_get(node: Tag, attr_name: str, default: Any = None) -> Any:
    attrs = getattr(node, "attrs", None)
    if not isinstance(attrs, dict):
        return default
    return attrs.get(attr_name, default)


def _coerce_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, set)):
        merged = " ".join(str(item) for item in value if item is not None).strip()
        return merged or None
    return str(value)


def _coerce_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if item is not None]
    return [str(value)]
