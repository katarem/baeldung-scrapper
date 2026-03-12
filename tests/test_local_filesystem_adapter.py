from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from baeldung_scrapper.domain.ports.cloud_storage import ArtifactKind, ArtifactObject
from baeldung_scrapper.infrastructure.cloud_storage.local_filesystem import (
    LocalFilesystemStorageAdapter,
    LocalStorageAdapterError,
)


def _artifact(*, object_path: str, payload: bytes) -> ArtifactObject:
    return ArtifactObject(
        object_path=object_path,
        kind=ArtifactKind.ARTICLE,
        mime_type="application/json",
        payload=payload,
        modified_at=datetime.now(timezone.utc),
    )


def test_upsert_creates_directories_and_reads_payload(tmp_path: Path) -> None:
    adapter = LocalFilesystemStorageAdapter(
        base_directory=str(tmp_path),
        destination_folder_path="team/scraper",
    )
    item = _artifact(object_path="team/scraper/articles/java/item-1.json", payload=b"{\"v\":1}")

    result = adapter.upsert(destination_root_id="local-filesystem", item=item)

    expected = tmp_path / "team" / "scraper" / "articles" / "java" / "item-1.json"
    assert expected.is_file()
    assert result.provider_object_id == str(expected.resolve())
    assert adapter.exists(destination_root_id="local-filesystem", object_path=item.object_path) is True
    assert adapter.read(destination_root_id="local-filesystem", object_path=item.object_path) == b"{\"v\":1}"


def test_upsert_is_idempotent_when_payload_matches(tmp_path: Path) -> None:
    adapter = LocalFilesystemStorageAdapter(
        base_directory=str(tmp_path),
        destination_folder_path="team/scraper",
    )
    item = _artifact(object_path="team/scraper/articles/java/item-1.json", payload=b"{\"v\":1}")

    first = adapter.upsert(destination_root_id="local-filesystem", item=item)
    second = adapter.upsert(destination_root_id="local-filesystem", item=item)

    assert first.provider_object_id == second.provider_object_id
    assert first.checksum_sha256 == second.checksum_sha256


def test_upsert_overwrites_existing_object_when_payload_changes(tmp_path: Path) -> None:
    adapter = LocalFilesystemStorageAdapter(
        base_directory=str(tmp_path),
        destination_folder_path="team/scraper",
    )
    object_path = "team/scraper/articles/java/item-1.json"

    adapter.upsert(
        destination_root_id="local-filesystem",
        item=_artifact(object_path=object_path, payload=b"{\"v\":1}"),
    )
    adapter.upsert(
        destination_root_id="local-filesystem",
        item=_artifact(object_path=object_path, payload=b"{\"v\":2}"),
    )

    assert adapter.read(destination_root_id="local-filesystem", object_path=object_path) == b"{\"v\":2}"


def test_read_returns_none_for_missing_object(tmp_path: Path) -> None:
    adapter = LocalFilesystemStorageAdapter(
        base_directory=str(tmp_path),
        destination_folder_path="team/scraper",
    )

    payload = adapter.read(
        destination_root_id="local-filesystem",
        object_path="team/scraper/articles/java/missing.json",
    )

    assert payload is None


def test_rejects_object_path_outside_destination_folder_path(tmp_path: Path) -> None:
    adapter = LocalFilesystemStorageAdapter(
        base_directory=str(tmp_path),
        destination_folder_path="team/scraper",
    )

    with pytest.raises(LocalStorageAdapterError, match="must start with destination_folder_path"):
        adapter.exists(
            destination_root_id="local-filesystem",
            object_path="other-root/articles/java/item-1.json",
        )
