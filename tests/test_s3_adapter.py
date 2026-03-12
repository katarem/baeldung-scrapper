from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from baeldung_scrapper.domain.ports.cloud_storage import ArtifactKind, ArtifactObject
from baeldung_scrapper.infrastructure.cloud_storage.s3 import CloudStorageAdapterError, S3Object, S3StorageAdapter


@dataclass
class _StoredObject:
    payload: bytes
    checksum_sha256: str


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], _StoredObject] = {}
        self.put_calls = 0

    def get_object(self, *, bucket: str, object_key: str) -> bytes | None:
        stored = self.objects.get((bucket, object_key))
        if stored is None:
            return None
        return stored.payload

    def head_object(self, *, bucket: str, object_key: str) -> S3Object | None:
        stored = self.objects.get((bucket, object_key))
        if stored is None:
            return None
        return S3Object(object_key=object_key, checksum_sha256=stored.checksum_sha256)

    def put_object(
        self,
        *,
        bucket: str,
        object_key: str,
        mime_type: str,
        payload: bytes,
        checksum_sha256: str,
    ) -> S3Object:
        _ = mime_type
        self.put_calls += 1
        self.objects[(bucket, object_key)] = _StoredObject(payload=payload, checksum_sha256=checksum_sha256)
        return S3Object(object_key=object_key, checksum_sha256=checksum_sha256)


def _artifact(*, object_path: str, payload: bytes) -> ArtifactObject:
    return ArtifactObject(
        object_path=object_path,
        kind=ArtifactKind.ARTICLE,
        mime_type="application/json",
        payload=payload,
        modified_at=datetime.now(timezone.utc),
    )


def test_upsert_creates_object_and_exists_reads_payload() -> None:
    client = FakeS3Client()
    adapter = S3StorageAdapter(client=client, destination_folder_path="team/scraper")
    item = _artifact(object_path="team/scraper/articles/java/item-1.json", payload=b"{\"v\":1}")

    result = adapter.upsert(destination_root_id="railway-bucket", item=item)

    assert result.provider_object_id == "railway-bucket/team/scraper/articles/java/item-1.json"
    assert adapter.exists(destination_root_id="railway-bucket", object_path=item.object_path) is True
    assert adapter.read(destination_root_id="railway-bucket", object_path=item.object_path) == b"{\"v\":1}"


def test_upsert_is_idempotent_when_checksum_matches() -> None:
    client = FakeS3Client()
    adapter = S3StorageAdapter(client=client, destination_folder_path="team/scraper")
    item = _artifact(object_path="team/scraper/articles/java/item-1.json", payload=b"{\"v\":1}")

    first = adapter.upsert(destination_root_id="railway-bucket", item=item)
    second = adapter.upsert(destination_root_id="railway-bucket", item=item)

    assert first.provider_object_id == second.provider_object_id
    assert client.put_calls == 1


def test_upsert_overwrites_existing_object_when_payload_changes() -> None:
    client = FakeS3Client()
    adapter = S3StorageAdapter(client=client, destination_folder_path="team/scraper")
    object_path = "team/scraper/articles/java/item-1.json"
    first = _artifact(object_path=object_path, payload=b"{\"v\":1}")
    second = _artifact(object_path=object_path, payload=b"{\"v\":2}")

    initial = adapter.upsert(destination_root_id="railway-bucket", item=first)
    updated = adapter.upsert(destination_root_id="railway-bucket", item=second)

    assert updated.provider_object_id == initial.provider_object_id
    assert client.put_calls == 2
    assert adapter.read(destination_root_id="railway-bucket", object_path=object_path) == b"{\"v\":2}"


def test_read_returns_none_when_object_does_not_exist() -> None:
    client = FakeS3Client()
    adapter = S3StorageAdapter(client=client, destination_folder_path="team/scraper")

    payload = adapter.read(
        destination_root_id="railway-bucket",
        object_path="team/scraper/articles/java/missing.json",
    )

    assert payload is None


def test_rejects_object_path_outside_destination_folder_path() -> None:
    client = FakeS3Client()
    adapter = S3StorageAdapter(client=client, destination_folder_path="team/scraper")

    with pytest.raises(CloudStorageAdapterError, match="must start with destination_folder_path"):
        adapter.exists(
            destination_root_id="railway-bucket",
            object_path="other-root/articles/java/item-1.json",
        )
