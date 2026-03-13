from typing import Any

import pytest

from baeldung_scrapper.config.settings import AppSettings
from baeldung_scrapper.domain.models.storage_layout import build_manifest_path


def _minimal_s3_settings() -> dict[str, Any]:
    return {
        "s3_endpoint": "https://bucket.up.railway.app",
        "s3_region": "us-east-1",
        "s3_bucket": "baeldung-artifacts",
        "s3_access_key_id": "access-key",
        "s3_secret_access_key": "secret-key",
    }


def test_defaults_to_path_style_for_s3_compatible_endpoints() -> None:
    settings = AppSettings(**_minimal_s3_settings())
    assert settings.s3_force_path_style is True


def test_defaults_s3_object_acl_to_private() -> None:
    settings = AppSettings(**_minimal_s3_settings())

    assert settings.s3_object_acl == "private"


def test_accepts_public_read_s3_object_acl() -> None:
    settings = AppSettings(**{**_minimal_s3_settings(), "s3_object_acl": "public-read"})

    assert settings.s3_object_acl == "public-read"


def test_defaults_fetch_backend_to_playwright() -> None:
    settings = AppSettings(**_minimal_s3_settings())

    assert settings.fetch_backend == "playwright"


def test_defaults_log_level_to_info() -> None:
    settings = AppSettings(**_minimal_s3_settings())

    assert settings.log_level == "INFO"


def test_normalizes_log_level_to_uppercase() -> None:
    settings = AppSettings(**{**_minimal_s3_settings(), "log_level": "warning"})

    assert settings.log_level == "WARNING"


def test_requires_non_empty_s3_settings() -> None:
    with pytest.raises(ValueError, match="S3 settings must not be empty"):
        AppSettings(**{**_minimal_s3_settings(), "s3_bucket": "  "})


def test_requires_absolute_s3_endpoint_url() -> None:
    with pytest.raises(ValueError, match="s3_endpoint"):
        AppSettings(**{**_minimal_s3_settings(), "s3_endpoint": "bucket.up.railway.app"})


def test_validates_backoff_cap_not_smaller_than_base() -> None:
    with pytest.raises(ValueError, match="retry_backoff_cap_seconds"):
        AppSettings(
            **_minimal_s3_settings(),
            retry_backoff_base_seconds=5,
            retry_backoff_cap_seconds=1,
        )


def test_normalizes_source_base_url() -> None:
    settings = AppSettings(
        **_minimal_s3_settings(),
        source_base_url="https://www.baeldung.com/",
    )
    assert settings.source_base_url == "https://www.baeldung.com"


def test_normalizes_storage_root_and_manifest_path() -> None:
    settings = AppSettings(
        **_minimal_s3_settings(),
        destination_folder_path="team//baeldung",
        storage_manifest_path="state//daily-manifest.json",
    )
    assert settings.destination_folder_path == "team/baeldung"
    assert settings.storage_root_prefix == "team/baeldung"
    assert settings.storage_manifest_path == "state/daily-manifest.json"


def test_accepts_legacy_storage_root_prefix_field_name() -> None:
    settings = AppSettings.model_validate(
        {**_minimal_s3_settings(), "storage_root_prefix": "team/legacy"}
    )
    assert settings.destination_folder_path == "team/legacy"


def test_rejects_non_json_manifest_path() -> None:
    with pytest.raises(ValueError, match="storage_manifest_path"):
        AppSettings(
            **_minimal_s3_settings(),
            storage_manifest_path="state/daily-manifest.txt",
        )


def test_rejects_absolute_destination_folder_path() -> None:
    with pytest.raises(ValueError, match="relative path"):
        AppSettings(**{**_minimal_s3_settings(), "destination_folder_path": "/team/baeldung"})


def test_destination_folder_path_resolves_manifest_location() -> None:
    settings = AppSettings(
        **_minimal_s3_settings(),
        destination_folder_path="team/scraper-a",
        storage_manifest_path="state/daily.json",
    )

    assert (
        build_manifest_path(
            root_prefix=settings.destination_folder_path,
            manifest_relative_path=settings.storage_manifest_path,
        )
        == "team/scraper-a/state/daily.json"
    )


def test_resolve_destination_root_id_for_s3_provider() -> None:
    settings = AppSettings(**{**_minimal_s3_settings(), "s3_bucket": "railway-bucket"})

    assert settings.resolve_destination_root_id() == "railway-bucket"


def test_local_backend_allows_missing_s3_configuration() -> None:
    settings = AppSettings(storage_backend="local", local_base_directory=".tmp/baeldung-artifacts")

    assert settings.storage_backend == "local"
    assert settings.local_base_directory == ".tmp/baeldung-artifacts"


def test_local_backend_requires_local_base_directory() -> None:
    with pytest.raises(ValueError, match="local_base_directory"):
        AppSettings(storage_backend="local")


def test_s3_backend_requires_s3_configuration() -> None:
    with pytest.raises(ValueError, match="S3 settings must not be empty"):
        AppSettings(storage_backend="s3")


def test_resolve_destination_root_id_for_local_backend() -> None:
    settings = AppSettings(storage_backend="local", local_base_directory="./tmp/artifacts")

    assert settings.resolve_destination_root_id() == "local-filesystem"
