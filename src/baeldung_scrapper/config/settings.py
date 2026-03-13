from __future__ import annotations

from typing import Literal, Optional

from pydantic import AliasChoices, Field, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from baeldung_scrapper.domain.models.storage_layout import (
    normalize_relative_path,
    normalize_storage_root,
)
class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SCRAPER_", extra="forbid")

    environment: str = "dev"

    destination_folder_path: str = Field(
        default="baeldung-java-daily",
        validation_alias=AliasChoices("destination_folder_path", "storage_root_prefix"),
    )
    storage_manifest_path: str = "manifests/latest.json"

    source_base_url: str = "https://www.baeldung.com"
    source_timeout_seconds: float = Field(default=15.0, gt=0.0, le=60.0)
    fetch_backend: Literal["httpx", "playwright"] = "playwright"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    max_retry_attempts: int = Field(default=4, ge=0, le=8)
    retry_backoff_base_seconds: float = Field(default=1.0, gt=0.0, le=30.0)
    retry_backoff_cap_seconds: float = Field(default=30.0, gt=0.0, le=300.0)

    fetch_concurrency: int = Field(default=2, ge=1, le=6)
    min_request_interval_ms: int = Field(default=750, ge=0, le=10_000)

    storage_backend: Literal["s3", "local"] = "s3"
    local_base_directory: Optional[str] = None

    s3_endpoint: Optional[str] = None
    s3_region: Optional[str] = None
    s3_bucket: Optional[str] = None
    s3_access_key_id: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    s3_force_path_style: bool = True
    s3_object_acl: Literal["private", "public-read"] = "private"

    @field_validator("source_base_url")
    @classmethod
    def validate_source_base_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("source_base_url must be an absolute URL")
        return value.rstrip("/")

    @field_validator("destination_folder_path")
    @classmethod
    def validate_destination_folder_path(cls, value: str) -> str:
        return normalize_storage_root(value)

    @field_validator("local_base_directory", "s3_endpoint", "s3_region", "s3_bucket", "s3_access_key_id", "s3_secret_access_key", mode="before")
    @classmethod
    def normalize_optional_string_settings(cls, value: object) -> object:
        if not isinstance(value, str):
            return value

        normalized = value.strip()
        if not normalized:
            return None
        return normalized

    @field_validator("s3_endpoint")
    @classmethod
    def validate_s3_endpoint(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        if not value.startswith(("http://", "https://")):
            raise ValueError("s3_endpoint must be an absolute URL")
        return value.rstrip("/")

    @field_validator("storage_manifest_path")
    @classmethod
    def validate_storage_manifest_path(cls, value: str) -> str:
        normalized = normalize_relative_path(value)
        if not normalized.endswith(".json"):
            raise ValueError("storage_manifest_path must point to a .json file")
        return normalized

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        return value.strip().upper()

    @model_validator(mode="after")
    def validate_provider_settings(self) -> "AppSettings":
        if self.retry_backoff_cap_seconds < self.retry_backoff_base_seconds:
            raise ValueError("retry_backoff_cap_seconds must be >= retry_backoff_base_seconds")

        if self.storage_backend == "s3":
            missing_fields = []
            for field_name in (
                "s3_endpoint",
                "s3_region",
                "s3_bucket",
                "s3_access_key_id",
                "s3_secret_access_key",
            ):
                if getattr(self, field_name) is None:
                    missing_fields.append(field_name)

            if missing_fields:
                fields = ", ".join(missing_fields)
                raise ValueError(f"S3 settings must not be empty: {fields}")

        if self.storage_backend == "local" and self.local_base_directory is None:
            raise ValueError("local_base_directory is required when storage_backend=local")

        return self

    @computed_field
    @property
    def storage_root_prefix(self) -> str:
        return self.destination_folder_path

    def resolve_destination_root_id(self) -> str:
        if self.storage_backend == "local":
            return "local-filesystem"
        if self.s3_bucket is None:
            raise ValueError("s3_bucket is required when storage_backend=s3")
        return self.s3_bucket
