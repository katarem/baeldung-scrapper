"""Cloud storage adapters and provider wiring."""

from baeldung_scrapper.infrastructure.cloud_storage.factory import (
    ProviderRuntimeBinding,
    build_cloud_storage_provider,
)

__all__ = ["ProviderRuntimeBinding", "build_cloud_storage_provider"]
