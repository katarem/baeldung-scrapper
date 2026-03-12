from typing import Literal, Optional

import pytest

from baeldung_scrapper.config.settings import AppSettings
from baeldung_scrapper.fetching.http_client import (
    FetchError,
    HttpxHttpClient,
    PlaywrightHttpClient,
    build_fetch_client,
)


def _local_settings(*, fetch_backend: Optional[Literal["httpx", "playwright"]] = None) -> AppSettings:
    if fetch_backend is None:
        return AppSettings(
            storage_backend="local",
            local_base_directory=".tmp/baeldung-artifacts",
        )

    return AppSettings(
        storage_backend="local",
        local_base_directory=".tmp/baeldung-artifacts",
        fetch_backend=fetch_backend,
    )


def test_build_fetch_client_defaults_to_playwright() -> None:
    settings = _local_settings()

    client = build_fetch_client(settings)

    assert isinstance(client, PlaywrightHttpClient)


def test_build_fetch_client_selects_httpx_when_configured() -> None:
    settings = _local_settings(fetch_backend="httpx")

    client = build_fetch_client(settings)

    assert isinstance(client, HttpxHttpClient)


def test_playwright_http_client_returns_page_content(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, object] = {}

    class _Response:
        status = 200

    class _Page:
        def set_extra_http_headers(self, headers: dict[str, str]) -> None:
            observed["headers"] = headers

        def goto(self, url: str, *, wait_until: str, timeout: int) -> _Response:
            observed["goto"] = {"url": url, "wait_until": wait_until, "timeout": timeout}
            return _Response()

        def wait_for_load_state(self, state: str, timeout: int) -> None:
            observed["wait_for_load_state"] = {"state": state, "timeout": timeout}

        def wait_for_timeout(self, timeout_ms: int) -> None:
            observed["wait_for_timeout"] = timeout_ms

        def content(self) -> str:
            return "<html>ok</html>"

    class _Context:
        def new_page(self) -> _Page:
            return _Page()

        def close(self) -> None:
            observed["context_closed"] = True

    class _Browser:
        def new_context(self, **kwargs: object) -> _Context:
            observed["new_context"] = kwargs
            return _Context()

        def close(self) -> None:
            observed["browser_closed"] = True

    class _Launcher:
        def launch(self, *, headless: bool) -> _Browser:
            observed["headless"] = headless
            return _Browser()

    class _Playwright:
        chromium = _Launcher()

    class _Manager:
        def __enter__(self) -> _Playwright:
            return _Playwright()

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    def _fake_import() -> tuple[object, type[Exception]]:
        return lambda: _Manager(), TimeoutError

    monkeypatch.setattr("baeldung_scrapper.fetching.http_client._import_playwright_sync_api", _fake_import)

    client = PlaywrightHttpClient()
    payload = client.get_text(url="https://www.baeldung.com/java", timeout_seconds=2.5)

    assert payload == "<html>ok</html>"
    assert observed["goto"] == {
        "url": "https://www.baeldung.com/java",
        "wait_until": "domcontentloaded",
        "timeout": 2500,
    }
    assert observed["wait_for_load_state"] == {"state": "networkidle", "timeout": 2500}
    assert observed["headless"] is True
    assert observed["headers"] == {
        "Accept-Language": "en-US,en;q=0.9",
        "Upgrade-Insecure-Requests": "1",
    }
    assert observed["new_context"] == {
        "user_agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "locale": "en-US",
        "viewport": {"width": 1366, "height": 768},
    }
    assert observed["context_closed"] is True
    assert observed["browser_closed"] is True


def test_playwright_http_client_raises_fetch_error_for_http_403(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Response:
        status = 403

    class _Page:
        def set_extra_http_headers(self, headers: dict[str, str]) -> None:
            _ = headers

        def goto(self, url: str, *, wait_until: str, timeout: int) -> _Response:
            _ = (url, wait_until, timeout)
            return _Response()

        def wait_for_load_state(self, state: str, timeout: int) -> None:
            _ = (state, timeout)

        def wait_for_timeout(self, timeout_ms: int) -> None:
            _ = timeout_ms

        def content(self) -> str:
            return "<html>blocked</html>"

    class _Context:
        def new_page(self) -> _Page:
            return _Page()

        def close(self) -> None:
            return None

    class _Browser:
        def new_context(self, **kwargs: object) -> _Context:
            _ = kwargs
            return _Context()

        def close(self) -> None:
            return None

    class _Launcher:
        def launch(self, *, headless: bool) -> _Browser:
            _ = headless
            return _Browser()

    class _Playwright:
        chromium = _Launcher()

    class _Manager:
        def __enter__(self) -> _Playwright:
            return _Playwright()

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    def _fake_import() -> tuple[object, type[Exception]]:
        return lambda: _Manager(), TimeoutError

    monkeypatch.setattr("baeldung_scrapper.fetching.http_client._import_playwright_sync_api", _fake_import)

    with pytest.raises(FetchError, match="failed to fetch url"):
        PlaywrightHttpClient().get_text(url="https://www.baeldung.com/java", timeout_seconds=2)


def test_playwright_http_client_retries_cloudflare_challenge_once(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, int] = {"goto_calls": 0, "wait_calls": 0}

    class _Response:
        status = 200

    class _Page:
        def set_extra_http_headers(self, headers: dict[str, str]) -> None:
            _ = headers

        def goto(self, url: str, *, wait_until: str, timeout: int) -> _Response:
            _ = (url, wait_until, timeout)
            observed["goto_calls"] += 1
            return _Response()

        def wait_for_load_state(self, state: str, timeout: int) -> None:
            _ = (state, timeout)

        def wait_for_timeout(self, timeout_ms: int) -> None:
            _ = timeout_ms
            observed["wait_calls"] += 1

        def content(self) -> str:
            if observed["goto_calls"] == 1:
                return "<html><body>Just a moment...</body></html>"
            return "<html><body>real content</body></html>"

    class _Context:
        def new_page(self) -> _Page:
            return _Page()

        def close(self) -> None:
            return None

    class _Browser:
        def new_context(self, **kwargs: object) -> _Context:
            _ = kwargs
            return _Context()

        def close(self) -> None:
            return None

    class _Launcher:
        def launch(self, *, headless: bool) -> _Browser:
            _ = headless
            return _Browser()

    class _Playwright:
        chromium = _Launcher()

    class _Manager:
        def __enter__(self) -> _Playwright:
            return _Playwright()

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    def _fake_import() -> tuple[object, type[Exception]]:
        return lambda: _Manager(), TimeoutError

    monkeypatch.setattr("baeldung_scrapper.fetching.http_client._import_playwright_sync_api", _fake_import)

    payload = PlaywrightHttpClient(max_attempts=2, challenge_wait_ms=10).get_text(
        url="https://www.baeldung.com/java", timeout_seconds=2
    )

    assert payload == "<html><body>real content</body></html>"
    assert observed["goto_calls"] == 2
    assert observed["wait_calls"] == 1
