from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from baeldung_scrapper.config.settings import AppSettings


class FetchError(RuntimeError):
    """Raised when source content cannot be fetched."""


class HttpClient(Protocol):
    def get_text(self, *, url: str, timeout_seconds: float) -> str:
        ...


@dataclass(frozen=True)
class HttpxHttpClient(HttpClient):
    def get_text(self, *, url: str, timeout_seconds: float) -> str:
        try:
            import httpx
        except ImportError as exc:
            raise RuntimeError("httpx is required for runtime fetching. Install project dependencies.") from exc

        try:
            response = httpx.get(url, timeout=timeout_seconds)
            response.raise_for_status()
        except Exception as exc:
            raise FetchError(f"failed to fetch url: {url}") from exc
        return response.text


@dataclass(frozen=True)
class PlaywrightHttpClient(HttpClient):
    browser_name: str = "chromium"
    max_attempts: int = 2
    challenge_wait_ms: int = 3500
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    def get_text(self, *, url: str, timeout_seconds: float) -> str:
        try:
            sync_playwright, timeout_error = _import_playwright_sync_api()
        except ImportError as exc:
            raise RuntimeError(
                "playwright is required for runtime fetching. Install project dependencies and run 'playwright install chromium'."
            ) from exc

        timeout_ms = max(int(timeout_seconds * 1000), 1)

        try:
            with sync_playwright() as playwright:
                launcher = getattr(playwright, self.browser_name, None)
                if launcher is None:
                    raise FetchError(f"failed to fetch url: {url}")

                browser = launcher.launch(headless=True)
                context = browser.new_context(
                    user_agent=self.user_agent,
                    locale="en-US",
                    viewport={"width": 1366, "height": 768},
                )
                try:
                    page = context.new_page()
                    page.set_extra_http_headers(
                        {
                            "Accept-Language": "en-US,en;q=0.9",
                            "Upgrade-Insecure-Requests": "1",
                        }
                    )

                    attempts = max(self.max_attempts, 1)
                    for attempt in range(1, attempts + 1):
                        response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                        status = getattr(response, "status", 200) if response is not None else 200

                        try:
                            page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 5000))
                        except timeout_error:
                            pass

                        content = page.content()
                        challenge_detected = _looks_like_cloudflare_challenge(content)

                        if status < 400 and not challenge_detected:
                            return content

                        if attempt < attempts:
                            page.wait_for_timeout(self.challenge_wait_ms)
                            continue

                        raise FetchError(f"failed to fetch url: {url}")

                    raise FetchError(f"failed to fetch url: {url}")
                finally:
                    context.close()
                    browser.close()
        except timeout_error as exc:
            raise FetchError(f"failed to fetch url: {url}") from exc
        except FetchError:
            raise
        except Exception as exc:
            raise FetchError(f"failed to fetch url: {url}") from exc


def build_fetch_client(settings: AppSettings) -> HttpClient:
    if settings.fetch_backend == "playwright":
        return PlaywrightHttpClient()
    return HttpxHttpClient()


def _import_playwright_sync_api() -> tuple[Any, type[Exception]]:
    sync_api = importlib.import_module("playwright.sync_api")
    return sync_api.sync_playwright, sync_api.TimeoutError


def _looks_like_cloudflare_challenge(html: str) -> bool:
    normalized = html.lower()
    challenge_tokens = (
        "cf-mitigated",
        "challenge-platform",
        "just a moment",
        "verify you are human",
        "cloudflare",
    )
    return any(token in normalized for token in challenge_tokens)
