"""Tests for shared scraper browser session selection."""

from __future__ import annotations

from typing import Any

import pytest

from denbust.config import BrowserConfig, BrowserMode
from denbust.sources.browser import (
    CHROME_CDP_START_HINT,
    close_scraper_browser_session,
    open_scraper_browser_session,
    sanitize_cdp_url,
)


@pytest.mark.asyncio
async def test_open_scraper_browser_session_connects_to_chrome_cdp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CDP mode should attach to the default Chrome context and avoid closing it."""
    import playwright.async_api as playwright_async_api

    events: list[str] = []

    async def route_handler(route: Any) -> None:
        del route

    class FakePage:
        async def set_viewport_size(self, viewport: dict[str, int]) -> None:
            events.append(f"viewport:{viewport['width']}x{viewport['height']}")

        async def route(self, pattern: str, handler: Any) -> None:
            assert pattern == "**/*"
            assert handler is route_handler
            events.append("page_route")

        async def unroute(self, pattern: str, handler: Any) -> None:
            assert pattern == "**/*"
            assert handler is route_handler
            events.append("page_unroute")

        async def evaluate(self, expression: str) -> str:
            assert expression == "() => navigator.userAgent"
            events.append("evaluate_user_agent")
            return "Real Chrome UA"

        async def close(self) -> None:
            events.append("page_close")

    class FakeContext:
        async def route(self, pattern: str, handler: Any) -> None:
            assert pattern == "**/*"
            assert handler is route_handler
            events.append("context_route")

        async def unroute(self, pattern: str, handler: Any) -> None:
            assert pattern == "**/*"
            assert handler is route_handler
            events.append("context_unroute")

        async def new_page(self) -> FakePage:
            events.append("new_page")
            return FakePage()

        async def close(self) -> None:
            events.append("context_close")

    class FakeBrowser:
        def __init__(self) -> None:
            self.contexts = [FakeContext()]

        async def close(self) -> None:
            events.append("browser_disconnect")

    class FakeChromium:
        async def connect_over_cdp(self, url: str) -> FakeBrowser:
            events.append(f"connect:{url}")
            return FakeBrowser()

    class FakePlaywright:
        chromium = FakeChromium()

    class FakeManager:
        async def __aenter__(self) -> FakePlaywright:
            events.append("enter")
            return FakePlaywright()

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            del exc_type, exc, tb
            events.append("exit")

    monkeypatch.setattr(playwright_async_api, "async_playwright", lambda: FakeManager())

    session = await open_scraper_browser_session(
        source_name="mako",
        browser_config=BrowserConfig(
            mode=BrowserMode.CHROME_CDP,
            chrome_cdp_url="http://127.0.0.1:9222",
        ),
        user_agent="Mozilla/5.0",
        locale="he-IL",
        viewport={"width": 1440, "height": 2000},
        route_handler=route_handler,
    )

    assert session.diagnostics() == {
        "mode": "chrome_cdp",
        "attached_to_chrome": True,
        "launched_managed_chromium": False,
        "cdp_url": "http://127.0.0.1:9222",
        "route_scope": "page",
        "requested_user_agent": "Mozilla/5.0",
        "effective_user_agent": "Real Chrome UA",
        "user_agent": "Real Chrome UA",
    }

    await close_scraper_browser_session(session)

    assert events == [
        "enter",
        "connect:http://127.0.0.1:9222",
        "new_page",
        "viewport:1440x2000",
        "page_route",
        "evaluate_user_agent",
        "page_unroute",
        "page_close",
        "browser_disconnect",
        "exit",
    ]
    assert "context_close" not in events
    assert "context_route" not in events
    assert "context_unroute" not in events


@pytest.mark.asyncio
async def test_open_scraper_browser_session_reports_cdp_attach_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CDP attach failures should tell operators how to start local Chrome."""
    import playwright.async_api as playwright_async_api

    class FakeChromium:
        async def connect_over_cdp(self, url: str) -> object:
            assert url == "http://user:secret@127.0.0.1:9222/json?token=secret#frag"
            raise RuntimeError("connection refused")

    class FakePlaywright:
        chromium = FakeChromium()

    class FakeManager:
        async def __aenter__(self) -> FakePlaywright:
            return FakePlaywright()

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            del exc_type, exc, tb

    monkeypatch.setattr(playwright_async_api, "async_playwright", lambda: FakeManager())

    with pytest.raises(RuntimeError) as exc_info:
        await open_scraper_browser_session(
            source_name="haaretz",
            browser_config=BrowserConfig(
                mode=BrowserMode.CHROME_CDP,
                chrome_cdp_url="http://user:secret@127.0.0.1:9222/json?token=secret#frag",
            ),
            user_agent="Mozilla/5.0",
            locale="he-IL",
            viewport={"width": 1440, "height": 2000},
            route_handler=None,
        )

    assert "Could not attach haaretz scraper to Chrome over CDP" in str(exc_info.value)
    assert "http://127.0.0.1:9222/json" in str(exc_info.value)
    assert "secret" not in str(exc_info.value)
    assert CHROME_CDP_START_HINT in str(exc_info.value)


def test_sanitize_cdp_url_strips_credentials_query_and_fragment() -> None:
    """Diagnostics should not leak CDP URL credentials or bearer-like query values."""
    assert (
        sanitize_cdp_url("http://user:secret@127.0.0.1:9222/json/version?token=secret#frag")
        == "http://127.0.0.1:9222/json/version"
    )
