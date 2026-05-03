"""Shared Playwright browser sessions for source scrapers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict
from urllib.parse import SplitResult, urlsplit, urlunsplit

from denbust.config import BrowserConfig, BrowserMode

if TYPE_CHECKING:
    from playwright.async_api import Browser, BrowserContext, Page, Playwright


PLAYWRIGHT_INSTALL_HINT = "python -m playwright install chromium"
DEFAULT_CHROME_CDP_URL = "http://127.0.0.1:9222"
CHROME_CDP_START_HINT = (
    'open -na "Google Chrome" --args --remote-debugging-port=9222 '
    '--user-data-dir="$HOME/.config/denbust/chrome-wet-test"'
)


class ViewportSize(TypedDict):
    """Viewport dimensions for Playwright browser pages."""

    width: int
    height: int


RouteHandler = Callable[[Any], Awaitable[None]]


@dataclass
class ScraperBrowserSession:
    """Open browser resources for a single source fetch cycle."""

    manager: Any
    browser: Browser
    context: BrowserContext
    page: Page
    mode: BrowserMode
    attached_to_chrome: bool
    cdp_url: str | None
    route_handler: RouteHandler | None
    route_scope: str | None
    requested_user_agent: str | None
    effective_user_agent: str | None

    def diagnostics(self) -> dict[str, Any]:
        """Return non-secret browser-session diagnostics."""
        return {
            "mode": self.mode.value,
            "attached_to_chrome": self.attached_to_chrome,
            "launched_managed_chromium": not self.attached_to_chrome,
            "cdp_url": self.cdp_url,
            "route_scope": self.route_scope,
            "requested_user_agent": self.requested_user_agent,
            "effective_user_agent": self.effective_user_agent,
            "user_agent": self.effective_user_agent,
        }


async def open_scraper_browser_session(
    *,
    source_name: str,
    browser_config: BrowserConfig,
    user_agent: str,
    locale: str,
    viewport: ViewportSize,
    route_handler: RouteHandler | None,
) -> ScraperBrowserSession:
    """Open a browser session using the configured scraper browser mode."""
    try:
        from playwright.async_api import async_playwright
    except ImportError as e:
        raise RuntimeError(
            "Playwright is not installed. Install it with `python -m pip install playwright`. "
            f"Managed Chromium mode also requires `{PLAYWRIGHT_INSTALL_HINT}`."
        ) from e

    manager = async_playwright()
    playwright = await manager.__aenter__()

    try:
        if browser_config.mode is BrowserMode.CHROME_CDP:
            return await _connect_chrome_cdp_session(
                manager=manager,
                playwright=playwright,
                source_name=source_name,
                cdp_url=browser_config.chrome_cdp_url,
                requested_user_agent=user_agent,
                viewport=viewport,
                route_handler=route_handler,
            )

        return await _launch_headless_session(
            manager=manager,
            playwright=playwright,
            user_agent=user_agent,
            locale=locale,
            viewport=viewport,
            route_handler=route_handler,
        )
    except Exception as e:
        await manager.__aexit__(type(e), e, e.__traceback__)
        raise


async def _launch_headless_session(
    *,
    manager: Any,
    playwright: Playwright,
    user_agent: str,
    locale: str,
    viewport: ViewportSize,
    route_handler: RouteHandler | None,
) -> ScraperBrowserSession:
    """Launch the backward-compatible managed Chromium session."""
    try:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=user_agent,
            locale=locale,
            viewport=viewport,
        )
        if route_handler is not None:
            await context.route("**/*", route_handler)
        page = await context.new_page()
        effective_user_agent = await _get_effective_user_agent(page)
    except Exception as e:
        raise RuntimeError(
            "Chromium could not be launched for browser-backed scraping. "
            f"Install it with `{PLAYWRIGHT_INSTALL_HINT}` or set "
            "`DENBUST_BROWSER_MODE=chrome_cdp` to attach to a running Chrome instance."
        ) from e

    return ScraperBrowserSession(
        manager=manager,
        browser=browser,
        context=context,
        page=page,
        mode=BrowserMode.PLAYWRIGHT_HEADLESS,
        attached_to_chrome=False,
        cdp_url=None,
        route_handler=route_handler,
        route_scope="context" if route_handler is not None else None,
        requested_user_agent=user_agent,
        effective_user_agent=effective_user_agent,
    )


async def _connect_chrome_cdp_session(
    *,
    manager: Any,
    playwright: Playwright,
    source_name: str,
    cdp_url: str,
    requested_user_agent: str,
    viewport: ViewportSize,
    route_handler: RouteHandler | None,
) -> ScraperBrowserSession:
    """Attach to an already-running local Chrome instance over CDP."""
    sanitized_cdp_url = sanitize_cdp_url(cdp_url)
    try:
        browser = await playwright.chromium.connect_over_cdp(cdp_url)
    except Exception as e:
        raise RuntimeError(
            f"Could not attach {source_name} scraper to Chrome over CDP at {sanitized_cdp_url}. "
            "Start normal Google Chrome with remote debugging, log in or clear challenges, "
            f"then retry. On macOS: `{CHROME_CDP_START_HINT}`."
        ) from e

    context = browser.contexts[0] if browser.contexts else await browser.new_context()
    page = await context.new_page()
    await page.set_viewport_size(viewport)
    if route_handler is not None:
        await page.route("**/*", route_handler)
    effective_user_agent = await _get_effective_user_agent(page)

    return ScraperBrowserSession(
        manager=manager,
        browser=browser,
        context=context,
        page=page,
        mode=BrowserMode.CHROME_CDP,
        attached_to_chrome=True,
        cdp_url=sanitized_cdp_url,
        route_handler=route_handler,
        route_scope="page" if route_handler is not None else None,
        requested_user_agent=requested_user_agent,
        effective_user_agent=effective_user_agent,
    )


async def close_scraper_browser_session(session: ScraperBrowserSession) -> None:
    """Close scraper-owned browser resources without closing a user's Chrome profile."""
    try:
        if session.route_handler is not None:
            if session.route_scope == "page":
                await session.page.unroute("**/*", session.route_handler)
            elif session.route_scope == "context":
                await session.context.unroute("**/*", session.route_handler)
    except Exception:
        pass

    try:
        await session.page.close()
    finally:
        try:
            if session.attached_to_chrome:
                await session.browser.close()
            else:
                await session.context.close()
                await session.browser.close()
        finally:
            await session.manager.__aexit__(None, None, None)


async def _get_effective_user_agent(page: Page) -> str | None:
    """Return the effective page user agent when Playwright can evaluate it."""
    try:
        user_agent = await page.evaluate("() => navigator.userAgent")
    except Exception:
        return None
    return user_agent if isinstance(user_agent, str) else None


def sanitize_cdp_url(cdp_url: str) -> str:
    """Strip credentials and other sensitive URL components from a CDP endpoint."""
    try:
        parsed = urlsplit(cdp_url)
    except ValueError:
        return cdp_url.split("?", maxsplit=1)[0].split("#", maxsplit=1)[0]

    if not parsed.netloc:
        return cdp_url.split("?", maxsplit=1)[0].split("#", maxsplit=1)[0]

    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"

    try:
        port = parsed.port
    except ValueError:
        port = None

    if port is not None:
        host = f"{host}:{port}"

    sanitized = SplitResult(
        scheme=parsed.scheme,
        netloc=host,
        path=parsed.path,
        query="",
        fragment="",
    )
    return urlunsplit(sanitized)
