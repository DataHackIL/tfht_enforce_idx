"""Candidate-level filtering policy for discovery search results."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlparse

from denbust.discovery.models import DiscoveredCandidate, ProducerKind

_UTILITY_SEARCH_DOMAINS: frozenset[str] = frozenset(
    {
        "morfix.co.il",
        "context.reverso.net",
        "dictionary.reverso.net",
        "wiktionary.org",
        "pealim.com",
    }
)

_APP_STORE_DOMAINS: frozenset[str] = frozenset(
    {"play.google.com", "apps.apple.com", "itunes.apple.com"}
)

_SOCIAL_PROFILE_DOMAINS: frozenset[str] = frozenset(
    {
        "facebook.com",
        "instagram.com",
        "linkedin.com",
        "tiktok.com",
        "twitter.com",
        "x.com",
        "youtube.com",
    }
)

# Domains that consistently produce off-topic candidates and waste classifier
# API budget.  All subdomains are matched (e.g. sport1.maariv.co.il matches
# "sport1.maariv.co.il").  Add new entries here when a domain is confirmed
# irrelevant via review-workbench bulk-exclusion or diagnostics evidence.
_IRRELEVANT_CONTENT_DOMAINS: frozenset[str] = frozenset(
    {
        "sport1.maariv.co.il",  # sports vertical — consistently off-topic
        "he.wikipedia.org",  # encyclopedia — never enforcement news
        "srugim.co.il",  # Orthodox community site — off-topic
        "themarker.com",  # financial/business news — off-topic
        "collab.mako.co.il",  # Mako user-generated content subdomain
        "kikar.co.il",  # ultra-Orthodox news — off-topic
        "atzat-nefesh.org",  # mental health org — off-topic
        "il.bongogirls.ru",  # noise
    }
)

_SOCIAL_PROFILE_PATH_PREFIXES: dict[str, tuple[str, ...]] = {
    "linkedin.com": ("/company/", "/in/", "/school/", "/showcase/"),
    "youtube.com": ("/@", "/channel/", "/c/", "/user/"),
}

_SOCIAL_POST_PATH_PREFIXES: dict[str, tuple[str, ...]] = {
    "facebook.com": ("/permalink.php", "/posts/", "/share/", "/story.php", "/watch/"),
    "instagram.com": ("/p/", "/reel/", "/tv/"),
    "linkedin.com": ("/feed/update/", "/posts/", "/pulse/"),
    "youtube.com": ("/shorts/", "/watch"),
}


_EXCLUDED_TITLE_TERMS: frozenset[str] = frozenset(
    {
        # ── military / geopolitical ──────────────────────────────────────────
        'צה"ל',  # IDF — ASCII double-quote form
        "צה״ל",  # IDF — Hebrew gershayim form
        "צהל",  # IDF — no punctuation form
        "איראן",  # Iran
        "חיזבאללה",  # Hezbollah
        "עזה",  # Gaza
        "קטאר",  # Qatar — geopolitical/sports noise
        "ונצואלה",  # Venezuela
        "מדורו",  # Maduro — Venezuelan politics
        "ממדאני",  # Madani — UN SG noise
        # ── politics ─────────────────────────────────────────────────────────
        "נתניהו",  # Netanyahu
        "טראמפ",  # Trump
        "בלפור",  # Balfour St protests — covers בלפור and בבלפור
        "פוליטי",  # generic political commentary
        # ── finance / business ───────────────────────────────────────────────
        "מניות",  # stocks / financial markets
        "שוק ההון",  # capital markets
        "גלובס",  # Globes financial news brand
        "themarker",  # TheMarker financial news brand (case-insensitive match)
        # ── supermarkets / retail noise ──────────────────────────────────────
        "שופרסל",  # Shufersal supermarket chain
        "ויקטורי",  # Victory supermarket chain
        "רמי לוי",  # Rami Levy supermarket chain
        "ksp",  # KSP electronics chain (case-insensitive)
        # ── sports ───────────────────────────────────────────────────────────
        "ספורט",  # sports
        "מכבי",  # Maccabi sports teams (Maccabi Haifa, Tel Aviv, etc.)
        # ── media brand names appearing as title suffixes ────────────────────
        "ויקיפדיה",  # Wikipedia entries
        "סרוגים",  # Srugim site brand name
        "כיכר השבת",  # Kikar HaShabbat ultra-Orthodox site brand
        "וואלה חדשות",  # Walla News brand in title (topic/nav pages)
        # ── celebrity / entertainment ────────────────────────────────────────
        "אייל גולן",  # Israeli singer — consistently off-topic
    }
)


def globally_excluded_title_terms() -> frozenset[str]:
    """Return the current set of title terms that short-circuit candidate processing."""
    return _EXCLUDED_TITLE_TERMS


class SearchNoiseReason(StrEnum):
    """Stable reason values for search-result noise classification."""

    APP_STORE = "app_store"
    SOCIAL_PROFILE = "social_profile"
    TITLE_KEYWORD_MATCH = "title_keyword_match"
    UNSUPPORTED_SEARCH_DOMAIN = "unsupported_search_domain"
    IRRELEVANT_CONTENT_DOMAIN = "irrelevant_content_domain"


@dataclass(frozen=True)
class SearchNoiseClassification:
    """Classification for a retained but non-scrapeable search-result surface."""

    reason: SearchNoiseReason
    matched_domain: str = ""
    matched_keyword: str = ""


def globally_excluded_search_domains() -> frozenset[str]:
    """Return the set of domains excluded from all broad/taxonomy search queries.

    These are domains that are structurally off-topic (sports verticals, utility
    sites, etc.) and should never consume search-engine quota.  The list mirrors
    ``_IRRELEVANT_CONTENT_DOMAINS``; callers should not duplicate it.
    """
    return _IRRELEVANT_CONTENT_DOMAINS


def normalize_domain(domain: str | None) -> str | None:
    """Normalize hosts for candidate-filter comparisons."""
    if domain is None:
        return None
    normalized = domain.strip().casefold()
    if not normalized:
        return None
    if normalized.startswith("www."):
        normalized = normalized[4:]
    return normalized or None


def candidate_domain(discovered: DiscoveredCandidate) -> str | None:
    """Return the normalized candidate host, preferring explicit model domain."""
    normalized_domain = normalize_domain(discovered.domain)
    if normalized_domain is not None:
        return normalized_domain
    return normalize_domain(
        urlparse(str(discovered.canonical_url or discovered.candidate_url)).netloc
    )


def candidate_path(discovered: DiscoveredCandidate) -> str:
    """Return the path for the current candidate identity URL."""
    parsed = urlparse(str(discovered.canonical_url or discovered.candidate_url))
    return (parsed.path or "/").casefold()


def match_domain(domain: str | None, configured_domains: frozenset[str]) -> str | None:
    """Return the configured base domain matched by a host, including subdomains."""
    if domain is None:
        return None
    return next(
        (
            configured
            for configured in sorted(configured_domains, key=len, reverse=True)
            if domain == configured or domain.endswith(f".{configured}")
        ),
        None,
    )


def _is_app_store_url(discovered: DiscoveredCandidate) -> bool:
    domain = candidate_domain(discovered)
    matched_domain = match_domain(domain, _APP_STORE_DOMAINS)
    if matched_domain is None:
        return False
    path = candidate_path(discovered)
    if matched_domain == "play.google.com":
        return path.startswith("/store/apps/")
    return path == "/app" or "/app/" in path


def _is_x_or_twitter_post_path(path: str) -> bool:
    segments = [segment for segment in path.split("/") if segment]
    return (
        len(segments) >= 3 and segments[1] in {"status", "statuses"} and segments[2].isdigit()
    ) or (
        len(segments) >= 4
        and segments[0] == "i"
        and segments[1] == "web"
        and segments[2] == "status"
        and segments[3].isdigit()
    )


def _is_tiktok_video_path(path: str) -> bool:
    segments = [segment for segment in path.split("/") if segment]
    return (
        len(segments) >= 3
        and segments[0].startswith("@")
        and segments[1] == "video"
        and segments[2].isdigit()
    )


def _is_social_profile_candidate(discovered: DiscoveredCandidate) -> bool:
    domain = candidate_domain(discovered)
    matched_domain = match_domain(domain, _SOCIAL_PROFILE_DOMAINS)
    if matched_domain is None:
        return False
    path = candidate_path(discovered)
    if matched_domain in {"x.com", "twitter.com"}:
        return not _is_x_or_twitter_post_path(path)
    if matched_domain == "tiktok.com":
        return not _is_tiktok_video_path(path)
    if matched_domain in _SOCIAL_PROFILE_PATH_PREFIXES:
        return any(
            path.startswith(prefix) for prefix in _SOCIAL_PROFILE_PATH_PREFIXES[matched_domain]
        )
    if matched_domain in _SOCIAL_POST_PATH_PREFIXES:
        return not any(
            path.startswith(prefix) for prefix in _SOCIAL_POST_PATH_PREFIXES[matched_domain]
        )
    return False


def classify_search_noise(
    discovered: DiscoveredCandidate,
) -> SearchNoiseClassification | None:
    """Classify obvious non-article search-result surfaces before scrape selection."""
    if discovered.producer_kind is not ProducerKind.SEARCH_ENGINE:
        return None
    domain = candidate_domain(discovered)
    if _is_app_store_url(discovered):
        matched_domain = match_domain(domain, _APP_STORE_DOMAINS)
        if matched_domain is not None:
            return SearchNoiseClassification(
                reason=SearchNoiseReason.APP_STORE,
                matched_domain=matched_domain,
            )
    if (matched_domain := match_domain(domain, _UTILITY_SEARCH_DOMAINS)) is not None:
        return SearchNoiseClassification(
            reason=SearchNoiseReason.UNSUPPORTED_SEARCH_DOMAIN,
            matched_domain=matched_domain,
        )
    if (matched_domain := match_domain(domain, _IRRELEVANT_CONTENT_DOMAINS)) is not None:
        return SearchNoiseClassification(
            reason=SearchNoiseReason.IRRELEVANT_CONTENT_DOMAIN,
            matched_domain=matched_domain,
        )
    if _is_social_profile_candidate(discovered):
        matched_domain = match_domain(domain, _SOCIAL_PROFILE_DOMAINS)
        if matched_domain is not None:
            return SearchNoiseClassification(
                reason=SearchNoiseReason.SOCIAL_PROFILE,
                matched_domain=matched_domain,
            )
    return None


def classify_title_noise(
    discovered: DiscoveredCandidate,
) -> SearchNoiseClassification | None:
    """Classify candidates whose title contains an excluded keyword.

    Applies to all producer kinds — this is a pre-scrape cost filter, not
    a search-result surface filter.  Returns the first matching term.
    """
    title = (discovered.title or "").casefold()
    if not title:
        return None
    for term in sorted(_EXCLUDED_TITLE_TERMS, key=len, reverse=True):
        if term.casefold() in title:
            return SearchNoiseClassification(
                reason=SearchNoiseReason.TITLE_KEYWORD_MATCH,
                matched_keyword=term,
            )
    return None
