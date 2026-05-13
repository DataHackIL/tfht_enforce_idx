"""Candidate-level filtering policy for discovery search results."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

from denbust.discovery.models import DiscoveredCandidate, ProducerKind

_ALWAYS_UNSUPPORTED_SEARCH_DOMAINS: frozenset[str] = frozenset(
    {
        "x.com",
        "twitter.com",
        "play.google.com",
        "apps.apple.com",
        "itunes.apple.com",
        "morfix.co.il",
        "context.reverso.net",
        "dictionary.reverso.net",
        "wiktionary.org",
        "pealim.com",
    }
)

_SOCIAL_PROFILE_DOMAINS: frozenset[str] = frozenset(
    {
        "instagram.com",
        "linkedin.com",
        "tiktok.com",
        "youtube.com",
    }
)

_SOCIAL_PROFILE_PATH_PREFIXES: dict[str, tuple[str, ...]] = {
    "linkedin.com": ("/company/", "/in/", "/school/", "/showcase/"),
    "youtube.com": ("/@", "/channel/", "/c/", "/user/"),
}

_SOCIAL_POST_PATH_PREFIXES: dict[str, tuple[str, ...]] = {
    "instagram.com": ("/p/", "/reel/", "/tv/"),
}


@dataclass(frozen=True)
class SearchNoiseClassification:
    """Classification for a retained but non-scrapeable search-result surface."""

    reason: str
    matched_domain: str


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
    return parsed.path or "/"


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


def _is_social_profile_candidate(discovered: DiscoveredCandidate) -> bool:
    domain = candidate_domain(discovered)
    matched_domain = match_domain(domain, _SOCIAL_PROFILE_DOMAINS)
    if matched_domain is None:
        return False
    path = candidate_path(discovered)
    if matched_domain in _SOCIAL_PROFILE_PATH_PREFIXES:
        return any(
            path.startswith(prefix) for prefix in _SOCIAL_PROFILE_PATH_PREFIXES[matched_domain]
        )
    if matched_domain in _SOCIAL_POST_PATH_PREFIXES:
        return not any(
            path.startswith(prefix) for prefix in _SOCIAL_POST_PATH_PREFIXES[matched_domain]
        )
    if matched_domain == "tiktok.com":
        return "/video/" not in path
    return False


def classify_search_noise(
    discovered: DiscoveredCandidate,
) -> SearchNoiseClassification | None:
    """Classify obvious non-article search-result surfaces before scrape selection."""
    if discovered.producer_kind is not ProducerKind.SEARCH_ENGINE:
        return None
    domain = candidate_domain(discovered)
    if (matched_domain := match_domain(domain, _ALWAYS_UNSUPPORTED_SEARCH_DOMAINS)) is not None:
        return SearchNoiseClassification(
            reason="unsupported_search_domain",
            matched_domain=matched_domain,
        )
    if _is_social_profile_candidate(discovered):
        matched_domain = match_domain(domain, _SOCIAL_PROFILE_DOMAINS)
        if matched_domain is not None:
            return SearchNoiseClassification(
                reason="social_profile",
                matched_domain=matched_domain,
            )
    return None
