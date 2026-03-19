"""Normalization helpers for the news_items dataset."""

from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING_QUERY_PARAMS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "oref",
    "partner",
    "utm_campaign",
    "utm_content",
    "utm_id",
    "utm_medium",
    "utm_source",
    "utm_term",
}


def canonicalize_news_url(url: str) -> str:
    """Normalize a news article URL into a deterministic canonical identity."""
    split = urlsplit(url)
    scheme = "https"
    netloc = split.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    cleaned_query = [
        (key, value)
        for key, value in parse_qsl(split.query, keep_blank_values=False)
        if key.lower() not in TRACKING_QUERY_PARAMS
    ]
    path = split.path.rstrip("/") or "/"
    query = urlencode(cleaned_query, doseq=True)
    canonical = urlunsplit((scheme, netloc, path, query, ""))
    return canonical.rstrip("?")


def build_news_item_id(canonical_url: str) -> str:
    """Build a stable deterministic identifier from the canonical URL."""
    digest = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()
    return f"newsitem_{digest[:24]}"


def source_domain_from_url(url: str) -> str:
    """Extract the normalized source domain from a URL."""
    return urlsplit(canonicalize_news_url(url)).netloc


def deduplicate_strings(values: list[str]) -> list[str]:
    """Return unique non-empty strings in first-seen order."""
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        candidate = " ".join(value.split()).strip()
        if not candidate:
            continue
        key = candidate.casefold()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(candidate)
    return normalized
