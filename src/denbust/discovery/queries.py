"""Query builders for multi-engine discovery."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

from denbust.config import Config, SourceConfig, SourceType
from denbust.discovery.models import DiscoveryQuery, DiscoveryQueryKind
from denbust.taxonomy import default_taxonomy

_SCRAPER_SOURCE_DOMAINS: dict[str, str] = {
    "mako": "www.mako.co.il",
    "maariv": "www.maariv.co.il",
    "walla": "news.walla.co.il",
    "haaretz": "www.haaretz.co.il",
    "ice": "www.ice.co.il",
}
SOCIAL_DISCOVERY_DOMAINS: tuple[str, ...] = ("www.facebook.com",)


def _normalize_keywords(keywords: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for keyword in keywords:
        value = keyword.strip()
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _source_domain(source: SourceConfig) -> str | None:
    if source.url:
        return urlparse(str(source.url)).netloc or None
    if source.type == SourceType.SCRAPER:
        return _SCRAPER_SOURCE_DOMAINS.get(source.name)
    return None


def enabled_source_domains(config: Config) -> list[tuple[str, str]]:
    source_domains: list[tuple[str, str]] = []
    for source in config.sources:
        if not source.enabled:
            continue
        domain = _source_domain(source)
        if domain is None:
            continue
        source_domains.append((source.name, domain))
    return source_domains


def _taxonomy_query_specs() -> list[tuple[str, list[str]]]:
    specs_by_term: dict[str, set[str]] = {}
    for category_id, subcategory_id, term in default_taxonomy().discovery_terms():
        tags = specs_by_term.setdefault(term, set())
        tags.update({"taxonomy", f"category:{category_id}", f"subcategory:{subcategory_id}"})
    return [
        (term, sorted(tags))
        for term, tags in sorted(specs_by_term.items(), key=lambda item: item[0])
    ]


def build_discovery_queries(
    config: Config,
    *,
    days: int,
    now: datetime | None = None,
) -> list[DiscoveryQuery]:
    """Build normalized discovery queries for enabled discovery engines."""
    keywords = _normalize_keywords(config.keywords)
    taxonomy_enabled = DiscoveryQueryKind.TAXONOMY_TARGETED in config.discovery.default_query_kinds
    if not keywords and not taxonomy_enabled:
        return []

    current_time = now or datetime.now(UTC)
    date_from = current_time - timedelta(days=days)
    date_to = current_time
    queries: list[DiscoveryQuery] = []
    seen_keys: set[tuple[object, ...]] = set()
    source_domains = enabled_source_domains(config)

    for keyword in keywords:
        if DiscoveryQueryKind.BROAD in config.discovery.default_query_kinds:
            broad_key = (DiscoveryQueryKind.BROAD, keyword)
            if broad_key not in seen_keys:
                queries.append(
                    DiscoveryQuery(
                        query_text=keyword,
                        language="he",
                        date_from=date_from,
                        date_to=date_to,
                        query_kind=DiscoveryQueryKind.BROAD,
                    )
                )
                seen_keys.add(broad_key)

        if DiscoveryQueryKind.SOURCE_TARGETED in config.discovery.default_query_kinds:
            for source_name, domain in source_domains:
                source_key = (DiscoveryQueryKind.SOURCE_TARGETED, keyword, source_name, domain)
                if source_key in seen_keys:
                    continue
                queries.append(
                    DiscoveryQuery(
                        query_text=keyword,
                        language="he",
                        date_from=date_from,
                        date_to=date_to,
                        preferred_domains=[domain],
                        source_hint=source_name,
                        query_kind=DiscoveryQueryKind.SOURCE_TARGETED,
                        tags=[source_name],
                    )
                )
                seen_keys.add(source_key)

        if DiscoveryQueryKind.SOCIAL_TARGETED in config.discovery.default_query_kinds:
            for domain in SOCIAL_DISCOVERY_DOMAINS:
                queries.append(
                    DiscoveryQuery(
                        query_text=keyword,
                        language="he",
                        date_from=date_from,
                        date_to=date_to,
                        preferred_domains=[domain],
                        source_hint=domain,
                        query_kind=DiscoveryQueryKind.SOCIAL_TARGETED,
                        tags=["social", domain],
                    )
                )

    if taxonomy_enabled:
        taxonomy_specs = _taxonomy_query_specs()
        for term, tags in taxonomy_specs:
            taxonomy_key = (DiscoveryQueryKind.TAXONOMY_TARGETED, term)
            if taxonomy_key in seen_keys:
                continue
            queries.append(
                DiscoveryQuery(
                    query_text=term,
                    language="he",
                    date_from=date_from,
                    date_to=date_to,
                    query_kind=DiscoveryQueryKind.TAXONOMY_TARGETED,
                    tags=tags,
                )
            )
            seen_keys.add(taxonomy_key)

    return queries
