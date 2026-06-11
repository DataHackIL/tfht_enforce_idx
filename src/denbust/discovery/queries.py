"""Query builders for multi-engine discovery."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

from denbust.config import Config, SourceConfig, SourceType
from denbust.discovery.candidate_filters import (
    globally_excluded_search_domains,
    match_domain,
    normalize_domain,
)
from denbust.discovery.models import DiscoveryQuery, DiscoveryQueryKind
from denbust.discovery.source_families import generic_fetch_source_domains
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


def enabled_discovery_domains(config: Config) -> list[tuple[str, str]]:
    """Return configured source domains plus generic-fetch source-family domains."""
    source_domains = enabled_source_domains(config)
    seen = {(source_name, domain) for source_name, domain in source_domains}
    for source_name, domain in generic_fetch_source_domains():
        key = (source_name, domain)
        if key in seen:
            continue
        source_domains.append(key)
        seen.add(key)
    return source_domains


def source_targeted_search_domains(config: Config) -> list[tuple[str, str]]:
    """Return the domains worth issuing source-targeted *search* queries for.

    Natively-crawled sources (ynet/mako/maariv/haaretz/walla/ice) are excluded
    by default — the source-native adapters already fetch them, so paying search
    budget to re-find their articles is redundant. Blocklisted domains are also
    excluded (search would only surface candidates we immediately suppress).
    Set ``discovery.search_native_source_domains`` to re-include native sources.
    """
    excluded = globally_excluded_search_domains()
    candidates: list[tuple[str, str]] = []
    if config.discovery.search_native_source_domains:
        candidates.extend(enabled_source_domains(config))
    candidates.extend(generic_fetch_source_domains())
    result: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for source_name, domain in candidates:
        key = (source_name, domain)
        if key in seen:
            continue
        seen.add(key)
        if match_domain(normalize_domain(domain), excluded) is not None:
            continue
        result.append(key)
    return result


# Query kinds in descending budget priority: open-web broad/taxonomy first
# (they find off-list outlets that the source-native crawl cannot), then
# source-targeted, then social. Used when ``max_queries_per_run`` caps a run.
_QUERY_KIND_PRIORITY: dict[DiscoveryQueryKind, int] = {
    DiscoveryQueryKind.BROAD: 0,
    DiscoveryQueryKind.TAXONOMY_TARGETED: 1,
    DiscoveryQueryKind.SOURCE_TARGETED: 2,
    DiscoveryQueryKind.SOCIAL_TARGETED: 3,
}


def select_run_queries(
    queries: list[DiscoveryQuery],
    max_queries: int | None,
    *,
    last_run_at: Callable[[DiscoveryQuery], datetime | None] | None = None,
    yield_of: Callable[[DiscoveryQuery], int] | None = None,
) -> list[DiscoveryQuery]:
    """Cap *queries* to *max_queries*, best queries first.

    Ordering, in priority: highest historical **yield** first (when *yield_of*
    is supplied — query texts that have produced index-relevant records), then
    highest-priority query **kind** (open-web broad/taxonomy), then
    **least-recently-run** (when *last_run_at* is supplied — cross-run rotation),
    then original order.
    """
    if max_queries is None or len(queries) <= max_queries:
        return queries

    def sort_key(pair: tuple[int, DiscoveryQuery]) -> tuple[object, ...]:
        index, query = pair
        yield_rank = -(yield_of(query) if yield_of is not None else 0)
        priority = _QUERY_KIND_PRIORITY.get(query.query_kind, 99)
        if last_run_at is None:
            return (yield_rank, priority, index)
        ran_at = last_run_at(query)
        recency = (0, 0.0) if ran_at is None else (1, ran_at.timestamp())
        return (yield_rank, priority, recency, index)

    ordered = sorted(enumerate(queries), key=sort_key)
    return [query for _, query in ordered[:max_queries]]


def apply_query_budget(
    queries: list[DiscoveryQuery], max_queries: int | None
) -> list[DiscoveryQuery]:
    """Priority-only cap (no rotation). Thin wrapper over ``select_run_queries``."""
    return select_run_queries(queries, max_queries)


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
    max_queries: int | None = None,
    last_run_at: Callable[[DiscoveryQuery], datetime | None] | None = None,
    yield_of: Callable[[DiscoveryQuery], int] | None = None,
) -> list[DiscoveryQuery]:
    """Build normalized discovery queries for enabled discovery engines.

    Source-targeted queries cover only ``source_targeted_search_domains`` —
    natively-crawled and blocklisted domains are dropped to save search budget.
    When *max_queries* (or ``config.discovery.max_queries_per_run``) is set, the
    result is capped — highest historical *yield_of* first, then kind priority,
    then *last_run_at* rotation.
    """
    keywords = _normalize_keywords(config.keywords)
    taxonomy_enabled = DiscoveryQueryKind.TAXONOMY_TARGETED in config.discovery.default_query_kinds
    if not keywords and not taxonomy_enabled:
        return []

    current_time = now or datetime.now(UTC)
    date_from = current_time - timedelta(days=days)
    date_to = current_time
    queries: list[DiscoveryQuery] = []
    seen_keys: set[tuple[object, ...]] = set()
    source_domains = source_targeted_search_domains(config)
    # Domains that are structurally off-topic — excluded from every broad and
    # taxonomy query to avoid wasting search-engine quota and classifier credits.
    # Source-targeted queries are already scoped to a single preferred domain, so
    # exclusions are not needed there.
    excluded_domains = sorted(globally_excluded_search_domains())

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
                        excluded_domains=excluded_domains,
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
                    excluded_domains=excluded_domains,
                )
            )
            seen_keys.add(taxonomy_key)
            if DiscoveryQueryKind.SOURCE_TARGETED in config.discovery.default_query_kinds:
                for source_name, domain in source_domains:
                    taxonomy_source_key = (
                        DiscoveryQueryKind.SOURCE_TARGETED,
                        "taxonomy",
                        term,
                        source_name,
                        domain,
                    )
                    if taxonomy_source_key in seen_keys:
                        continue
                    queries.append(
                        DiscoveryQuery(
                            query_text=term,
                            language="he",
                            date_from=date_from,
                            date_to=date_to,
                            preferred_domains=[domain],
                            source_hint=source_name,
                            query_kind=DiscoveryQueryKind.SOURCE_TARGETED,
                            tags=[source_name, *tags],
                        )
                    )
                    seen_keys.add(taxonomy_source_key)

    budget = max_queries if max_queries is not None else config.discovery.max_queries_per_run
    return select_run_queries(queries, budget, last_run_at=last_run_at, yield_of=yield_of)
