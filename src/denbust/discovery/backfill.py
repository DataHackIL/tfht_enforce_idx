"""Historical backfill planning and window-aware discovery helpers."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel

from denbust.config import Config
from denbust.discovery.models import DiscoveryQuery, DiscoveryQueryKind
from denbust.discovery.queries import SOCIAL_DISCOVERY_DOMAINS
from denbust.taxonomy import default_taxonomy

BACKFILL_BATCH_ID_ENV = "DENBUST_BACKFILL_BATCH_ID"
BACKFILL_DATE_FROM_ENV = "DENBUST_BACKFILL_DATE_FROM"
BACKFILL_DATE_TO_ENV = "DENBUST_BACKFILL_DATE_TO"


class BackfillWindow(BaseModel):
    """One contiguous historical slice inside a backfill batch."""

    index: int
    date_from: datetime
    date_to: datetime


def parse_backfill_datetime(value: str, *, env_name: str) -> datetime:
    """Parse a required backfill timestamp from an environment variable."""
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{env_name} must not be empty")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def resolve_backfill_request_window() -> tuple[datetime, datetime]:
    """Resolve the requested historical window for a backfill job."""
    date_from = os.getenv(BACKFILL_DATE_FROM_ENV)
    date_to = os.getenv(BACKFILL_DATE_TO_ENV)
    if not date_from or not date_to:
        missing = [
            env_name
            for env_name, value in [
                (BACKFILL_DATE_FROM_ENV, date_from),
                (BACKFILL_DATE_TO_ENV, date_to),
            ]
            if not value
        ]
        raise ValueError(
            "Missing required backfill window environment variable(s): " + ", ".join(missing)
        )
    resolved_from = parse_backfill_datetime(date_from, env_name=BACKFILL_DATE_FROM_ENV)
    resolved_to = parse_backfill_datetime(date_to, env_name=BACKFILL_DATE_TO_ENV)
    if resolved_from > resolved_to:
        raise ValueError(
            f"{BACKFILL_DATE_FROM_ENV} must be earlier than or equal to {BACKFILL_DATE_TO_ENV}"
        )
    return resolved_from, resolved_to


def plan_backfill_windows(
    *,
    date_from: datetime,
    date_to: datetime,
    batch_window_days: int,
) -> list[BackfillWindow]:
    """Split a requested historical window into contiguous slices."""
    windows: list[BackfillWindow] = []
    current_start = date_from
    index = 0
    while current_start <= date_to:
        current_end = min(
            date_to,
            current_start + timedelta(days=batch_window_days) - timedelta(microseconds=1),
        )
        windows.append(BackfillWindow(index=index, date_from=current_start, date_to=current_end))
        index += 1
        current_start = current_end + timedelta(microseconds=1)
    return windows


def _normalize_keywords(keywords: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for keyword in keywords:
        value = keyword.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _taxonomy_query_specs() -> list[tuple[str, list[str]]]:
    specs_by_term: dict[str, set[str]] = {}
    for category_id, subcategory_id, term in default_taxonomy().discovery_terms():
        tags = specs_by_term.setdefault(term, set())
        tags.update({"taxonomy", f"category:{category_id}", f"subcategory:{subcategory_id}"})
    return [
        (term, sorted(tags))
        for term, tags in sorted(specs_by_term.items(), key=lambda item: item[0])
    ]


def build_backfill_queries(
    config: Config,
    *,
    window: BackfillWindow,
) -> list[DiscoveryQuery]:
    """Build normalized historical discovery queries for one backfill window."""
    from denbust.discovery.queries import enabled_source_domains

    keywords = _normalize_keywords(config.keywords)
    if not keywords:
        return []

    queries: list[DiscoveryQuery] = []
    seen_keys: set[tuple[object, ...]] = set()
    source_domains = enabled_source_domains(config)
    for keyword in keywords:
        if DiscoveryQueryKind.BROAD in config.discovery.default_query_kinds:
            broad_key = (DiscoveryQueryKind.BROAD, keyword, window.index)
            if broad_key not in seen_keys:
                queries.append(
                    DiscoveryQuery(
                        query_text=keyword,
                        language="he",
                        date_from=window.date_from,
                        date_to=window.date_to,
                        query_kind=DiscoveryQueryKind.BROAD,
                        tags=["backfill", f"window:{window.index}"],
                    )
                )
                seen_keys.add(broad_key)

        if DiscoveryQueryKind.SOURCE_TARGETED in config.discovery.default_query_kinds:
            for source_name, domain in source_domains:
                source_key = (
                    DiscoveryQueryKind.SOURCE_TARGETED,
                    keyword,
                    source_name,
                    domain,
                    window.index,
                )
                if source_key in seen_keys:
                    continue
                queries.append(
                    DiscoveryQuery(
                        query_text=keyword,
                        language="he",
                        date_from=window.date_from,
                        date_to=window.date_to,
                        preferred_domains=[domain],
                        source_hint=source_name,
                        query_kind=DiscoveryQueryKind.SOURCE_TARGETED,
                        tags=["backfill", source_name, f"window:{window.index}"],
                    )
                )
                seen_keys.add(source_key)
        if DiscoveryQueryKind.TAXONOMY_TARGETED in config.discovery.default_query_kinds:
            for term, tags in _taxonomy_query_specs():
                taxonomy_key = (DiscoveryQueryKind.TAXONOMY_TARGETED, term, window.index)
                if taxonomy_key in seen_keys:
                    continue
                queries.append(
                    DiscoveryQuery(
                        query_text=term,
                        language="he",
                        date_from=window.date_from,
                        date_to=window.date_to,
                        query_kind=DiscoveryQueryKind.TAXONOMY_TARGETED,
                        tags=["backfill", *tags, f"window:{window.index}"],
                    )
                )
                seen_keys.add(taxonomy_key)
        if DiscoveryQueryKind.SOCIAL_TARGETED in config.discovery.default_query_kinds:
            for domain in SOCIAL_DISCOVERY_DOMAINS:
                queries.append(
                    DiscoveryQuery(
                        query_text=keyword,
                        language="he",
                        date_from=window.date_from,
                        date_to=window.date_to,
                        preferred_domains=[domain],
                        source_hint=domain,
                        query_kind=DiscoveryQueryKind.SOCIAL_TARGETED,
                        tags=["backfill", "social", domain, f"window:{window.index}"],
                    )
                )
    return queries


def backfill_metadata(
    *,
    batch_id: str,
    window: BackfillWindow,
) -> dict[str, Any]:
    """Build stable metadata tags for candidates discovered during backfill."""
    return {
        "backfill_batch_id": batch_id,
        "backfill_window_index": window.index,
        "backfill_window_start": window.date_from.isoformat(),
        "backfill_window_end": window.date_to.isoformat(),
    }
