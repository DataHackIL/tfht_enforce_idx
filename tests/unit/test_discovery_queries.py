"""Unit tests for discovery query builders."""

from __future__ import annotations

from datetime import UTC, datetime

from denbust.config import Config, SourceConfig, SourceType
from denbust.discovery.models import DiscoveryQueryKind
from denbust.discovery.queries import build_discovery_queries, enabled_source_domains


def test_build_discovery_queries_creates_broad_and_source_targeted_queries() -> None:
    """Query construction should include both broad and source-targeted variants by default."""
    config = Config(
        keywords=["בית בושת", "זנות"],
        sources=[
            SourceConfig(
                name="ynet",
                type=SourceType.RSS,
                url="https://www.ynet.co.il/Integration/StoryRss2.xml",
            ),
            SourceConfig(name="mako", type=SourceType.SCRAPER),
        ],
        discovery={"enabled": True},
    )

    queries = build_discovery_queries(
        config,
        days=5,
        now=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
    )

    broad_queries = [query for query in queries if query.query_kind is DiscoveryQueryKind.BROAD]
    targeted_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED
    ]

    assert len(broad_queries) == 2
    assert len(targeted_queries) == 4
    assert {query.query_text for query in broad_queries} == {"בית בושת", "זנות"}
    assert {(query.source_hint, tuple(query.preferred_domains)) for query in targeted_queries} == {
        ("ynet", ("www.ynet.co.il",)),
        ("mako", ("www.mako.co.il",)),
    }
    assert all(query.language == "he" for query in queries)
    assert all(query.date_from is not None and query.date_to is not None for query in queries)


def test_build_discovery_queries_respects_enabled_query_kinds() -> None:
    """Only configured query kinds should be generated."""
    config = Config(
        keywords=["בית בושת"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={
            "enabled": True,
            "default_query_kinds": [DiscoveryQueryKind.SOURCE_TARGETED],
        },
    )

    queries = build_discovery_queries(config, days=3)

    assert len(queries) == 1
    assert queries[0].query_kind is DiscoveryQueryKind.SOURCE_TARGETED
    assert queries[0].source_hint == "mako"


def test_build_discovery_queries_filters_blank_duplicate_and_unusable_sources() -> None:
    """Blank/duplicate keywords and unusable sources should be skipped cleanly."""
    config = Config(
        keywords=["", "  ", "זנות", "זנות"],
        sources=[
            SourceConfig(name="disabled", type=SourceType.SCRAPER, enabled=False),
            SourceConfig(name="unknown", type=SourceType.SCRAPER),
            SourceConfig(name="rss-no-url", type=SourceType.RSS),
            SourceConfig(name="mako", type=SourceType.SCRAPER),
        ],
        discovery={"enabled": True},
    )

    queries = build_discovery_queries(config, days=3)

    broad_queries = [query for query in queries if query.query_kind is DiscoveryQueryKind.BROAD]
    targeted_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED
    ]

    assert len(broad_queries) == 1
    assert len(targeted_queries) == 1
    assert broad_queries[0].query_text == "זנות"
    assert targeted_queries[0].source_hint == "mako"


def test_build_discovery_queries_returns_empty_for_empty_keyword_set() -> None:
    """If all keywords collapse away, no discovery queries should be built."""
    config = Config(
        keywords=["", "   "],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"enabled": True},
    )

    assert build_discovery_queries(config, days=3) == []


def test_build_discovery_queries_avoids_duplicate_source_targeted_entries() -> None:
    """Duplicate source domains should not emit duplicate source-targeted queries."""
    config = Config(
        keywords=["זנות", "זנות"],
        sources=[
            SourceConfig(name="ynet", type=SourceType.RSS, url="https://www.ynet.co.il/feed.xml"),
            SourceConfig(
                name="ynet",
                type=SourceType.RSS,
                url="https://www.ynet.co.il/another-feed.xml",
            ),
        ],
        discovery={"enabled": True},
    )

    queries = build_discovery_queries(config, days=3)
    targeted_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED
    ]

    assert len(targeted_queries) == 1
    assert targeted_queries[0].preferred_domains == ["www.ynet.co.il"]


def test_enabled_source_domains_returns_only_enabled_resolved_domains() -> None:
    """Public source-domain resolution should expose reusable enabled discovery domains."""
    config = Config(
        sources=[
            SourceConfig(name="disabled", type=SourceType.SCRAPER, enabled=False),
            SourceConfig(name="rss-no-url", type=SourceType.RSS),
            SourceConfig(name="ynet", type=SourceType.RSS, url="https://www.ynet.co.il/feed.xml"),
            SourceConfig(name="mako", type=SourceType.SCRAPER),
        ]
    )

    assert enabled_source_domains(config) == [
        ("ynet", "www.ynet.co.il"),
        ("mako", "www.mako.co.il"),
    ]
