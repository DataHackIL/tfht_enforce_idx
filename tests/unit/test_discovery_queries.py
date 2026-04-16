"""Unit tests for discovery query builders."""

from __future__ import annotations

from datetime import UTC, datetime

from denbust.config import Config, SourceConfig, SourceType
from denbust.discovery.models import DiscoveryQueryKind
from denbust.discovery.queries import build_discovery_queries


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
