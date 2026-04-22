"""Unit tests for discovery query builders."""

from __future__ import annotations

from datetime import UTC, datetime

from denbust.config import Config, SourceConfig, SourceType
from denbust.discovery.models import DiscoveryQueryKind
from denbust.discovery.queries import build_discovery_queries, enabled_source_domains
from denbust.taxonomy import default_taxonomy


def test_build_discovery_queries_creates_all_default_query_types() -> None:
    """Query construction should include broad, source-targeted, taxonomy-targeted, and social-targeted variants."""
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
    taxonomy_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.TAXONOMY_TARGETED
    ]
    social_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOCIAL_TARGETED
    ]

    assert len(broad_queries) == 2
    assert len(targeted_queries) == 4
    assert len(taxonomy_queries) == len(
        {term for _, _, term in default_taxonomy().discovery_terms()}
    )
    assert len(social_queries) == 2
    assert {query.query_text for query in broad_queries} == {"בית בושת", "זנות"}
    assert {(query.source_hint, tuple(query.preferred_domains)) for query in targeted_queries} == {
        ("ynet", ("www.ynet.co.il",)),
        ("mako", ("www.mako.co.il",)),
    }
    assert all(not query.preferred_domains for query in taxonomy_queries)
    assert any(
        query.query_text == "נישואין בכפייה"
        and {
            "taxonomy",
            "category:human_trafficking",
            "subcategory:trafficking_forced_marriage",
        }.issubset(set(query.tags))
        for query in taxonomy_queries
    )
    assert any(
        query.query_text == "המודל הנורדי" and "subcategory:nordic_model_law" in query.tags
        for query in taxonomy_queries
    )
    assert {(query.source_hint, tuple(query.preferred_domains)) for query in social_queries} == {
        ("www.facebook.com", ("www.facebook.com",)),
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
    """If all keywords collapse away, only taxonomy-targeted queries should remain."""
    config = Config(
        keywords=["", "   "],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"enabled": True},
    )

    queries = build_discovery_queries(config, days=3)

    assert queries
    assert all(query.query_kind is DiscoveryQueryKind.TAXONOMY_TARGETED for query in queries)


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


def test_build_discovery_queries_can_disable_social_targeted_generation() -> None:
    """Explicit query-kind configuration should still allow social discovery to be disabled."""
    config = Config(
        keywords=["זנות"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["broad", "source_targeted"]},
    )

    queries = build_discovery_queries(config, days=3)

    assert all(query.query_kind is not DiscoveryQueryKind.SOCIAL_TARGETED for query in queries)


def test_build_discovery_queries_can_disable_taxonomy_targeted_generation() -> None:
    """Explicit query-kind configuration should still allow taxonomy discovery to be disabled."""
    config = Config(
        keywords=["זנות"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["broad", "source_targeted", "social_targeted"]},
    )

    queries = build_discovery_queries(config, days=3)

    assert all(query.query_kind is not DiscoveryQueryKind.TAXONOMY_TARGETED for query in queries)


def test_build_discovery_queries_deduplicates_social_targeted_entries() -> None:
    """Duplicate normalized keywords should not emit duplicate social-targeted queries."""
    config = Config(
        keywords=["זנות", "  זנות  "],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["social_targeted"]},
    )

    queries = build_discovery_queries(config, days=3)

    social_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOCIAL_TARGETED
    ]
    assert len(social_queries) == 1
    assert social_queries[0].preferred_domains == ["www.facebook.com"]


def test_build_discovery_queries_deduplicates_taxonomy_terms(monkeypatch) -> None:
    """Duplicate taxonomy terms should collapse into one query with merged taxonomy tags."""

    class _FakeTaxonomy:
        def discovery_terms(self) -> list[tuple[str, str, str]]:
            return [
                ("cat_a", "leaf_a", "מונח משותף"),
                ("cat_b", "leaf_b", "מונח משותף"),
                ("cat_b", "leaf_b", "מונח ייחודי"),
            ]

    monkeypatch.setattr("denbust.discovery.queries.default_taxonomy", lambda: _FakeTaxonomy())

    config = Config(
        keywords=["זנות"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["taxonomy_targeted"]},
    )

    queries = build_discovery_queries(config, days=3)

    assert [query.query_text for query in queries] == ["מונח ייחודי", "מונח משותף"]
    shared_query = queries[1]
    assert "category:cat_a" in shared_query.tags
    assert "category:cat_b" in shared_query.tags
    assert "subcategory:leaf_a" in shared_query.tags
    assert "subcategory:leaf_b" in shared_query.tags


def test_build_discovery_queries_skips_duplicate_taxonomy_specs(monkeypatch) -> None:
    """Repeated taxonomy specs should hit the seen-key guard only once."""
    monkeypatch.setattr(
        "denbust.discovery.queries._taxonomy_query_specs",
        lambda: [
            ("מונח משותף", ["taxonomy", "category:cat_a"]),
            ("מונח משותף", ["taxonomy", "category:cat_b"]),
        ],
    )

    config = Config(
        keywords=[],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["taxonomy_targeted"]},
    )

    queries = build_discovery_queries(config, days=3)

    assert len(queries) == 1
    assert queries[0].query_text == "מונח משותף"


def test_build_discovery_queries_returns_empty_when_only_keyword_driven_kinds_are_enabled() -> None:
    """Empty keyword sets should short-circuit when taxonomy-targeted discovery is disabled."""
    config = Config(
        keywords=["", "   "],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["broad", "source_targeted", "social_targeted"]},
    )

    assert build_discovery_queries(config, days=3) == []


def test_build_discovery_queries_does_not_load_taxonomy_when_disabled(monkeypatch) -> None:
    """Taxonomy helpers should not run when taxonomy-targeted discovery is disabled."""
    monkeypatch.setattr(
        "denbust.discovery.queries._taxonomy_query_specs",
        lambda: (_ for _ in ()).throw(AssertionError("should not load taxonomy specs")),
    )

    config = Config(
        keywords=["זנות"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["broad", "source_targeted", "social_targeted"]},
    )

    queries = build_discovery_queries(config, days=3)

    assert queries
    assert all(query.query_kind is not DiscoveryQueryKind.TAXONOMY_TARGETED for query in queries)


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
