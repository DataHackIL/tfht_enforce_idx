"""Unit tests for discovery query builders."""

from __future__ import annotations

from datetime import UTC, datetime

from denbust.config import Config, SourceConfig, SourceType
from denbust.discovery.models import DiscoveryQueryKind
from denbust.discovery.queries import (
    build_discovery_queries,
    enabled_discovery_domains,
    enabled_source_domains,
)
from denbust.taxonomy import default_taxonomy


def test_build_discovery_queries_creates_all_default_query_types() -> None:
    """Query construction should include broad, source-targeted, taxonomy-targeted, and social-targeted variants."""
    # search_native_source_domains=True restores native sources in source-targeted
    # (default drops them as redundant with the source-native crawl). globes/themarker
    # stay dropped because they are blocklisted.
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
        discovery={"enabled": True, "search_native_source_domains": True},
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
    keyword_targeted_queries = [query for query in targeted_queries if "taxonomy" not in query.tags]
    taxonomy_targeted_source_queries = [
        query for query in targeted_queries if "taxonomy" in query.tags
    ]
    # 2 keywords x 2 native sources (globes/themarker dropped as blocklisted).
    assert len(keyword_targeted_queries) == 4
    assert len(taxonomy_targeted_source_queries) == len(taxonomy_queries) * 2
    assert len(taxonomy_queries) == len(
        {term for _, _, term in default_taxonomy().discovery_terms()}
    )
    assert len(social_queries) == 2
    assert {query.query_text for query in broad_queries} == {"בית בושת", "זנות"}
    assert {
        (query.source_hint, tuple(query.preferred_domains)) for query in keyword_targeted_queries
    } == {
        ("ynet", ("www.ynet.co.il",)),
        ("mako", ("www.mako.co.il",)),
    }
    assert all(not query.preferred_domains for query in taxonomy_queries)
    assert {
        (query.source_hint, tuple(query.preferred_domains))
        for query in taxonomy_targeted_source_queries
    } == {
        ("ynet", ("www.ynet.co.il",)),
        ("mako", ("www.mako.co.il",)),
    }
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
            "search_native_source_domains": True,
            "default_query_kinds": [DiscoveryQueryKind.SOURCE_TARGETED],
        },
    )

    queries = build_discovery_queries(config, days=3)

    # Native mako kept (flag on); globes/themarker dropped as blocklisted.
    assert [query.source_hint for query in queries] == ["mako"]
    assert all(query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED for query in queries)


def test_build_discovery_queries_excludes_news1_source_targeted_fanout() -> None:
    """Candidate-only News1 evidence should not add recurring source-targeted fanout."""
    config = Config(
        keywords=["בית בושת"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["source_targeted", "taxonomy_targeted"]},
    )

    queries = build_discovery_queries(config, days=3)

    assert all(query.source_hint != "news1" for query in queries)
    assert all(tuple(query.preferred_domains) != ("www.news1.co.il",) for query in queries)


def test_build_discovery_queries_emits_source_targeted_taxonomy_terms(
    monkeypatch,
) -> None:
    """Taxonomy recall terms should also be constrained to configured source domains."""
    monkeypatch.setattr(
        "denbust.discovery.queries._taxonomy_query_specs",
        lambda: [("דירה דיסקרטית", ["taxonomy", "category:brothels"])],
    )
    config = Config(
        keywords=["זנות"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={
            "search_native_source_domains": True,
            "default_query_kinds": ["source_targeted", "taxonomy_targeted"],
        },
    )

    queries = build_discovery_queries(
        config,
        days=5,
        now=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
    )

    taxonomy_source_queries = [
        query
        for query in queries
        if query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED
        and query.query_text == "דירה דיסקרטית"
    ]
    assert {
        (query.source_hint, tuple(query.preferred_domains)) for query in taxonomy_source_queries
    } == {
        ("mako", ("www.mako.co.il",)),
    }
    assert all(
        query.date_from == datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
        and query.date_to == datetime(2026, 4, 16, 12, 0, tzinfo=UTC)
        for query in taxonomy_source_queries
    )
    assert all(
        {"taxonomy", "category:brothels"}.issubset(set(query.tags))
        for query in taxonomy_source_queries
    )


def test_build_discovery_queries_keeps_taxonomy_source_provenance_for_keyword_overlap(
    monkeypatch,
) -> None:
    """Keyword and taxonomy source queries with the same term should both be retained."""
    monkeypatch.setattr(
        "denbust.discovery.queries._taxonomy_query_specs",
        lambda: [("זנות", ["taxonomy", "category:brothels"])],
    )
    config = Config(
        keywords=["זנות"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={
            "search_native_source_domains": True,
            "default_query_kinds": ["source_targeted", "taxonomy_targeted"],
        },
    )

    queries = build_discovery_queries(config, days=3)

    source_queries = [
        query
        for query in queries
        if query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED and query.query_text == "זנות"
    ]
    # Only native mako (globes/themarker blocklisted): one keyword + one taxonomy query.
    assert len(source_queries) == 2
    assert sorted("taxonomy" in query.tags for query in source_queries) == [False, True]


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
        discovery={"enabled": True, "search_native_source_domains": True},
    )

    queries = build_discovery_queries(config, days=3)

    broad_queries = [query for query in queries if query.query_kind is DiscoveryQueryKind.BROAD]
    targeted_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED
    ]
    keyword_targeted_queries = [query for query in targeted_queries if "taxonomy" not in query.tags]

    assert len(broad_queries) == 1
    # Only native mako survives (unknown source has no domain; globes/themarker blocklisted).
    assert len(keyword_targeted_queries) == 1
    assert broad_queries[0].query_text == "זנות"
    assert {query.source_hint for query in keyword_targeted_queries} == {"mako"}


def test_build_discovery_queries_returns_taxonomy_queries_for_empty_keyword_set() -> None:
    """If keywords collapse away, taxonomy terms should still drive broad and source queries."""
    config = Config(
        keywords=["", "   "],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"enabled": True, "search_native_source_domains": True},
    )

    queries = build_discovery_queries(config, days=3)

    assert queries
    assert {query.query_kind for query in queries} == {
        DiscoveryQueryKind.TAXONOMY_TARGETED,
        DiscoveryQueryKind.SOURCE_TARGETED,
    }
    assert all("taxonomy" in query.tags for query in queries)


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
        discovery={"enabled": True, "search_native_source_domains": True},
    )

    queries = build_discovery_queries(config, days=3)
    targeted_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED
    ]
    keyword_targeted_queries = [query for query in targeted_queries if "taxonomy" not in query.tags]

    assert {
        (query.source_hint, tuple(query.preferred_domains)) for query in keyword_targeted_queries
    } == {
        ("ynet", ("www.ynet.co.il",)),
    }


def test_build_discovery_queries_broad_queries_carry_excluded_domains() -> None:
    """Broad queries should include the globally-excluded domain list."""
    config = Config(
        keywords=["בית בושת"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["broad"]},
    )

    queries = build_discovery_queries(config, days=3)

    broad_queries = [query for query in queries if query.query_kind is DiscoveryQueryKind.BROAD]
    assert broad_queries
    for query in broad_queries:
        assert "sport1.maariv.co.il" in query.excluded_domains


def test_build_discovery_queries_taxonomy_queries_carry_excluded_domains() -> None:
    """Taxonomy-targeted queries should include the globally-excluded domain list."""
    config = Config(
        keywords=[],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["taxonomy_targeted"]},
    )

    queries = build_discovery_queries(config, days=3)

    taxonomy_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.TAXONOMY_TARGETED
    ]
    assert taxonomy_queries
    for query in taxonomy_queries:
        assert "sport1.maariv.co.il" in query.excluded_domains


def test_build_discovery_queries_source_targeted_queries_have_no_excluded_domains() -> None:
    """Source-targeted queries are already scoped; they must not carry excluded_domains."""
    config = Config(
        keywords=["בית בושת"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={
            "search_native_source_domains": True,
            "default_query_kinds": ["source_targeted"],
        },
    )

    queries = build_discovery_queries(config, days=3)

    source_queries = [
        query for query in queries if query.query_kind is DiscoveryQueryKind.SOURCE_TARGETED
    ]
    assert source_queries
    for query in source_queries:
        assert not query.excluded_domains


def test_build_discovery_queries_drops_native_source_targeted_by_default() -> None:
    """By default, natively-crawled sources get no source-targeted search queries."""
    config = Config(
        keywords=["זנות"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["broad", "source_targeted"]},
    )

    queries = build_discovery_queries(config, days=3)

    # mako is native (dropped); globes/themarker are blocklisted (dropped) → none left.
    assert [q for q in queries if q.query_kind is DiscoveryQueryKind.SOURCE_TARGETED] == []
    assert [q.query_kind for q in queries] == [DiscoveryQueryKind.BROAD]


def test_build_discovery_queries_query_budget_keeps_highest_priority_kinds() -> None:
    """A query budget keeps open-web (broad/taxonomy) kinds first, dropping the rest."""
    config = Config(
        keywords=["זנות", "בית בושת", "סרסור"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={
            "search_native_source_domains": True,
            "default_query_kinds": ["broad", "source_targeted", "social_targeted"],
        },
    )

    full = build_discovery_queries(config, days=3)
    assert len(full) > 3  # broad + source-targeted + social present

    capped = build_discovery_queries(config, days=3, max_queries=3)
    assert len(capped) == 3
    # The 3 broad (open-web) queries outrank source-targeted and social.
    assert all(q.query_kind is DiscoveryQueryKind.BROAD for q in capped)


def test_build_discovery_queries_query_budget_from_config() -> None:
    """The cap can come from config.discovery.max_queries_per_run."""
    config = Config(
        keywords=["זנות", "בית בושת"],
        sources=[SourceConfig(name="mako", type=SourceType.SCRAPER)],
        discovery={"default_query_kinds": ["broad"], "max_queries_per_run": 1},
    )
    assert len(build_discovery_queries(config, days=3)) == 1


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
    """Configured source-domain resolution should not include generic source families."""
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


def test_enabled_discovery_domains_adds_generic_fetch_source_families() -> None:
    """Discovery-domain resolution should include bounded generic source families."""
    config = Config(
        sources=[
            SourceConfig(name="ynet", type=SourceType.RSS, url="https://www.ynet.co.il/feed.xml"),
        ]
    )

    assert enabled_discovery_domains(config) == [
        ("ynet", "www.ynet.co.il"),
        ("globes", "www.globes.co.il"),
        ("themarker", "www.themarker.com"),
    ]
