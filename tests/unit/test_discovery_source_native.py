"""Unit tests for source-native discovery candidacy persistence."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import HttpUrl

from denbust.data_models import RawArticle
from denbust.discovery.models import (
    CandidateStatus,
    ContentBasis,
    DiscoveredCandidate,
    DiscoveryRun,
    ProducerKind,
)
from denbust.discovery.source_native import (
    _candidate_domain,
    _normalize_domain,
    persist_discovered_candidates,
    raw_article_to_discovered_candidate,
)
from denbust.discovery.state_paths import resolve_discovery_state_paths
from denbust.discovery.storage import StateRepoDiscoveryPersistence
from denbust.models.common import DatasetName


def build_raw_article(
    url: str,
    *,
    source_name: str = "ynet",
    title: str = "פשיטה על בית בושת",
    discovered_at: datetime | None = None,
) -> RawArticle:
    """Build a raw article fixture for source-native discovery tests."""
    return RawArticle(
        url=HttpUrl(url),
        title=title,
        snippet="המשטרה ביצעה פשיטה.",
        date=discovered_at or datetime(2026, 4, 11, 8, 0, tzinfo=UTC),
        source_name=source_name,
    )


def test_raw_article_to_discovered_candidate_uses_source_native_defaults() -> None:
    """Raw source articles should normalize into source-native discovered candidates."""
    article = build_raw_article("https://www.ynet.co.il/news/article/abc?utm_source=test")

    candidate = raw_article_to_discovered_candidate(article)

    assert candidate.producer_kind.value == "source_native"
    assert str(candidate.canonical_url) == "https://ynet.co.il/news/article/abc"
    assert candidate.source_hint == "ynet"
    assert candidate.metadata["source_name"] == "ynet"


def test_persist_discovered_candidates_merges_repeat_discovery_by_canonical_url(
    tmp_path: Path,
) -> None:
    """Repeated discovery of the same article should upsert a single durable candidate."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    first_seen = datetime(2026, 4, 11, 8, 0, tzinfo=UTC)
    second_seen = first_seen + timedelta(hours=2)

    first_discovery = raw_article_to_discovered_candidate(
        build_raw_article(
            "https://www.ynet.co.il/news/article/abc?utm_source=test",
            title="פשיטה ראשונה",
            discovered_at=first_seen,
        ),
        discovered_at=first_seen,
    )
    second_discovery = raw_article_to_discovered_candidate(
        build_raw_article(
            "https://ynet.co.il/news/article/abc?Partner=searchResults",
            title="פשיטה ראשונה עודכן",
            discovered_at=second_seen,
        ),
        discovered_at=second_seen,
    )

    persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-1"),
        discovered_candidates=[first_discovery],
        persistence=persistence,
    )
    persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-2"),
        discovered_candidates=[second_discovery],
        persistence=persistence,
    )

    candidates = persistence.list_candidates()
    assert len(candidates) == 1
    candidate = candidates[0]
    assert str(candidate.canonical_url) == "https://ynet.co.il/news/article/abc"
    assert candidate.titles == ["פשיטה ראשונה", "פשיטה ראשונה עודכן"]
    assert candidate.source_discovery_only is True
    assert candidate.first_seen_at == first_seen
    assert candidate.last_seen_at == second_seen

    provenance = persistence.list_provenance(candidate.candidate_id)
    assert len(provenance) == 2
    run_snapshots = sorted(paths.runs_dir.glob("*.json"))
    assert len(run_snapshots) == 2
    latest_snapshot = json.loads(run_snapshots[-1].read_text(encoding="utf-8"))
    assert latest_snapshot["merged_candidate_count"] == 1
    assert latest_snapshot["finished_at"] is not None


def test_persist_discovered_candidates_marks_runs_finished_on_success(tmp_path: Path) -> None:
    """Successful persistence should stamp a terminal finished_at value on the run."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    discovery = raw_article_to_discovered_candidate(
        build_raw_article("https://www.ynet.co.il/news/article/xyz")
    )

    persisted = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-finished"),
        discovered_candidates=[discovery],
        persistence=persistence,
    )

    assert persisted.run.status.value == "succeeded"
    assert persisted.run.finished_at is not None


def test_persist_discovered_candidates_marks_social_search_candidates_unsupported(
    tmp_path: Path,
) -> None:
    """Social-search candidates should be retained durably but excluded from scrape eligibility."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    discovery = DiscoveredCandidate(
        producer_name="brave",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        query_text="בית בושת",
        candidate_url=HttpUrl("https://www.facebook.com/story.php?story_fbid=1&id=2"),
        canonical_url=HttpUrl("https://www.facebook.com/story.php?story_fbid=1&id=2"),
        title="פוסט פייסבוק",
        snippet="חשד לבית בושת",
        discovered_at=datetime(2026, 4, 11, 8, 0, tzinfo=UTC),
        source_hint="www.facebook.com",
        metadata={"query_kind": "social_targeted"},
    )

    persisted = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-social"),
        discovered_candidates=[discovery],
        persistence=persistence,
    )

    candidate = persisted.candidates[0]
    provenance = persisted.provenance[0]
    assert candidate.candidate_status.value == "unsupported_source"
    assert candidate.needs_review is True
    assert candidate.discovered_via == ["brave"]
    assert provenance.producer_kind is ProducerKind.SOCIAL_SEARCH


def test_persist_discovered_candidates_keeps_social_posts_scrapeable_without_social_query_kind(
    tmp_path: Path,
) -> None:
    """Broad social post-like URLs should not be suppressed as social profiles."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    discovery = DiscoveredCandidate(
        producer_name="brave",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        query_text="בית בושת",
        candidate_url=HttpUrl("https://facebook.com/story.php?story_fbid=5&id=6"),
        canonical_url=HttpUrl("https://facebook.com/story.php?story_fbid=5&id=6"),
        title="פוסט פייסבוק רחב",
        snippet="חשד לבית בושת",
        discovered_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
        source_hint="facebook.com",
        metadata={"query_kind": "broad"},
    )

    persisted = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-social-domain"),
        discovered_candidates=[discovery],
        persistence=persistence,
    )

    candidate = persisted.candidates[0]
    provenance = persisted.provenance[0]
    assert candidate.candidate_status.value == "new"
    assert candidate.needs_review is False
    assert provenance.producer_kind is ProducerKind.SEARCH_ENGINE


def test_persist_discovered_candidates_marks_social_profile_subdomains_unsupported(
    tmp_path: Path,
) -> None:
    """Social profile subdomains should stay out of scrape eligibility."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    discovery = DiscoveredCandidate(
        producer_name="brave",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        query_text="בית בושת",
        candidate_url=HttpUrl("https://m.facebook.com/profile.php?id=6"),
        canonical_url=HttpUrl("https://m.facebook.com/profile.php?id=6"),
        title="פוסט פייסבוק רחב",
        snippet="חשד לבית בושת",
        discovered_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
        source_hint="m.facebook.com",
        metadata={"query_kind": "broad"},
    )

    persisted = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-social-subdomain"),
        discovered_candidates=[discovery],
        persistence=persistence,
    )

    candidate = persisted.candidates[0]
    provenance = persisted.provenance[0]
    assert candidate.candidate_status.value == "unsupported_source"
    assert candidate.needs_review is False
    assert candidate.metadata["unsupported_source_filter"] == "search_noise"
    assert candidate.metadata["unsupported_source_reason"] == "social_profile"
    assert candidate.metadata["unsupported_source_domain"] == "facebook.com"
    assert provenance.producer_kind is ProducerKind.SEARCH_ENGINE


def test_persist_discovered_candidates_marks_search_noise_unsupported(
    tmp_path: Path,
) -> None:
    """Obvious non-article search results should be retained but not scrape-eligible."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    discoveries = [
        DiscoveredCandidate(
            producer_name="brave",
            producer_kind=ProducerKind.SEARCH_ENGINE,
            query_text="בית בושת",
            candidate_url=HttpUrl(url),
            canonical_url=HttpUrl(url),
            title="noise",
            snippet="metadata-poor result",
            discovered_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
            metadata={"query_kind": "broad"},
        )
        for url in [
            "https://x.com/example_profile",
            "https://play.google.com/store/apps/details?id=com.example",
            "https://apps.apple.com/il/app/example/id123456789",
            "https://morfix.co.il/example",
            "https://context.reverso.net/translation/hebrew-english/example",
            "https://en.wiktionary.org/wiki/example",
            "https://www.linkedin.com/company/example-org",
            "https://www.instagram.com/example_profile/",
        ]
    ]

    persisted = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-search-noise"),
        discovered_candidates=discoveries,
        persistence=persistence,
    )

    assert {candidate.candidate_status.value for candidate in persisted.candidates} == {
        "unsupported_source"
    }
    assert all(not candidate.needs_review for candidate in persisted.candidates)
    assert {event.producer_kind for event in persisted.provenance} == {ProducerKind.SEARCH_ENGINE}
    assert {
        candidate.metadata["unsupported_source_filter"] for candidate in persisted.candidates
    } == {"search_noise"}
    assert {
        candidate.metadata["unsupported_source_reason"] for candidate in persisted.candidates
    } == {"app_store", "social_profile", "unsupported_search_domain"}
    assert {
        candidate.metadata["unsupported_source_domain"] for candidate in persisted.candidates
    } == {
        "x.com",
        "play.google.com",
        "apps.apple.com",
        "morfix.co.il",
        "context.reverso.net",
        "wiktionary.org",
        "linkedin.com",
        "instagram.com",
    }
    assert {
        candidate.metadata["latest_discovery_metadata"]["search_noise_filter_reason"]
        for candidate in persisted.candidates
    } == {"app_store", "social_profile", "unsupported_search_domain"}
    assert {
        candidate.metadata["latest_discovery_metadata"]["search_noise_filter_domain"]
        for candidate in persisted.candidates
    } == {
        "x.com",
        "play.google.com",
        "apps.apple.com",
        "morfix.co.il",
        "context.reverso.net",
        "wiktionary.org",
        "linkedin.com",
        "instagram.com",
    }


def test_persist_discovered_candidates_demotes_existing_unattempted_search_noise(
    tmp_path: Path,
) -> None:
    """Existing scrapeable noise should be removed from scrape eligibility when rediscovered."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    existing = raw_article_to_discovered_candidate(
        build_raw_article(
            "https://x.com/example_profile",
            source_name="brave",
            title="profile",
        )
    )
    persisted_existing = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-existing"),
        discovered_candidates=[existing],
        persistence=persistence,
    ).candidates[0]
    assert persisted_existing.candidate_status is CandidateStatus.NEW

    rediscovered = DiscoveredCandidate(
        producer_name="exa",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        query_text="בית בושת",
        candidate_url=HttpUrl("https://x.com/example_profile"),
        canonical_url=HttpUrl("https://x.com/example_profile"),
        title="profile",
        snippet="metadata-poor result",
        discovered_at=datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
        metadata={"query_kind": "broad"},
    )

    persisted = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-existing-noise"),
        discovered_candidates=[rediscovered],
        persistence=persistence,
    )

    candidate = persisted.candidates[0]
    assert candidate.candidate_id == persisted_existing.candidate_id
    assert candidate.candidate_status is CandidateStatus.UNSUPPORTED_SOURCE
    assert candidate.scrape_attempt_count == 0
    assert candidate.metadata["unsupported_source_filter"] == "search_noise"
    assert candidate.metadata["unsupported_source_reason"] == "social_profile"
    assert candidate.metadata["unsupported_source_domain"] == "x.com"
    assert candidate.metadata["latest_discovery_metadata"]["search_noise_filter_reason"] == (
        "social_profile"
    )


def test_persist_discovered_candidates_preserves_attempted_existing_noise_status(
    tmp_path: Path,
) -> None:
    """Noise rediscovery should not rewrite candidates with scrape history."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    existing = raw_article_to_discovered_candidate(
        build_raw_article(
            "https://x.com/example_profile",
            source_name="brave",
            title="profile",
        )
    )
    persisted_existing = (
        persist_discovered_candidates(
            run=DiscoveryRun(run_id="run-existing-attempted"),
            discovered_candidates=[existing],
            persistence=persistence,
        )
        .candidates[0]
        .model_copy(
            update={
                "candidate_status": CandidateStatus.PARTIALLY_SCRAPED,
                "content_basis": ContentBasis.PARTIAL_PAGE,
                "scrape_attempt_count": 1,
            }
        )
    )
    persistence.upsert_candidates([persisted_existing])
    rediscovered = DiscoveredCandidate(
        producer_name="exa",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        query_text="בית בושת",
        candidate_url=HttpUrl("https://x.com/example_profile"),
        canonical_url=HttpUrl("https://x.com/example_profile"),
        title="profile",
        snippet="metadata-poor result",
        discovered_at=datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
        metadata={"query_kind": "broad"},
    )

    persisted = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-existing-attempted-noise"),
        discovered_candidates=[rediscovered],
        persistence=persistence,
    )

    candidate = persisted.candidates[0]
    assert candidate.candidate_status is CandidateStatus.PARTIALLY_SCRAPED
    assert candidate.content_basis is ContentBasis.PARTIAL_PAGE
    assert candidate.scrape_attempt_count == 1
    assert "unsupported_source_reason" not in candidate.metadata


def test_persist_discovered_candidates_keeps_supported_news_articles_scrapeable(
    tmp_path: Path,
) -> None:
    """Noise filtering should not suppress normal article URLs from supported news domains."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    discovery = DiscoveredCandidate(
        producer_name="exa",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        query_text="בית בושת",
        candidate_url=HttpUrl("https://www.ynet.co.il/news/article/abc123"),
        canonical_url=HttpUrl("https://www.ynet.co.il/news/article/abc123"),
        title="כתבת חדשות רגילה",
        snippet="חשד לבית בושת",
        discovered_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
        source_hint="ynet",
        metadata={"query_kind": "broad"},
    )

    persisted = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-supported-news"),
        discovered_candidates=[discovery],
        persistence=persistence,
    )

    candidate = persisted.candidates[0]
    assert candidate.candidate_status.value == "new"
    assert candidate.needs_review is False
    assert "search_noise_filter_reason" not in candidate.metadata["latest_discovery_metadata"]


def test_source_native_domain_helpers_normalize_and_fallback() -> None:
    """Source-native social classification should use canonical host normalization."""
    discovered = DiscoveredCandidate(
        producer_name="brave",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        candidate_url=HttpUrl("https://www.facebook.com/story.php?story_fbid=7&id=8"),
        canonical_url=HttpUrl("https://www.facebook.com/story.php?story_fbid=7&id=8"),
        discovered_at=datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
    ).model_copy(update={"domain": None}, deep=True)

    assert _normalize_domain(None) is None
    assert _normalize_domain("   ") is None
    assert _normalize_domain("WWW.FACEBOOK.COM") == "facebook.com"
    assert _candidate_domain(discovered) == "facebook.com"


def test_persist_discovered_candidates_keeps_non_social_broad_results_scrapeable(
    tmp_path: Path,
) -> None:
    """Non-social broad search results should remain in the normal scrape flow."""
    paths = resolve_discovery_state_paths(state_root=tmp_path, dataset_name=DatasetName.NEWS_ITEMS)
    persistence = StateRepoDiscoveryPersistence(paths)
    discovery = DiscoveredCandidate(
        producer_name="brave",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        query_text="בית בושת",
        candidate_url=HttpUrl("https://example.com/news/normal-result"),
        canonical_url=HttpUrl("https://example.com/news/normal-result"),
        title="תוצאה רגילה",
        snippet="חשד לבית בושת",
        discovered_at=datetime(2026, 4, 11, 11, 0, tzinfo=UTC),
        metadata={"query_kind": "broad"},
    )

    persisted = persist_discovered_candidates(
        run=DiscoveryRun(run_id="run-non-social"),
        discovered_candidates=[discovery],
        persistence=persistence,
    )

    candidate = persisted.candidates[0]
    provenance = persisted.provenance[0]
    assert candidate.candidate_status.value == "new"
    assert candidate.needs_review is False
    assert provenance.producer_kind is ProducerKind.SEARCH_ENGINE


def test_persist_discovered_candidates_writes_failed_run_before_reraising() -> None:
    """Persistence failures should update the run record to failed before propagating."""

    class FailingPersistence(StateRepoDiscoveryPersistence):
        def __init__(self, paths: object) -> None:
            super().__init__(paths)
            self.written_runs: list[DiscoveryRun] = []

        def write_run(self, run: DiscoveryRun) -> None:
            self.written_runs.append(run.model_copy(deep=True))

        def upsert_candidates(self, candidates: object) -> None:
            del candidates
            raise RuntimeError("candidate write failed")

    paths = resolve_discovery_state_paths(
        state_root=Path("/tmp/discovery-source-native-failure"),
        dataset_name=DatasetName.NEWS_ITEMS,
    )
    persistence = FailingPersistence(paths)
    discovery = raw_article_to_discovered_candidate(
        build_raw_article("https://www.ynet.co.il/news/article/failure")
    )

    try:
        persist_discovered_candidates(
            run=DiscoveryRun(run_id="run-failure"),
            discovered_candidates=[discovery],
            persistence=persistence,
        )
    except RuntimeError as exc:
        assert str(exc) == "candidate write failed"
    else:
        raise AssertionError("expected RuntimeError")

    assert len(persistence.written_runs) == 1
    failed_run = persistence.written_runs[0]
    assert failed_run.status.value == "failed"
    assert failed_run.finished_at is not None
    assert failed_run.errors == ["persistence: RuntimeError: candidate write failed"]
