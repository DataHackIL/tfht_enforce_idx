"""Unit tests for discovery candidate filtering policy."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import HttpUrl

from denbust.discovery.candidate_filters import classify_search_noise
from denbust.discovery.models import DiscoveredCandidate, ProducerKind


def _search_candidate(url: str) -> DiscoveredCandidate:
    return DiscoveredCandidate(
        producer_name="brave",
        producer_kind=ProducerKind.SEARCH_ENGINE,
        query_text="בית בושת",
        candidate_url=HttpUrl(url),
        canonical_url=HttpUrl(url),
        discovered_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("url", "reason", "matched_domain"),
    [
        ("https://x.com/example_profile", "social_profile", "x.com"),
        ("https://mobile.twitter.com/example_profile", "social_profile", "twitter.com"),
        (
            "https://play.google.com/store/apps/details?id=com.example",
            "app_store",
            "play.google.com",
        ),
        (
            "https://apps.apple.com/il/app/example/id123456789",
            "app_store",
            "apps.apple.com",
        ),
        ("https://morfix.co.il/example", "unsupported_search_domain", "morfix.co.il"),
        (
            "https://context.reverso.net/translation/hebrew-english/example",
            "unsupported_search_domain",
            "context.reverso.net",
        ),
        (
            "https://dictionary.reverso.net/hebrew-english/example",
            "unsupported_search_domain",
            "dictionary.reverso.net",
        ),
        ("https://en.wiktionary.org/wiki/example", "unsupported_search_domain", "wiktionary.org"),
        ("https://www.pealim.com/dict/1", "unsupported_search_domain", "pealim.com"),
        ("https://m.facebook.com/profile.php?id=123", "social_profile", "facebook.com"),
        ("https://m.linkedin.com/company/example-org", "social_profile", "linkedin.com"),
        ("https://www.instagram.com/example_profile/", "social_profile", "instagram.com"),
        ("https://m.youtube.com/@example", "social_profile", "youtube.com"),
        ("https://www.tiktok.com/@example", "social_profile", "tiktok.com"),
    ],
)
def test_classify_search_noise_marks_expected_noise_domains(
    url: str,
    reason: str,
    matched_domain: str,
) -> None:
    """Each configured noise URL should produce its exact reason and matched base domain."""
    classification = classify_search_noise(_search_candidate(url))

    assert classification is not None
    assert classification.reason == reason
    assert classification.matched_domain == matched_domain


@pytest.mark.parametrize(
    "url",
    [
        "https://www.ynet.co.il/news/article/abc123",
        "https://www.maariv.co.il/news/law/article-1270778",
        "https://sport1.maariv.co.il/israeli-soccer/ligat-haal/article/1739884",
        "https://x.com/example/status/123456789",
        "https://mobile.twitter.com/example/status/123456789",
        "https://facebook.com/story.php?story_fbid=5&id=6",
        "https://www.instagram.com/p/example-post/",
        "https://www.instagram.com/reel/example-post/",
        "https://www.tiktok.com/@example/video/123456789",
        "https://play.google.com/books/reader?id=example",
        "https://apps.apple.com/us/story/id123456789",
    ],
)
def test_classify_search_noise_keeps_article_and_social_post_urls(url: str) -> None:
    """Article/post-like URLs and non-app store paths should remain scrapeable candidates."""
    assert classify_search_noise(_search_candidate(url)) is None


def test_classify_search_noise_ignores_source_native_candidates() -> None:
    """Source-native article discovery should not be filtered by search-result policy."""
    candidate = _search_candidate("https://x.com/example_profile").model_copy(
        update={"producer_kind": ProducerKind.SOURCE_NATIVE}
    )

    assert classify_search_noise(candidate) is None
