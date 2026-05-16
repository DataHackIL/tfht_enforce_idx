"""Unit tests for discovery candidate filtering policy."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import HttpUrl

from denbust.discovery.candidate_filters import (
    classify_search_noise,
    classify_title_noise,
    globally_excluded_search_domains,
    globally_excluded_title_terms,
)
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
        (
            "https://sport1.maariv.co.il/israeli-soccer/ligat-haal/article/1739884",
            "irrelevant_content_domain",
            "sport1.maariv.co.il",
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


def test_classify_search_noise_irrelevant_content_domain_subdomain_match() -> None:
    """Subdomain of an irrelevant-content domain should also be classified as noise."""
    # sport1.maariv.co.il is a sub-domain of maariv.co.il but only the sub-domain
    # is listed — maariv.co.il articles should still pass through.
    maariv_candidate = _search_candidate("https://www.maariv.co.il/news/law/article-1")
    sport1_candidate = _search_candidate(
        "https://sport1.maariv.co.il/israeli-soccer/ligat-haal/article/1739884"
    )

    assert classify_search_noise(maariv_candidate) is None
    classification = classify_search_noise(sport1_candidate)
    assert classification is not None
    assert classification.reason == "irrelevant_content_domain"
    assert classification.matched_domain == "sport1.maariv.co.il"


def test_globally_excluded_search_domains_contains_sport1_maariv() -> None:
    """The globally-excluded domain set should include the sports sub-domain."""
    excluded = globally_excluded_search_domains()

    assert "sport1.maariv.co.il" in excluded


def test_globally_excluded_search_domains_does_not_contain_parent_domain() -> None:
    """Only the off-topic sub-domain is excluded, not the broader news outlet."""
    excluded = globally_excluded_search_domains()

    assert "maariv.co.il" not in excluded


def _candidate_with_title(
    title: str, producer_kind: ProducerKind = ProducerKind.SEARCH_ENGINE
) -> DiscoveredCandidate:
    return DiscoveredCandidate(
        producer_name="brave",
        producer_kind=producer_kind,
        query_text="בית בושת",
        candidate_url=HttpUrl("https://www.ynet.co.il/news/article/abc123"),
        canonical_url=HttpUrl("https://www.ynet.co.il/news/article/abc123"),
        title=title,
        discovered_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("title", "expected_keyword"),
    [
        ('כוחות צה"ל פשטו על המקום', 'צה"ל'),
        ("פעולות צה״ל בעזה", "צה״ל"),
        ("חייל צהל נהרג בפיגוע", "צהל"),
        ('דו"ח: צה"ל הפר הסכמים', 'צה"ל'),
        ("הצהרת צהל על הפסקת אש", "צהל"),
        ("ישראל וטראמפ דנים בהסכם", "טראמפ"),
        ("נתניהו נפגש עם שגריר אמריקה", "נתניהו"),
        ("מתיחות עם איראן בשיא", "איראן"),
        ("חיזבאללה ירה על הצפון", "חיזבאללה"),
        ("מבצע ספורט לנוער בתל אביב", "ספורט"),
        ("מניות הבנקים ירדו הבוקר", "מניות"),
        ("לוחמים בעזה בלילה האחרון", "עזה"),
    ],
)
def test_classify_title_noise_matches_idf_terms(title: str, expected_keyword: str) -> None:
    """Titles containing excluded terms should be classified as title keyword noise."""
    classification = classify_title_noise(_candidate_with_title(title))

    assert classification is not None
    assert classification.reason == "title_keyword_match"
    assert classification.matched_keyword == expected_keyword


@pytest.mark.parametrize(
    "title",
    [
        "עשרות נשים נסחרו לזנות בתל אביב",
        "בית בושת נסגר בצו שיפוטי",
        "סוחרי סמים נעצרו ליד בית בושת",
        "נשים קורבנות סחר מינפגישות עם מטפלים",
    ],
)
def test_classify_title_noise_ignores_unrelated_titles(title: str) -> None:
    """Titles without excluded keywords should not be filtered."""
    assert classify_title_noise(_candidate_with_title(title)) is None


def test_classify_title_noise_ignores_candidate_without_title() -> None:
    """Candidates with no title should pass through the title noise filter."""
    candidate = _candidate_with_title("").model_copy(update={"title": None})
    assert classify_title_noise(candidate) is None


def test_classify_title_noise_applies_to_source_native_candidates() -> None:
    """Title noise filter applies regardless of producer kind — it is a cost filter."""
    candidate = _candidate_with_title('כוחות צה"ל', producer_kind=ProducerKind.SOURCE_NATIVE)
    classification = classify_title_noise(candidate)

    assert classification is not None
    assert classification.reason == "title_keyword_match"


def test_globally_excluded_title_terms_contains_expected_terms() -> None:
    """The excluded-terms set should include IDF forms and all configured off-topic terms."""
    terms = globally_excluded_title_terms()

    assert 'צה"ל' in terms
    assert "צה״ל" in terms
    assert "צהל" in terms
    assert "איראן" in terms
    assert "נתניהו" in terms
    assert "טראמפ" in terms
    assert "חיזבאללה" in terms
    assert "ספורט" in terms
    assert "מניות" in terms
    assert "עזה" in terms
