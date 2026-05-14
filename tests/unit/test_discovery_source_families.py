"""Unit tests for search-discovered source-family helpers."""

from __future__ import annotations

from denbust.discovery.source_families import (
    generic_fetch_source_domains,
    source_family_name_for_domain,
    source_family_name_for_url,
)


def test_source_family_name_for_domain_matches_known_article_domains() -> None:
    """Known source-family domains should resolve to stable source labels."""
    assert source_family_name_for_domain("www.globes.co.il") == "globes"
    assert source_family_name_for_domain("www.themarker.com") == "themarker"
    assert source_family_name_for_domain("www.israelhayom.co.il") == "israelhayom"
    assert (
        source_family_name_for_url("https://www.israelhayom.co.il/news/law/article/19616169")
        == "israelhayom"
    )


def test_israelhayom_family_matching_is_exact_to_main_domain() -> None:
    """Israel Hayom support is bounded to the main domain until subdomains are justified."""
    assert source_family_name_for_domain("knesset.israelhayom.co.il") is None
    assert source_family_name_for_domain("nadlan.israelhayom.co.il") is None
    assert source_family_name_for_domain("coalition.israelhayom.co.il") is None


def test_source_targeted_discovery_domains_excludes_israelhayom_until_extraction_evidence() -> None:
    """Candidate-only evidence should not automatically spend recurring source-query budget."""
    assert generic_fetch_source_domains() == [
        ("globes", "www.globes.co.il"),
        ("themarker", "www.themarker.com"),
    ]
