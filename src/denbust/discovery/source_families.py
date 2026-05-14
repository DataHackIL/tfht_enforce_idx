"""Known source-family helpers for search-discovered article candidates."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

from denbust.discovery.candidate_filters import normalize_domain


@dataclass(frozen=True)
class SourceFamily:
    """A search-discovered source family supported without source-native discovery."""

    name: str
    domains: frozenset[str]
    discovery_domain: str
    include_subdomains: bool = True
    source_targeted_discovery: bool = True


GENERIC_FETCH_SOURCE_FAMILIES: tuple[SourceFamily, ...] = (
    SourceFamily(
        name="globes",
        domains=frozenset({"globes.co.il"}),
        discovery_domain="www.globes.co.il",
    ),
    SourceFamily(
        name="themarker",
        domains=frozenset({"themarker.com"}),
        discovery_domain="www.themarker.com",
    ),
    SourceFamily(
        name="israelhayom",
        domains=frozenset({"israelhayom.co.il"}),
        discovery_domain="www.israelhayom.co.il",
        include_subdomains=False,
        source_targeted_discovery=False,
    ),
    SourceFamily(
        name="kan",
        domains=frozenset({"kan.org.il"}),
        discovery_domain="www.kan.org.il",
        include_subdomains=False,
        source_targeted_discovery=False,
    ),
)


def source_family_name_for_domain(domain: str | None) -> str | None:
    """Return the known generic-fetch source family for a domain."""
    normalized = normalize_domain(domain)
    if normalized is None:
        return None
    for family in GENERIC_FETCH_SOURCE_FAMILIES:
        for family_domain in family.domains:
            if normalized == family_domain or (
                family.include_subdomains and normalized.endswith(f".{family_domain}")
            ):
                return family.name
    return None


def source_family_name_for_url(url: str | None) -> str | None:
    """Return the known generic-fetch source family for a URL."""
    if not url:
        return None
    return source_family_name_for_domain(urlparse(url).netloc)


def generic_fetch_source_domains() -> list[tuple[str, str]]:
    """Return source-targeted discovery domains for generic-fetch families."""
    return [
        (family.name, family.discovery_domain)
        for family in GENERIC_FETCH_SOURCE_FAMILIES
        if family.source_targeted_discovery
    ]
