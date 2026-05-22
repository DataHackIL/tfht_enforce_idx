"""Adapters exposing pipeline objects as CandidateView for the prefilter cascade.

This module bridges :class:`~denbust.discovery.models.PersistentCandidate` and
:class:`~denbust.data_models.RawArticle` – which live in the pipeline data layer –
into the :class:`~denbust.prefilter.models.CandidateView` protocol consumed by the
cascade stages.  Both adapters are lightweight (no copies, no I/O) and are kept
here rather than inside ``discovery/`` or ``data_models`` to avoid introducing a
dependency cycle.

Usage example::

    from denbust.prefilter.adapters import PersistentCandidateView, RawArticleCandidateView

    view = PersistentCandidateView(candidate)
    decision = orchestrator.evaluate_thin(view)

    article_view = RawArticleCandidateView(article, candidate_id=cid)
    decision = orchestrator.evaluate_thick(article_view, body=None)
"""

from __future__ import annotations

from urllib.parse import urlparse

from denbust.data_models import RawArticle
from denbust.discovery.models import PersistentCandidate

# ---------------------------------------------------------------------------
# Two-part TLD suffixes (no public-suffix-list dependency)
#
# ``_etld1_from_host`` checks whether the last two labels of a hostname form a
# known two-part suffix and, if so, takes one extra label so that, e.g.,
# ``www.ynet.co.il`` yields ``ynet.co.il`` rather than the useless ``co.il``.
# The set covers the suffixes relevant to Israeli news and a handful of other
# common two-part ccTLDs.  It is intentionally small; add entries as needed
# rather than pulling in ``tldextract``.
# ---------------------------------------------------------------------------

_TWO_PART_TLDS: frozenset[str] = frozenset(
    {
        # Israeli
        "co.il",
        "org.il",
        "net.il",
        "ac.il",
        "gov.il",
        "k12.il",
        # British
        "co.uk",
        "org.uk",
        "me.uk",
        "net.uk",
        "ltd.uk",
        "plc.uk",
        # Australian / New Zealand
        "com.au",
        "net.au",
        "org.au",
        "edu.au",
        "co.nz",
        "org.nz",
        "net.nz",
        # South African
        "co.za",
        "org.za",
    }
)


def _etld1_from_host(host: str) -> str | None:
    """Return a best-effort eTLD+1 from a bare *host* string.

    Unlike a naïve last-two-labels approach, this function checks against a
    curated :data:`_TWO_PART_TLDS` set so that ``www.ynet.co.il`` yields
    ``ynet.co.il`` rather than the meaningless ``co.il``.

    Always returns a non-empty string or ``None`` — never an empty string.
    """
    parts = [p for p in host.split(".") if p]
    if len(parts) >= 3:
        two_part = ".".join(parts[-2:])
        if two_part in _TWO_PART_TLDS:
            return ".".join(parts[-3:])
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return parts[0] if parts else None


def _etld1(url_str: str) -> str | None:
    """Return a best-effort eTLD+1 from a full *url_str* without external dependencies.

    Delegates hostname extraction to stdlib :func:`urllib.parse.urlparse`, then
    calls :func:`_etld1_from_host`.  Always returns a non-empty string or
    ``None`` — never an empty string.
    """
    host = urlparse(url_str).hostname or ""
    return _etld1_from_host(host)


class PersistentCandidateView:
    """Read-only adapter that exposes :class:`PersistentCandidate` as ``CandidateView``.

    Attribute mapping
    -----------------
    ``candidate_id``
        → ``PersistentCandidate.candidate_id``
    ``domain``
        → best-effort eTLD+1 derived from ``PersistentCandidate.domain`` (the
        stored netloc) via :func:`_etld1_from_host`.  For example, a stored
        domain of ``www.ynet.co.il`` yields ``ynet.co.il``.  Consistent with
        :class:`RawArticleCandidateView`.
    ``title``
        → first element of ``PersistentCandidate.titles``, or ``None``
    ``snippet``
        → first element of ``PersistentCandidate.snippets``, or ``None``
    ``url``
        → ``str(canonical_url)`` when set, else ``str(current_url)``
    """

    def __init__(self, candidate: PersistentCandidate) -> None:
        self._c = candidate

    @property
    def candidate_id(self) -> str:
        return self._c.candidate_id

    @property
    def domain(self) -> str | None:
        # _c.domain stores the raw netloc (e.g. "www.ynet.co.il").  Normalise
        # to eTLD+1 so domain-reputation lookups use the same key regardless of
        # whether a candidate or a raw article is being evaluated.
        return _etld1_from_host(self._c.domain or "")

    @property
    def title(self) -> str | None:
        return self._c.titles[0] if self._c.titles else None

    @property
    def snippet(self) -> str | None:
        return self._c.snippets[0] if self._c.snippets else None

    @property
    def url(self) -> str | None:
        return str(self._c.canonical_url or self._c.current_url)


class RawArticleCandidateView:
    """Read-only adapter that exposes :class:`RawArticle` as ``CandidateView``.

    Parameters
    ----------
    article:
        The scraped article to wrap.
    candidate_id:
        The persistent-candidate ID that produced this article.  Callers
        should supply the ID from the scrape batch's ``selected_candidates``
        list; fall back to ``str(article.url)`` when the mapping is absent.

    Attribute mapping
    -----------------
    ``candidate_id``
        → caller-supplied (see *candidate_id* above)
    ``domain``
        → best-effort eTLD+1 extracted from ``article.url`` via :func:`_etld1`
    ``title``
        → ``article.title``
    ``snippet``
        → ``article.snippet``
    ``url``
        → ``str(article.url)``
    """

    def __init__(self, article: RawArticle, *, candidate_id: str) -> None:
        self._a = article
        self._candidate_id = candidate_id

    @property
    def candidate_id(self) -> str:
        return self._candidate_id

    @property
    def domain(self) -> str | None:
        return _etld1(str(self._a.url))

    @property
    def title(self) -> str | None:
        return self._a.title

    @property
    def snippet(self) -> str | None:
        return self._a.snippet

    @property
    def url(self) -> str | None:
        return str(self._a.url)
