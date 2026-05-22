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
    decision = orchestrator.evaluate_thick(article_view, body=article.snippet)
"""

from __future__ import annotations

from urllib.parse import urlparse

from denbust.data_models import RawArticle
from denbust.discovery.models import PersistentCandidate


def _etld1(url_str: str) -> str | None:
    """Return a best-effort eTLD+1 from *url_str* without external dependencies.

    Uses only stdlib ``urllib.parse``; always returns a non-empty string or
    ``None`` — never an empty string.
    """
    host = urlparse(url_str).hostname or ""
    parts = [p for p in host.split(".") if p]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host or None


class PersistentCandidateView:
    """Read-only adapter that exposes :class:`PersistentCandidate` as ``CandidateView``.

    Attribute mapping
    -----------------
    ``candidate_id``
        → ``PersistentCandidate.candidate_id``
    ``domain``
        → ``PersistentCandidate.domain``
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
        return self._c.domain

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
