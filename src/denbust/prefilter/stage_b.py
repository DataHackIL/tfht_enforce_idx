"""Stage B — trained text classifier (Naive Bayes default, SetFit option).

LPF-PR-01 stub: ``evaluate`` always returns ``None`` (stage skipped).
Full implementation lands in LPF-PR-04 (Naive Bayes) and LPF-PR-05 (SetFit).
"""

from __future__ import annotations

from denbust.prefilter.models import CandidateView, PassKind, StageScore


class StageBScorer:
    """Stub scorer for Stage B.

    Returns ``None`` for every candidate so the cascade always passes
    through this stage.  Replace with the real implementation in LPF-PR-04.
    """

    def evaluate(
        self,
        _candidate: CandidateView,
        _pass_kind: PassKind,
        _body: str | None = None,
    ) -> StageScore | None:
        """Return ``None`` — stage is not yet implemented."""
        return None
