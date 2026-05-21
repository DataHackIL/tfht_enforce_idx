"""Stage A — scored lexicon + domain reputation + URL heuristics.

LPF-PR-01 stub: ``evaluate`` always returns ``None`` (stage skipped).
Full implementation lands in LPF-PR-03.
"""

from __future__ import annotations

from denbust.prefilter.models import CandidateView, StageScore


class StageAScorer:
    """Stub scorer for Stage A.

    Returns ``None`` for every candidate so the cascade always passes
    through this stage.  Replace with the real implementation in LPF-PR-03.
    """

    def evaluate(self, _candidate: CandidateView) -> StageScore | None:
        """Return ``None`` — stage is not yet implemented."""
        return None
