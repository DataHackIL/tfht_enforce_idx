"""Stage D — local SLM logprob judge (DictaLM-2.0-Instruct via MLX).

LPF-PR-01 stub: ``evaluate`` always returns ``None`` (stage skipped).
Full implementation lands in LPF-PR-07.
"""

from __future__ import annotations

from denbust.prefilter.models import CandidateView, StageScore


class StageDJudge:
    """Stub judge for Stage D.

    Returns ``None`` for every candidate so the cascade always passes
    through this stage.  Replace with the real implementation in LPF-PR-07.
    """

    def evaluate(
        self,
        _candidate: CandidateView,
        _body: str | None = None,
    ) -> StageScore | None:
        """Return ``None`` — stage is not yet implemented."""
        return None
