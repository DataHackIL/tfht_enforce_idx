"""Yield-weighted query prioritization.

Not all search keywords are equal: ``סחר בבני אדם`` (human trafficking) and
``בית בושת`` (brothel) have produced verified enforcement records, while others
mostly surface noise. This module measures, per query text, how many
index-relevant records that query actually contributed to discovering, and
caches the map so the budget cap can spend on proven keywords first.

The signal chain is record → candidate → query:

* an index-relevant operational record lists the ``event_candidate_ids`` it was
  built from;
* each candidate records the ``discovery_queries`` (keyword/term texts) that
  found it;
* so each query text that fed an index-relevant record earns one yield point.

``compute_query_yield`` is pure (takes plain inputs) for testability;
``QueryYieldStore`` caches the result as JSON under the discovery state dir.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any


def compute_query_yield(
    records: Iterable[Mapping[str, Any]],
    candidate_queries: Mapping[str, Sequence[str]],
) -> dict[str, int]:
    """Return ``{query_text: index_relevant_record_count}``.

    *records* are operational rows (dicts) with ``index_relevant`` and
    ``event_candidate_ids``. *candidate_queries* maps candidate id → the query
    texts that discovered it. A query text earns one point per distinct
    index-relevant record any of its candidates contributed to.
    """
    yield_map: dict[str, int] = defaultdict(int)
    for record in records:
        if not record.get("index_relevant"):
            continue
        texts: set[str] = set()
        for candidate_id in record.get("event_candidate_ids") or []:
            for query_text in candidate_queries.get(candidate_id, ()):
                if query_text:
                    texts.add(query_text)
        for query_text in texts:
            yield_map[query_text] += 1
    return dict(yield_map)


class QueryYieldStore:
    """JSON cache of ``{query_text: yield}`` under the discovery state dir."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, int]:
        if not self.path.exists():
            return {}
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}
        return {str(k): int(v) for k, v in data.items()}

    def save(self, yield_map: Mapping[str, int]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        ordered = dict(sorted(yield_map.items(), key=lambda kv: (-kv[1], kv[0])))
        self.path.write_text(json.dumps(ordered, ensure_ascii=False, indent=2), encoding="utf-8")
