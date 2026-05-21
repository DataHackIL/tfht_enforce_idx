"""Labeled-candidates dataset assembly for the prefilter cascade.

Merges manual triage decisions, auto-triage decisions, and past Claude
classifier outputs with a documented conflict-resolution priority, then
assigns a deterministic stratified train/val/test split.

Label priority (highest to lowest):
    triage_manual > claude_classifier > triage_auto

Triage action mapping:
    exclude  (no auto flag) → negative, triage_manual
    prioritize              → positive, triage_manual
    exclude  (auto: true)   → negative, triage_auto
    reset                   → candidate dropped from labeled set
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import random
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from denbust.discovery.state_paths import DiscoveryStatePaths

if TYPE_CHECKING:
    from denbust.ops.storage import OperationalStore

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

LabelSourceName = Literal["triage_manual", "triage_auto", "claude_classifier"]
Label = Literal["positive", "negative"]
Split = Literal["train", "val", "test"]

_SOURCE_PRIORITY: dict[str, int] = {
    "triage_manual": 0,
    "claude_classifier": 1,
    "triage_auto": 2,
}


# ---------------------------------------------------------------------------
# Core data model
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class LabeledCandidate:
    """One labeled candidate row for prefilter model training.

    Attributes
    ----------
    candidate_id:
        Stable identifier matching ``PersistentCandidate.candidate_id``.
    domain:
        Normalized eTLD+1 host (empty string if unavailable).
    url:
        Canonical URL as a plain string.
    title:
        First title string from the candidate snapshot (empty if missing).
    snippet:
        First snippet string from the candidate snapshot (empty if missing).
    article_body:
        Full scraped body text, or ``None`` when the candidate has not yet
        been scraped.
    label:
        ``"positive"`` — index-relevant; ``"negative"`` — true negative.
    label_source:
        Which signal provided the label.
    split:
        Dataset split assignment: ``"train"``, ``"val"``, or ``"test"``.
    labeled_at:
        ISO-8601 UTC timestamp of the source event that set the label.
    decision_hash:
        SHA-1 of the source row used to set the label, for dedup auditing.
    """

    candidate_id: str
    domain: str
    url: str
    title: str
    snippet: str
    article_body: str | None
    label: Label
    label_source: LabelSourceName
    split: Split
    labeled_at: str
    decision_hash: str


# ---------------------------------------------------------------------------
# Parquet I/O
# ---------------------------------------------------------------------------


def _parquet_schema() -> object:
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("candidate_id", pa.string(), nullable=False),
            pa.field("domain", pa.string(), nullable=False),
            pa.field("url", pa.string(), nullable=False),
            pa.field("title", pa.string(), nullable=False),
            pa.field("snippet", pa.string(), nullable=False),
            pa.field("article_body", pa.string(), nullable=True),
            pa.field("label", pa.string(), nullable=False),
            pa.field("label_source", pa.string(), nullable=False),
            pa.field("split", pa.string(), nullable=False),
            pa.field("labeled_at", pa.string(), nullable=False),
            pa.field("decision_hash", pa.string(), nullable=False),
        ]
    )


def write_labels_parquet(rows: list[LabeledCandidate], out_path: Path) -> None:
    """Serialise *rows* to a Parquet file at *out_path*.

    The parent directory is created if it does not exist.
    Requires ``pyarrow`` to be installed.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [dataclasses.asdict(row) for row in rows]
    table = pa.Table.from_pylist(payload, schema=_parquet_schema())
    write_fn: Callable[[Any, Any], None] = pq.write_table
    write_fn(table, out_path)


def read_labels_parquet(path: Path) -> list[LabeledCandidate]:
    """Deserialise a Parquet file written by :func:`write_labels_parquet`.

    Requires ``pyarrow`` to be installed.
    """
    import pyarrow.parquet as pq

    read_fn: Callable[[Any], Any] = pq.read_table
    table = read_fn(path)
    out: list[LabeledCandidate] = []
    for row in table.to_pylist():
        out.append(
            LabeledCandidate(
                candidate_id=str(row["candidate_id"]),
                domain=str(row["domain"]),
                url=str(row["url"]),
                title=str(row["title"]),
                snippet=str(row["snippet"]),
                article_body=row["article_body"],
                label=cast(Label, row["label"]),
                label_source=cast(LabelSourceName, row["label_source"]),
                split=cast(Split, row["split"]),
                labeled_at=str(row["labeled_at"]),
                decision_hash=str(row["decision_hash"]),
            )
        )
    return out


# ---------------------------------------------------------------------------
# Assembly helpers
# ---------------------------------------------------------------------------


def _row_hash(row: dict[str, object]) -> str:
    serialized = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(serialized.encode()).hexdigest()  # noqa: S324


def _latest_triage_decisions(decisions_path: Path) -> dict[str, dict[str, object]]:
    """Return the latest triage decision per ``candidate_id`` (file order wins)."""
    latest: dict[str, dict[str, object]] = {}
    if not decisions_path.exists():
        return latest
    with decisions_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row: dict[str, object] = json.loads(line)
            except json.JSONDecodeError:
                continue
            cid = row.get("candidate_id")
            if cid:
                latest[str(cid)] = row
    return latest


def _triage_label(
    decision: dict[str, object],
) -> tuple[Label, LabelSourceName] | None:
    """Map one triage decision to ``(label, source)``.

    Returns ``None`` for ``reset`` decisions (candidate dropped from label set).
    """
    action = decision.get("action")
    is_auto = bool(decision.get("auto", False))
    if action == "prioritize":
        return "positive", "triage_manual"
    if action == "exclude":
        if is_auto:
            return "negative", "triage_auto"
        return "negative", "triage_manual"
    return None  # reset or unknown: drop


def _load_candidates(candidates_path: Path) -> dict[str, dict[str, object]]:
    """Return ``{candidate_id: record}`` from a ``latest_candidates.jsonl`` file."""
    candidates: dict[str, dict[str, object]] = {}
    if not candidates_path.exists():
        return candidates
    with candidates_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row: dict[str, object] = json.loads(line)
            except json.JSONDecodeError:
                continue
            cid = row.get("candidate_id")
            if cid:
                candidates[str(cid)] = row
    return candidates


def _operational_labels(
    store: OperationalStore,
    dataset_name: str,
) -> dict[str, tuple[Label, str]]:
    """Return ``{canonical_url: (label, labeled_at)}`` from the operational store.

    Failures are silenced — the store is optional infrastructure.
    """
    result: dict[str, tuple[Label, str]] = {}
    try:
        records = store.fetch_records(dataset_name)
    except Exception:  # noqa: BLE001
        return result
    for rec in records:
        url = rec.get("canonical_url")
        if not url:
            continue
        index_relevant = rec.get("index_relevant")
        if index_relevant is None:
            continue
        lbl: Label = "positive" if index_relevant else "negative"
        labeled_at = str(rec.get("updated_at") or rec.get("created_at") or "")
        result[str(url)] = (lbl, labeled_at)
    return result


def _assign_splits(
    rows: list[LabeledCandidate],
    seed: int,
    val_fraction: float,
    test_fraction: float,
) -> list[LabeledCandidate]:
    """Return *rows* with ``split`` fields filled by stratified sampling.

    Stratification key: ``(label, label_source)``.  Within each stratum,
    candidates are sorted by ``candidate_id`` before shuffling so the
    assignment is independent of file-read order.
    """
    strata: dict[tuple[str, str], list[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        strata[(row.label, row.label_source)].append(idx)

    rng = random.Random(seed)
    split_by_idx: dict[int, Split] = {}

    for indices in strata.values():
        # stable sort before shuffle so file-order doesn't affect outcome
        indices.sort(key=lambda i: rows[i].candidate_id)
        rng.shuffle(indices)
        n = len(indices)
        n_val = round(val_fraction * n)
        n_test = round(test_fraction * n)
        # never exhaust the training split entirely
        if n_val + n_test >= n:
            n_val = min(n_val, max(0, n - 1))
            n_test = min(n_test, max(0, n - n_val - 1))
        n_train = n - n_val - n_test
        for i in indices[:n_train]:
            split_by_idx[i] = "train"
        for i in indices[n_train : n_train + n_val]:
            split_by_idx[i] = "val"
        for i in indices[n_train + n_val :]:
            split_by_idx[i] = "test"

    return [
        LabeledCandidate(
            candidate_id=row.candidate_id,
            domain=row.domain,
            url=row.url,
            title=row.title,
            snippet=row.snippet,
            article_body=row.article_body,
            label=row.label,
            label_source=row.label_source,
            split=split_by_idx[i],
            labeled_at=row.labeled_at,
            decision_hash=row.decision_hash,
        )
        for i, row in enumerate(rows)
    ]


# ---------------------------------------------------------------------------
# Public assembly entry-point
# ---------------------------------------------------------------------------


def assemble_labels(
    discovery_paths: DiscoveryStatePaths,
    operational_store: OperationalStore | None = None,
    *,
    seed: int = 20260521,
    val_fraction: float = 0.15,
    test_fraction: float = 0.15,
) -> list[LabeledCandidate]:
    """Assemble the labeled-candidates dataset.

    Parameters
    ----------
    discovery_paths:
        Resolved discovery state paths (provides candidates directory).
    operational_store:
        Optional operational store for Claude classifier labels.  When
        ``None`` or when ``fetch_records`` fails, classifier labels are
        silently skipped.
    seed:
        RNG seed for the deterministic stratified split.
    val_fraction:
        Fraction of each stratum to allocate to the validation split.
    test_fraction:
        Fraction of each stratum to allocate to the test split.

    Returns
    -------
    list[LabeledCandidate]
        Rows with split assignments, sorted by ``candidate_id``.
    """
    decisions_path = discovery_paths.candidates_dir / "triage_decisions.jsonl"
    triage = _latest_triage_decisions(decisions_path)
    candidates = _load_candidates(discovery_paths.latest_candidates_path)

    # Build url → candidate_id index for joining with the operational store
    url_to_cid: dict[str, str] = {}
    for cid, cand in candidates.items():
        url = cand.get("canonical_url") or cand.get("current_url")
        if url:
            url_to_cid[str(url)] = cid

    # label_map: candidate_id → (label, source, labeled_at, decision_hash)
    label_map: dict[str, tuple[Label, LabelSourceName, str, str]] = {}

    # 1. Seed with claude_classifier labels (lowest priority above triage_auto)
    if operational_store is not None:
        op_labels = _operational_labels(operational_store, str(discovery_paths.dataset_name))
        for url, (lbl, labeled_at) in op_labels.items():
            maybe_cid = url_to_cid.get(url)
            if maybe_cid is None:
                continue
            dhash = _row_hash({"source": "claude_classifier", "canonical_url": url, "label": lbl})
            label_map[maybe_cid] = (lbl, "claude_classifier", labeled_at, dhash)

    # 2. Process triage decisions: override by priority or drop for reset
    for cid, decision in triage.items():
        result = _triage_label(decision)
        if result is None:
            # reset: drop from the labeled set entirely
            label_map.pop(cid, None)
            continue
        triage_lbl, triage_src = result
        existing = label_map.get(cid)
        if existing is None or _SOURCE_PRIORITY[triage_src] < _SOURCE_PRIORITY[existing[1]]:
            labeled_at = str(decision.get("decided_at", ""))
            label_map[cid] = (triage_lbl, triage_src, labeled_at, _row_hash(dict(decision)))

    # 3. Build LabeledCandidate rows (split placeholder = "train")
    rows: list[LabeledCandidate] = []
    for cid, (lbl, src, labeled_at, dhash) in label_map.items():
        maybe_cand = candidates.get(cid)
        if maybe_cand is None:
            continue  # no candidate snapshot for this id; skip
        titles: list[str] = cast(list[str], maybe_cand.get("titles") or [])
        snippets: list[str] = cast(list[str], maybe_cand.get("snippets") or [])
        rows.append(
            LabeledCandidate(
                candidate_id=cid,
                domain=str(maybe_cand.get("domain") or ""),
                url=str(maybe_cand.get("canonical_url") or maybe_cand.get("current_url") or ""),
                title=titles[0] if titles else "",
                snippet=snippets[0] if snippets else "",
                article_body=None,
                label=lbl,
                label_source=src,
                split="train",
                labeled_at=labeled_at,
                decision_hash=dhash,
            )
        )

    if not rows:
        return []

    rows.sort(key=lambda r: r.candidate_id)
    return _assign_splits(rows, seed=seed, val_fraction=val_fraction, test_fraction=test_fraction)
