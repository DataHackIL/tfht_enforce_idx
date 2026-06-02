"""Concrete storage backends and factories for discovery persistence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import httpx

from denbust.config import Config, OperationalProvider
from denbust.discovery.models import (
    BackfillBatch,
    BackfillBatchStatus,
    CandidateProvenance,
    CandidateStatus,
    DiscoveryRun,
    ExecutedBackfillQuery,
    PersistentCandidate,
    ScrapeAttempt,
)
from denbust.discovery.persistence import (
    BackfillBatchStore,
    BackfillCandidateCounts,
    CandidateStore,
    DiscoveryRunStore,
    ExecutedQueryStore,
    ProvenanceStore,
    ScrapeAttemptStore,
)
from denbust.discovery.state_paths import (
    DiscoveryStatePaths,
    write_candidate_jsonl,
    write_discovery_run_snapshot,
    write_json_snapshot,
    write_model_jsonl,
)


def _remove_postgres_unsupported_nuls(value: Any) -> Any:
    """Remove NUL characters before sending JSON/text payloads to PostgREST."""
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_remove_postgres_unsupported_nuls(item) for item in value]
    if isinstance(value, dict):
        return {key: _remove_postgres_unsupported_nuls(item) for key, item in value.items()}
    return value


def _executed_query_key(record: ExecutedBackfillQuery) -> tuple[str, ...]:
    return (
        record.engine,
        record.query_kind.value,
        record.query_text,
        record.source_hint or "",
        record.date_from.isoformat(),
        record.date_to.isoformat(),
    )


class DiscoveryPersistence(
    DiscoveryRunStore,
    CandidateStore,
    BackfillBatchStore,
    ProvenanceStore,
    ScrapeAttemptStore,
    ExecutedQueryStore,
):
    """Combined discovery persistence boundary."""

    def close(self) -> None:
        """Release any backend resources held by the persistence layer."""
        return None


class NullDiscoveryPersistence(DiscoveryPersistence):
    """No-op discovery persistence backend."""

    def write_run(self, run: DiscoveryRun) -> None:
        del run

    def upsert_candidates(self, candidates: Sequence[PersistentCandidate]) -> None:
        del candidates

    def get_candidate(self, candidate_id: str) -> PersistentCandidate | None:
        del candidate_id
        return None

    def list_candidates(
        self,
        *,
        statuses: Sequence[CandidateStatus] | None = None,
        backfill_batch_id: str | None = None,
        limit: int | None = None,
    ) -> list[PersistentCandidate]:
        del statuses, backfill_batch_id, limit
        return []

    def count_candidates(
        self,
        *,
        statuses: Sequence[CandidateStatus] | None = None,
        backfill_batch_id: str | None = None,
    ) -> int:
        del statuses, backfill_batch_id
        return 0

    def count_backfill_batch_candidates(
        self,
        *,
        batch_id: str,
        scrapeable_statuses: Sequence[CandidateStatus],
    ) -> BackfillCandidateCounts:
        del batch_id, scrapeable_statuses
        return BackfillCandidateCounts(merged_candidate_count=0, queued_for_scrape_count=0)

    def upsert_backfill_batches(self, batches: Sequence[BackfillBatch]) -> None:
        del batches

    def get_backfill_batch(self, batch_id: str) -> BackfillBatch | None:
        del batch_id
        return None

    def list_backfill_batches(
        self,
        *,
        statuses: Sequence[BackfillBatchStatus] | None = None,
        limit: int | None = None,
    ) -> list[BackfillBatch]:
        del statuses, limit
        return []

    def find_candidate_by_urls(
        self,
        *,
        canonical_url: str | None,
        current_url: str,
    ) -> PersistentCandidate | None:
        del canonical_url, current_url
        return None

    def append_provenance(self, events: Sequence[CandidateProvenance]) -> None:
        del events

    def list_provenance(
        self,
        candidate_id: str,
        *,
        limit: int | None = None,
    ) -> list[CandidateProvenance]:
        del candidate_id, limit
        return []

    def append_attempts(self, attempts: Sequence[ScrapeAttempt]) -> None:
        del attempts

    def list_attempts(
        self,
        candidate_id: str,
        *,
        limit: int | None = None,
    ) -> list[ScrapeAttempt]:
        del candidate_id, limit
        return []

    def load_executed_backfill_query_keys(self) -> frozenset[tuple[str, ...]]:
        return frozenset()

    def append_executed_backfill_queries(self, queries: Sequence[ExecutedBackfillQuery]) -> None:
        del queries

    def close(self) -> None:
        return None


class StateRepoDiscoveryPersistence(DiscoveryPersistence):
    """State-repo-backed discovery persistence using JSONL files.

    The candidate store is loaded from disk once (lazily on first access) into
    two in-memory indexes:

    * ``_by_id``  – candidate_id → PersistentCandidate (the authoritative map)
    * ``_by_url`` – url string    → candidate_id       (O(1) URL lookups)

    Both indexes are kept in sync on every ``upsert_candidates`` call, so
    subsequent ``find_candidate_by_urls`` calls are O(1) instead of scanning
    the full JSONL file on every invocation.
    """

    def __init__(self, paths: DiscoveryStatePaths) -> None:
        self.paths = paths
        self._by_id: dict[str, PersistentCandidate] = {}
        self._by_url: dict[str, str] = {}  # url string → candidate_id
        self._index_loaded: bool = False

    # ------------------------------------------------------------------
    # Internal index helpers
    # ------------------------------------------------------------------

    def _ensure_index(self) -> None:
        """Load the candidate store into memory on first access (lazy)."""
        if self._index_loaded:
            return
        for candidate in self._read_jsonl(self.paths.latest_candidates_path, PersistentCandidate):
            self._index_candidate(candidate)
        self._index_loaded = True

    def _index_candidate(self, candidate: PersistentCandidate) -> None:
        """Register *candidate* in both lookup indexes."""
        self._by_id[candidate.candidate_id] = candidate
        if candidate.canonical_url is not None:
            self._by_url[str(candidate.canonical_url)] = candidate.candidate_id
        self._by_url[str(candidate.current_url)] = candidate.candidate_id

    @property
    def provenance_path(self) -> Path:
        return self.paths.candidate_provenance_path

    @property
    def attempts_path(self) -> Path:
        return self.paths.scrape_attempts_path

    def write_run(self, run: DiscoveryRun) -> None:
        write_discovery_run_snapshot(
            self.paths.runs_dir,
            run.model_dump(mode="json"),
            run_timestamp=run.started_at,
        )

    def upsert_candidates(self, candidates: Sequence[PersistentCandidate]) -> None:
        if not candidates:
            return
        self._ensure_index()
        for candidate in candidates:
            self._index_candidate(candidate)

        all_candidates = sorted(
            self._by_id.values(),
            key=lambda candidate: (
                candidate.last_seen_at,
                candidate.first_seen_at,
                candidate.candidate_id,
            ),
            reverse=True,
        )
        write_candidate_jsonl(self.paths.latest_candidates_path, all_candidates)
        retry_candidates = [
            candidate
            for candidate in all_candidates
            if candidate.candidate_status
            in {
                CandidateStatus.QUEUED,
                CandidateStatus.SCRAPE_PENDING,
                CandidateStatus.SCRAPE_FAILED,
                CandidateStatus.PARTIALLY_SCRAPED,
                CandidateStatus.UNSUPPORTED_SOURCE,
            }
        ]
        write_candidate_jsonl(self.paths.retry_queue_path, retry_candidates)
        backfill_candidates = [
            candidate for candidate in all_candidates if candidate.backfill_batch_id is not None
        ]
        write_candidate_jsonl(self.paths.backfill_queue_path, backfill_candidates)

    def upsert_backfill_batches(self, batches: Sequence[BackfillBatch]) -> None:
        if not batches:
            return
        existing = {batch.batch_id: batch for batch in self.list_backfill_batches()}
        for batch in batches:
            existing[batch.batch_id] = batch
            write_json_snapshot(
                self.paths.backfill_batches_dir / f"{batch.batch_id}.json",
                batch.model_dump(mode="json"),
            )
        ordered = sorted(
            existing.values(),
            key=lambda batch: (batch.requested_date_from, batch.created_at, batch.batch_id),
        )
        write_model_jsonl(self.paths.latest_backfill_batches_path, ordered)

    def get_backfill_batch(self, batch_id: str) -> BackfillBatch | None:
        for batch in self.list_backfill_batches():
            if batch.batch_id == batch_id:
                return batch
        return None

    def list_backfill_batches(
        self,
        *,
        statuses: Sequence[BackfillBatchStatus] | None = None,
        limit: int | None = None,
    ) -> list[BackfillBatch]:
        batches = self._read_jsonl(self.paths.latest_backfill_batches_path, BackfillBatch)
        if statuses is not None:
            allowed = set(statuses)
            batches = [batch for batch in batches if batch.status in allowed]
        if limit is not None:
            return batches[:limit]
        return batches

    def get_candidate(self, candidate_id: str) -> PersistentCandidate | None:
        self._ensure_index()
        return self._by_id.get(candidate_id)

    def list_candidates(
        self,
        *,
        statuses: Sequence[CandidateStatus] | None = None,
        backfill_batch_id: str | None = None,
        limit: int | None = None,
    ) -> list[PersistentCandidate]:
        self._ensure_index()
        candidates: list[PersistentCandidate] = list(self._by_id.values())
        if statuses is not None:
            allowed = set(statuses)
            candidates = [c for c in candidates if c.candidate_status in allowed]
        if backfill_batch_id is not None:
            candidates = [c for c in candidates if c.backfill_batch_id == backfill_batch_id]
        candidates.sort(
            key=lambda c: (c.last_seen_at, c.first_seen_at, c.candidate_id),
            reverse=True,
        )
        if limit is not None:
            return candidates[:limit]
        return candidates

    def count_candidates(
        self,
        *,
        statuses: Sequence[CandidateStatus] | None = None,
        backfill_batch_id: str | None = None,
    ) -> int:
        self._ensure_index()
        allowed = set(statuses) if statuses is not None else None
        count = 0
        for candidate in self._by_id.values():
            if backfill_batch_id is not None and candidate.backfill_batch_id != backfill_batch_id:
                continue
            if allowed is not None and candidate.candidate_status not in allowed:
                continue
            count += 1
        return count

    def count_backfill_batch_candidates(
        self,
        *,
        batch_id: str,
        scrapeable_statuses: Sequence[CandidateStatus],
    ) -> BackfillCandidateCounts:
        self._ensure_index()
        scrapeable_set = set(scrapeable_statuses)
        merged_candidate_count = 0
        queued_for_scrape_count = 0
        for candidate in self._by_id.values():
            if candidate.backfill_batch_id != batch_id:
                continue
            merged_candidate_count += 1
            if candidate.candidate_status in scrapeable_set:
                queued_for_scrape_count += 1
        return BackfillCandidateCounts(
            merged_candidate_count=merged_candidate_count,
            queued_for_scrape_count=queued_for_scrape_count,
        )

    def find_candidate_by_urls(
        self,
        *,
        canonical_url: str | None,
        current_url: str,
    ) -> PersistentCandidate | None:
        self._ensure_index()
        if canonical_url is not None:
            cid = self._by_url.get(canonical_url)
            if cid is not None:
                return self._by_id.get(cid)
        cid = self._by_url.get(current_url)
        if cid is not None:
            return self._by_id.get(cid)
        return None

    def append_provenance(self, events: Sequence[CandidateProvenance]) -> None:
        self._append_jsonl(self.provenance_path, events)

    def list_provenance(
        self,
        candidate_id: str,
        *,
        limit: int | None = None,
    ) -> list[CandidateProvenance]:
        events = [
            event
            for event in self._read_jsonl(self.provenance_path, CandidateProvenance)
            if event.candidate_id == candidate_id
        ]
        if limit is not None:
            return events[:limit]
        return events

    def append_attempts(self, attempts: Sequence[ScrapeAttempt]) -> None:
        self._append_jsonl(self.attempts_path, attempts)

    def list_attempts(
        self,
        candidate_id: str,
        *,
        limit: int | None = None,
    ) -> list[ScrapeAttempt]:
        attempts = [
            attempt
            for attempt in self._read_jsonl(self.attempts_path, ScrapeAttempt)
            if attempt.candidate_id == candidate_id
        ]
        if limit is not None:
            return attempts[:limit]
        return attempts

    def load_executed_backfill_query_keys(self) -> frozenset[tuple[str, ...]]:
        records = self._read_jsonl(self.paths.backfill_executed_queries_path, ExecutedBackfillQuery)
        return frozenset(_executed_query_key(r) for r in records)

    def append_executed_backfill_queries(self, queries: Sequence[ExecutedBackfillQuery]) -> None:
        self._append_jsonl(self.paths.backfill_executed_queries_path, queries)

    def close(self) -> None:
        return None

    def _append_jsonl(self, path: Path, rows: Sequence[Any]) -> None:
        if not rows:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(row.model_dump_json())
                handle.write("\n")

    def _read_jsonl(self, path: Path, model: type[Any]) -> list[Any]:
        if not path.exists():
            return []
        rows: list[Any] = []
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rows.append(model.model_validate_json(line))
        return rows


class SupabaseDiscoveryPersistence(DiscoveryPersistence):
    """Supabase-backed discovery persistence via PostgREST."""

    def __init__(
        self,
        *,
        base_url: str,
        service_role_key: str,
        schema: str,
        table_names: Mapping[str, str],
        client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._service_role_key = service_role_key
        self._schema = schema
        self._table_names = dict(table_names)
        self._client = client or httpx.Client(timeout=30.0)

    def close(self) -> None:
        self._client.close()

    def write_run(self, run: DiscoveryRun) -> None:
        self._request(
            "POST",
            self._table_names["discovery_runs"],
            params={"on_conflict": "run_id"},
            json=_remove_postgres_unsupported_nuls(
                {
                    **run.model_dump(mode="json"),
                    "errors": run.errors,
                }
            ),
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def upsert_candidates(self, candidates: Sequence[PersistentCandidate]) -> None:
        if not candidates:
            return
        payload = [
            _remove_postgres_unsupported_nuls(candidate.model_dump(mode="json"))
            for candidate in candidates
        ]
        self._request(
            "POST",
            self._table_names["persistent_candidates"],
            params={"on_conflict": "candidate_id"},
            json=payload,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def upsert_backfill_batches(self, batches: Sequence[BackfillBatch]) -> None:
        if not batches:
            return
        payload = [
            _remove_postgres_unsupported_nuls(batch.model_dump(mode="json")) for batch in batches
        ]
        self._request(
            "POST",
            self._table_names["backfill_batches"],
            params={"on_conflict": "batch_id"},
            json=payload,
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def get_backfill_batch(self, batch_id: str) -> BackfillBatch | None:
        response = self._request(
            "GET",
            self._table_names["backfill_batches"],
            params={"select": "*", "batch_id": f"eq.{batch_id}", "limit": "1"},
        )
        payload = response.json()
        if isinstance(payload, list) and payload:
            return BackfillBatch.model_validate(payload[0])
        return None

    def list_backfill_batches(
        self,
        *,
        statuses: Sequence[BackfillBatchStatus] | None = None,
        limit: int | None = None,
    ) -> list[BackfillBatch]:
        params: dict[str, str] = {"select": "*", "order": "requested_date_from.asc"}
        if statuses:
            joined = ",".join(status.value for status in statuses)
            params["status"] = f"in.({joined})"
        if limit is not None:
            params["limit"] = str(limit)
        response = self._request("GET", self._table_names["backfill_batches"], params=params)
        payload = response.json()
        if isinstance(payload, list):
            return [BackfillBatch.model_validate(item) for item in payload]
        return []

    def get_candidate(self, candidate_id: str) -> PersistentCandidate | None:
        response = self._request(
            "GET",
            self._table_names["persistent_candidates"],
            params={"select": "*", "candidate_id": f"eq.{candidate_id}", "limit": "1"},
        )
        payload = response.json()
        if isinstance(payload, list) and payload:
            return PersistentCandidate.model_validate(payload[0])
        return None

    def list_candidates(
        self,
        *,
        statuses: Sequence[CandidateStatus] | None = None,
        backfill_batch_id: str | None = None,
        limit: int | None = None,
    ) -> list[PersistentCandidate]:
        params: dict[str, str] = {"select": "*", "order": "last_seen_at.desc"}
        if statuses:
            joined = ",".join(status.value for status in statuses)
            params["candidate_status"] = f"in.({joined})"
        if backfill_batch_id is not None:
            params["backfill_batch_id"] = f"eq.{backfill_batch_id}"
        if limit is not None:
            params["limit"] = str(limit)
        response = self._request("GET", self._table_names["persistent_candidates"], params=params)
        payload = response.json()
        if isinstance(payload, list):
            return [PersistentCandidate.model_validate(item) for item in payload]
        return []

    def count_candidates(
        self,
        *,
        statuses: Sequence[CandidateStatus] | None = None,
        backfill_batch_id: str | None = None,
    ) -> int:
        params: dict[str, str] = {"select": "candidate_id", "limit": "1"}
        if statuses:
            joined = ",".join(status.value for status in statuses)
            params["candidate_status"] = f"in.({joined})"
        if backfill_batch_id is not None:
            params["backfill_batch_id"] = f"eq.{backfill_batch_id}"
        response = self._request(
            "GET",
            self._table_names["persistent_candidates"],
            params=params,
            extra_headers={"Prefer": "count=exact"},
        )
        content_range = response.headers.get("content-range")
        if content_range is not None and "/" in content_range:
            return int(content_range.rsplit("/", 1)[1])
        payload = response.json()
        return len(payload) if isinstance(payload, list) else 0

    def count_backfill_batch_candidates(
        self,
        *,
        batch_id: str,
        scrapeable_statuses: Sequence[CandidateStatus],
    ) -> BackfillCandidateCounts:
        return BackfillCandidateCounts(
            merged_candidate_count=self.count_candidates(backfill_batch_id=batch_id),
            queued_for_scrape_count=self.count_candidates(
                statuses=scrapeable_statuses,
                backfill_batch_id=batch_id,
            ),
        )

    def find_candidate_by_urls(
        self,
        *,
        canonical_url: str | None,
        current_url: str,
    ) -> PersistentCandidate | None:
        if canonical_url:
            response = self._request(
                "GET",
                self._table_names["persistent_candidates"],
                params={"select": "*", "canonical_url": f"eq.{canonical_url}", "limit": "1"},
            )
            payload = response.json()
            if isinstance(payload, list) and payload:
                return PersistentCandidate.model_validate(payload[0])

        response = self._request(
            "GET",
            self._table_names["persistent_candidates"],
            params={"select": "*", "current_url": f"eq.{current_url}", "limit": "1"},
        )
        payload = response.json()
        if isinstance(payload, list) and payload:
            return PersistentCandidate.model_validate(payload[0])
        return None

    def append_provenance(self, events: Sequence[CandidateProvenance]) -> None:
        if not events:
            return
        self._request(
            "POST",
            self._table_names["candidate_provenance"],
            json=[
                _remove_postgres_unsupported_nuls(event.model_dump(mode="json")) for event in events
            ],
            extra_headers={"Prefer": "return=minimal"},
        )

    def list_provenance(
        self,
        candidate_id: str,
        *,
        limit: int | None = None,
    ) -> list[CandidateProvenance]:
        params: dict[str, str] = {
            "select": "*",
            "candidate_id": f"eq.{candidate_id}",
            "order": "discovered_at.desc",
        }
        if limit is not None:
            params["limit"] = str(limit)
        response = self._request("GET", self._table_names["candidate_provenance"], params=params)
        payload = response.json()
        if isinstance(payload, list):
            return [CandidateProvenance.model_validate(item) for item in payload]
        return []

    def append_attempts(self, attempts: Sequence[ScrapeAttempt]) -> None:
        if not attempts:
            return
        self._request(
            "POST",
            self._table_names["scrape_attempts"],
            json=[
                _remove_postgres_unsupported_nuls(attempt.model_dump(mode="json"))
                for attempt in attempts
            ],
            extra_headers={"Prefer": "return=minimal"},
        )

    def list_attempts(
        self,
        candidate_id: str,
        *,
        limit: int | None = None,
    ) -> list[ScrapeAttempt]:
        params: dict[str, str] = {
            "select": "*",
            "candidate_id": f"eq.{candidate_id}",
            "order": "started_at.desc",
        }
        if limit is not None:
            params["limit"] = str(limit)
        response = self._request("GET", self._table_names["scrape_attempts"], params=params)
        payload = response.json()
        if isinstance(payload, list):
            return [ScrapeAttempt.model_validate(item) for item in payload]
        return []

    def load_executed_backfill_query_keys(self) -> frozenset[tuple[str, ...]]:
        return frozenset()

    def append_executed_backfill_queries(self, queries: Sequence[ExecutedBackfillQuery]) -> None:
        del queries

    def _request(
        self,
        method: str,
        table: str,
        *,
        params: Mapping[str, str] | None = None,
        json: Any | None = None,
        extra_headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        headers = {
            "apikey": self._service_role_key,
            "Authorization": f"Bearer {self._service_role_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Accept-Profile": self._schema,
            "Content-Profile": self._schema,
        }
        if extra_headers:
            headers.update(extra_headers)
        response = self._client.request(
            method,
            f"{self._base_url}/rest/v1/{table}",
            params=params,
            json=json,
            headers=headers,
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            body = response.text[:1000]
            raise httpx.HTTPStatusError(
                f"{exc}; response_body={body}",
                request=exc.request,
                response=exc.response,
            ) from exc
        return response


class CompositeDiscoveryPersistence(DiscoveryPersistence):
    """Fan out writes to multiple stores while reading from the primary."""

    def __init__(
        self,
        primary: DiscoveryPersistence,
        mirrors: Sequence[DiscoveryPersistence],
        count_source: DiscoveryPersistence | None = None,
    ) -> None:
        self.primary = primary
        self.mirrors = list(mirrors)
        self.count_source = count_source or primary

    def close(self) -> None:
        self.primary.close()
        for mirror in self.mirrors:
            mirror.close()

    def write_run(self, run: DiscoveryRun) -> None:
        self.primary.write_run(run)
        for mirror in self.mirrors:
            mirror.write_run(run)

    def upsert_candidates(self, candidates: Sequence[PersistentCandidate]) -> None:
        self.primary.upsert_candidates(candidates)
        for mirror in self.mirrors:
            mirror.upsert_candidates(candidates)

    def upsert_backfill_batches(self, batches: Sequence[BackfillBatch]) -> None:
        self.primary.upsert_backfill_batches(batches)
        for mirror in self.mirrors:
            mirror.upsert_backfill_batches(batches)

    def get_candidate(self, candidate_id: str) -> PersistentCandidate | None:
        return self.primary.get_candidate(candidate_id)

    def get_backfill_batch(self, batch_id: str) -> BackfillBatch | None:
        return self.primary.get_backfill_batch(batch_id)

    def list_candidates(
        self,
        *,
        statuses: Sequence[CandidateStatus] | None = None,
        backfill_batch_id: str | None = None,
        limit: int | None = None,
    ) -> list[PersistentCandidate]:
        return self.primary.list_candidates(
            statuses=statuses,
            backfill_batch_id=backfill_batch_id,
            limit=limit,
        )

    def count_candidates(
        self,
        *,
        statuses: Sequence[CandidateStatus] | None = None,
        backfill_batch_id: str | None = None,
    ) -> int:
        return self.count_source.count_candidates(
            statuses=statuses,
            backfill_batch_id=backfill_batch_id,
        )

    def count_backfill_batch_candidates(
        self,
        *,
        batch_id: str,
        scrapeable_statuses: Sequence[CandidateStatus],
    ) -> BackfillCandidateCounts:
        return self.count_source.count_backfill_batch_candidates(
            batch_id=batch_id,
            scrapeable_statuses=scrapeable_statuses,
        )

    def list_backfill_batches(
        self,
        *,
        statuses: Sequence[BackfillBatchStatus] | None = None,
        limit: int | None = None,
    ) -> list[BackfillBatch]:
        return self.primary.list_backfill_batches(statuses=statuses, limit=limit)

    def find_candidate_by_urls(
        self,
        *,
        canonical_url: str | None,
        current_url: str,
    ) -> PersistentCandidate | None:
        return self.primary.find_candidate_by_urls(
            canonical_url=canonical_url,
            current_url=current_url,
        )

    def append_provenance(self, events: Sequence[CandidateProvenance]) -> None:
        self.primary.append_provenance(events)
        for mirror in self.mirrors:
            mirror.append_provenance(events)

    def list_provenance(
        self,
        candidate_id: str,
        *,
        limit: int | None = None,
    ) -> list[CandidateProvenance]:
        return self.primary.list_provenance(candidate_id, limit=limit)

    def append_attempts(self, attempts: Sequence[ScrapeAttempt]) -> None:
        self.primary.append_attempts(attempts)
        for mirror in self.mirrors:
            mirror.append_attempts(attempts)

    def list_attempts(
        self,
        candidate_id: str,
        *,
        limit: int | None = None,
    ) -> list[ScrapeAttempt]:
        return self.primary.list_attempts(candidate_id, limit=limit)

    def load_executed_backfill_query_keys(self) -> frozenset[tuple[str, ...]]:
        return self.primary.load_executed_backfill_query_keys()

    def append_executed_backfill_queries(self, queries: Sequence[ExecutedBackfillQuery]) -> None:
        self.primary.append_executed_backfill_queries(queries)
        for mirror in self.mirrors:
            mirror.append_executed_backfill_queries(queries)


def create_discovery_persistence(config: Config) -> DiscoveryPersistence:
    """Create the configured discovery persistence backend."""
    state_store = StateRepoDiscoveryPersistence(config.discovery_state_paths)
    if (
        config.operational.provider is OperationalProvider.SUPABASE
        and config.supabase_url
        and config.supabase_service_role_key
    ):
        supabase_store = SupabaseDiscoveryPersistence(
            base_url=config.supabase_url,
            service_role_key=config.supabase_service_role_key,
            schema=config.operational.supabase_schema,
            table_names={
                "discovery_runs": config.candidates.discovery_runs_table,
                "persistent_candidates": config.candidates.supabase_table,
                "backfill_batches": config.candidates.backfill_batches_table,
                "candidate_provenance": config.candidates.provenance_table,
                "scrape_attempts": config.candidates.scrape_attempts_table,
            },
        )
        return CompositeDiscoveryPersistence(
            state_store,
            [supabase_store],
            count_source=supabase_store,
        )
    return state_store
