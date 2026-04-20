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
    PersistentCandidate,
    ScrapeAttempt,
)
from denbust.discovery.persistence import (
    BackfillBatchStore,
    CandidateStore,
    DiscoveryRunStore,
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


class DiscoveryPersistence(
    DiscoveryRunStore,
    CandidateStore,
    BackfillBatchStore,
    ProvenanceStore,
    ScrapeAttemptStore,
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

    def close(self) -> None:
        return None


class StateRepoDiscoveryPersistence(DiscoveryPersistence):
    """State-repo-backed discovery persistence using JSONL files."""

    def __init__(self, paths: DiscoveryStatePaths) -> None:
        self.paths = paths

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
        existing = {candidate.candidate_id: candidate for candidate in self.list_candidates()}
        for candidate in candidates:
            existing[candidate.candidate_id] = candidate

        all_candidates = sorted(
            existing.values(),
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
        for candidate in self.list_candidates():
            if candidate.candidate_id == candidate_id:
                return candidate
        return None

    def list_candidates(
        self,
        *,
        statuses: Sequence[CandidateStatus] | None = None,
        backfill_batch_id: str | None = None,
        limit: int | None = None,
    ) -> list[PersistentCandidate]:
        candidates = self._read_jsonl(self.paths.latest_candidates_path, PersistentCandidate)
        if statuses is not None:
            allowed = set(statuses)
            candidates = [
                candidate for candidate in candidates if candidate.candidate_status in allowed
            ]
        if backfill_batch_id is not None:
            candidates = [
                candidate
                for candidate in candidates
                if candidate.backfill_batch_id == backfill_batch_id
            ]
        if limit is not None:
            return candidates[:limit]
        return candidates

    def find_candidate_by_urls(
        self,
        *,
        canonical_url: str | None,
        current_url: str,
    ) -> PersistentCandidate | None:
        for candidate in self.list_candidates():
            if canonical_url is not None and str(candidate.canonical_url or "") == canonical_url:
                return candidate
            if str(candidate.current_url) == current_url:
                return candidate
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
            json={
                **run.model_dump(mode="json"),
                "errors": run.errors,
            },
            extra_headers={"Prefer": "return=minimal"},
        )

    def upsert_candidates(self, candidates: Sequence[PersistentCandidate]) -> None:
        if not candidates:
            return
        payload = [candidate.model_dump(mode="json") for candidate in candidates]
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
        payload = [batch.model_dump(mode="json") for batch in batches]
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
            json=[event.model_dump(mode="json") for event in events],
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
            json=[attempt.model_dump(mode="json") for attempt in attempts],
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
        response.raise_for_status()
        return response


class CompositeDiscoveryPersistence(DiscoveryPersistence):
    """Fan out writes to multiple stores while reading from the primary."""

    def __init__(
        self,
        primary: DiscoveryPersistence,
        mirrors: Sequence[DiscoveryPersistence],
    ) -> None:
        self.primary = primary
        self.mirrors = list(mirrors)

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
        return CompositeDiscoveryPersistence(state_store, [supabase_store])
    return state_store
