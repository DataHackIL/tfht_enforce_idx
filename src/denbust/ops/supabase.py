"""Supabase-backed operational store for the news_items dataset."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import httpx

from denbust.config import OperationalConfig
from denbust.models.runs import RunSnapshot
from denbust.ops.storage import OperationalStore


class SupabaseOperationalStore(OperationalStore):
    """Operational store backed by Supabase PostgREST endpoints."""

    def __init__(
        self,
        *,
        base_url: str,
        service_role_key: str,
        config: OperationalConfig,
        client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._service_role_key = service_role_key
        self._config = config
        self._client = client or httpx.Client(timeout=30.0)

    def close(self) -> None:
        self._client.close()

    def write_run_metadata(self, snapshot: RunSnapshot) -> None:
        table = self._table_for_job(snapshot.job_name.value)
        payload = snapshot.model_dump(mode="json")
        payload.pop("items", None)
        self._request(
            "POST",
            f"/rest/v1/{table}",
            json=payload,
            extra_headers={"Prefer": "return=minimal"},
        )

    def upsert_records(self, dataset_name: str, records: Sequence[Mapping[str, Any]]) -> None:
        del dataset_name
        if not records:
            return
        self._request(
            "POST",
            f"/rest/v1/{self._config.news_items_table}",
            params={"on_conflict": "canonical_url"},
            json=[dict(record) for record in records],
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def fetch_records(self, dataset_name: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        del dataset_name
        params: dict[str, str] = {
            "select": "*",
            "order": "publication_datetime.desc",
        }
        if limit is not None:
            params["limit"] = str(limit)
        response = self._request(
            "GET",
            f"/rest/v1/{self._config.news_items_table}",
            params=params,
        )
        payload = response.json()
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, dict)]
        return []

    def fetch_suppression_rules(self, dataset_name: str) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            f"/rest/v1/{self._config.suppression_rules_table}",
            params={
                "select": "*",
                "dataset_name": f"eq.{dataset_name}",
                "active": "eq.true",
            },
        )
        payload = response.json()
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, dict)]
        return []

    def fetch_news_item_corrections(self, dataset_name: str) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            f"/rest/v1/{self._config.news_items_corrections_table}",
            params={
                "select": "*",
                "dataset_name": f"eq.{dataset_name}",
                "order": "reviewed_at.desc.nullslast",
            },
        )
        payload = response.json()
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, dict)]
        return []

    def fetch_missing_news_items(self, dataset_name: str) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            f"/rest/v1/{self._config.news_items_missing_items_table}",
            params={
                "select": "*",
                "dataset_name": f"eq.{dataset_name}",
                "order": "event_date.desc",
            },
        )
        payload = response.json()
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, dict)]
        return []

    def upsert_news_item_corrections(
        self,
        dataset_name: str,
        records: Sequence[Mapping[str, Any]],
    ) -> None:
        if not records:
            return
        self._request(
            "POST",
            f"/rest/v1/{self._config.news_items_corrections_table}",
            params={"on_conflict": "dataset_name,record_id,canonical_url"},
            json=[{"dataset_name": dataset_name, **dict(record)} for record in records],
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def upsert_missing_news_items(
        self,
        dataset_name: str,
        records: Sequence[Mapping[str, Any]],
    ) -> None:
        if not records:
            return
        self._request(
            "POST",
            f"/rest/v1/{self._config.news_items_missing_items_table}",
            params={"on_conflict": "dataset_name,annotation_id"},
            json=[{"dataset_name": dataset_name, **dict(record)} for record in records],
            extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def mark_publication_state(
        self,
        dataset_name: str,
        record_ids: Sequence[str],
        publication_status: str,
    ) -> None:
        del dataset_name
        if not record_ids:
            return
        joined = ",".join(record_ids)
        self._request(
            "PATCH",
            f"/rest/v1/{self._config.news_items_table}",
            params={"id": f"in.({joined})"},
            json={
                "publication_status": publication_status,
                "updated_at": datetime.now(UTC).isoformat(),
            },
            extra_headers={"Prefer": "return=minimal"},
        )

    def _table_for_job(self, job_name: str) -> str:
        if job_name == "ingest":
            return self._config.ingestion_runs_table
        if job_name == "release":
            return self._config.release_runs_table
        return self._config.backup_runs_table

    def _request(
        self,
        method: str,
        path: str,
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
            "Accept-Profile": self._config.supabase_schema,
            "Content-Profile": self._config.supabase_schema,
        }
        if extra_headers:
            headers.update(extra_headers)
        response = self._client.request(
            method,
            f"{self._base_url}{path}",
            params=params,
            json=json,
            headers=headers,
        )
        response.raise_for_status()
        return response
