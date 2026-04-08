"""Release builder and publication helpers for the news_items dataset."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from denbust.config import Config
from denbust.models.common import DatasetName
from denbust.news_items.models import NewsItemOperationalRecord, NewsItemPublicRecord
from denbust.news_items.policy import is_publicly_releasable
from denbust.publish.release import (
    ReleaseArtifact,
    ReleaseBuilder,
    ReleaseFormat,
    ReleaseManifest,
)

logger = logging.getLogger(__name__)


def release_version_for_datetime(moment: datetime | None = None) -> str:
    """Return a simple UTC date-based release version."""
    effective = moment or datetime.now(UTC)
    return effective.astimezone(UTC).strftime("%Y-%m-%d")


def parse_operational_records(rows: Sequence[dict[str, Any]]) -> list[NewsItemOperationalRecord]:
    """Validate operational rows loaded from the persistence layer."""
    records: list[NewsItemOperationalRecord] = []
    for row in rows:
        try:
            records.append(NewsItemOperationalRecord.model_validate(row))
        except Exception as exc:
            logger.warning("Skipping invalid news_items row %s: %s", row, exc)
    return records


def select_releasable_records(
    rows: Sequence[dict[str, Any]],
    *,
    release_version: str,
) -> list[NewsItemPublicRecord]:
    """Project operational rows into public metadata rows with release gating."""
    records = parse_operational_records(rows)
    public_rows: list[NewsItemPublicRecord] = []
    for record in records:
        if not is_publicly_releasable(record):
            continue
        public_rows.append(record.to_public_record(release_version=release_version))
    public_rows.sort(key=lambda row: row.publication_datetime, reverse=True)
    return public_rows


def _artifact_for_path(path: Path, *, fmt: ReleaseFormat, row_count: int = 0) -> ReleaseArtifact:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return ReleaseArtifact(path=path, format=fmt, row_count=row_count, sha256=digest)


def _schema_json() -> dict[str, Any]:
    return NewsItemPublicRecord.model_json_schema()


def _serialized_row(row: NewsItemPublicRecord) -> dict[str, Any]:
    payload = row.model_dump(mode="json")
    for key, value in list(payload.items()):
        if isinstance(value, list):
            payload[key] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, bool):
            payload[key] = str(value)
        elif value is None:
            payload[key] = ""
        elif not isinstance(value, str):
            payload[key] = str(value)
    return payload


def _schema_markdown() -> str:
    schema = _schema_json()
    properties = schema.get("properties", {})
    lines = ["# news_items schema", "", "| field | type | required |", "|---|---|---|"]
    required = set(schema.get("required", []))
    for field_name, payload in properties.items():
        field_type = payload.get("type") or payload.get("anyOf") or payload.get("$ref", "object")
        if isinstance(field_type, list):
            rendered_type = ", ".join(str(part) for part in field_type)
        elif isinstance(field_type, dict):
            rendered_type = json.dumps(field_type, ensure_ascii=False)
        else:
            rendered_type = str(field_type)
        lines.append(
            f"| {field_name} | {rendered_type} | {'yes' if field_name in required else 'no'} |"
        )
    lines.append("")
    lines.append("Public exports are metadata-only and intentionally exclude article full text.")
    return "\n".join(lines) + "\n"


def _public_dataset_readme(manifest: ReleaseManifest) -> str:
    files = "\n".join(f"- `{artifact.path.name}`" for artifact in manifest.primary_files)
    return (
        f"# {manifest.dataset_name}\n\n"
        "This release contains metadata-only rows about Israeli news items related to trafficking, "
        "prostitution, brothels, pimping, enforcement, and related legal developments.\n\n"
        "## Included files\n\n"
        f"{files}\n\n"
        "## Public-data constraints\n\n"
        "- no article full text\n"
        "- no cached HTML or page snapshots\n"
        "- rows that are suppressed or fail privacy/review gating are excluded\n"
        "- rights class for public rows is `metadata_only`\n"
    )


class NewsItemsReleaseBuilder(ReleaseBuilder):
    """Concrete release builder for the news_items dataset."""

    def __init__(self, *, config: Config) -> None:
        self._config = config

    def build_manifest(self, dataset_name: str, publication_dir: Path) -> ReleaseManifest:
        release_version = release_version_for_datetime()
        return ReleaseManifest(
            dataset_name=dataset_name,
            release_version=release_version,
            schema_version=self._config.release.schema_version,
            rights_policy_version=self._config.release.rights_policy_version,
            privacy_policy_version=self._config.release.privacy_policy_version,
            primary_files=[
                ReleaseArtifact(
                    path=publication_dir / release_version / "news_items.parquet",
                    format=ReleaseFormat.PARQUET,
                )
            ],
        )

    def build_release_bundle(
        self,
        *,
        publication_dir: Path,
        rows: Sequence[dict[str, Any]],
    ) -> ReleaseManifest:
        """Write the release bundle and return its manifest."""
        release_version = release_version_for_datetime()
        output_dir = publication_dir / release_version
        output_dir.mkdir(parents=True, exist_ok=True)

        public_rows = select_releasable_records(rows, release_version=release_version)
        manifest = ReleaseManifest(
            dataset_name=DatasetName.NEWS_ITEMS.value,
            release_version=release_version,
            schema_version=self._config.release.schema_version,
            row_count=len(public_rows),
            rights_policy_version=self._config.release.rights_policy_version,
            privacy_policy_version=self._config.release.privacy_policy_version,
        )
        if public_rows:
            newest = max(row.publication_datetime for row in public_rows).astimezone(UTC)
            oldest = min(row.publication_datetime for row in public_rows).astimezone(UTC)
            manifest.source_coverage_window = (
                f"{oldest.date().isoformat()}..{newest.date().isoformat()}"
            )
        else:
            manifest.warnings.append("Release contains zero publicly releasable rows.")

        artifacts: list[ReleaseArtifact] = []
        parquet_path = output_dir / "news_items.parquet"
        self._write_parquet(public_rows, parquet_path)
        artifacts.append(
            _artifact_for_path(parquet_path, fmt=ReleaseFormat.PARQUET, row_count=len(public_rows))
        )

        if self._config.release.include_csv:
            csv_path = output_dir / "news_items.csv"
            self._write_csv(public_rows, csv_path)
            artifacts.append(
                _artifact_for_path(csv_path, fmt=ReleaseFormat.CSV, row_count=len(public_rows))
            )

        schema_json_path = output_dir / "SCHEMA.json"
        schema_json_path.write_text(
            json.dumps(_schema_json(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        artifacts.append(_artifact_for_path(schema_json_path, fmt=ReleaseFormat.JSON))

        schema_md_path = output_dir / "SCHEMA.md"
        schema_md_path.write_text(_schema_markdown(), encoding="utf-8")
        artifacts.append(_artifact_for_path(schema_md_path, fmt=ReleaseFormat.MARKDOWN))

        manifest.primary_files = artifacts.copy()

        readme_path = output_dir / "README.md"
        readme_path.write_text(_public_dataset_readme(manifest), encoding="utf-8")
        artifacts.append(_artifact_for_path(readme_path, fmt=ReleaseFormat.MARKDOWN))

        manifest.primary_files = artifacts.copy()
        manifest_path = output_dir / "MANIFEST.json"
        manifest_path.write_text(
            json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        manifest_artifact = _artifact_for_path(manifest_path, fmt=ReleaseFormat.JSON)
        artifacts_with_manifest = artifacts + [manifest_artifact]

        checksums_path = output_dir / "checksums.txt"
        checksums_path.write_text(
            "".join(
                f"{artifact.sha256}  {artifact.path.name}\n"
                for artifact in artifacts_with_manifest
                if artifact.sha256
            ),
            encoding="utf-8",
        )
        return manifest

    def describe(self) -> str:
        return "Builds a metadata-only public release bundle for news_items."

    def _write_csv(self, rows: Sequence[NewsItemPublicRecord], path: Path) -> None:
        field_names = list(NewsItemPublicRecord.model_fields)
        with open(path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=field_names)
            writer.writeheader()
            for row in rows:
                writer.writerow(_serialized_row(row))

    def _write_parquet(self, rows: Sequence[NewsItemPublicRecord], path: Path) -> None:
        try:
            import pyarrow as pa  # type: ignore[import-untyped]
            import pyarrow.parquet as pq  # type: ignore[import-untyped]
        except ImportError as exc:
            raise RuntimeError(
                "pyarrow is required to build the news_items Parquet release."
            ) from exc

        schema = pa.schema(
            [
                (
                    field_name,
                    pa.string(),
                )
                for field_name in NewsItemPublicRecord.model_fields
            ]
        )
        payload = [_serialized_row(row) for row in rows]
        table = pa.Table.from_pylist(payload, schema=schema)
        pq.write_table(table, path)
