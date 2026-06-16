"""Unit tests for the discovery-layer Supabase migration scaffold."""

import re
from pathlib import Path

from denbust.discovery.models import (
    BackfillBatch,
    CandidateProvenance,
    DiscoveryRun,
    PersistentCandidate,
    ScrapeAttempt,
)

MIGRATION_PATH = Path("supabase/migrations/20260410_discovery_candidacy_foundation.sql")
SKIPPED_QUERY_COUNT_MIGRATION = Path(
    "supabase/migrations/20260616_discovery_runs_skipped_query_count.sql"
)

_MIGRATIONS_DIR = Path("supabase/migrations")
_ALL_MIGRATIONS = "\n".join(
    p.read_text(encoding="utf-8") for p in sorted(_MIGRATIONS_DIR.glob("*.sql"))
)

# Tokens that begin a table-constraint line rather than a column definition.
_NON_COLUMN_TOKENS = {"primary", "unique", "constraint", "foreign", "check", "references"}

# Each persisted discovery model and the table its writer POSTs a full
# ``model_dump()`` to. The third element lists fields that are intentionally NOT
# stored as a same-named column (none today); adding such a field is then a
# conscious choice rather than a silent production break.
_MODEL_TABLES: list[tuple[type, str, set[str]]] = [
    (DiscoveryRun, "discovery_runs", set()),
    (PersistentCandidate, "persistent_candidates", set()),
    (BackfillBatch, "backfill_batches", set()),
    (ScrapeAttempt, "scrape_attempts", set()),
    (CandidateProvenance, "candidate_provenance", set()),
]


def _table_columns(table: str) -> set[str]:
    """Columns of ``public.<table>``, parsed from its CREATE TABLE block and any
    ALTER ... ADD COLUMN statements across all migrations.

    Scoped to the specific table — a substring search over all migrations is
    unsound, because discovery models share field names (``status``, ``errors``,
    ``dataset_name``, ...) with columns on *other* tables, so a column missing
    from this table could be masked by a same-named column elsewhere.
    """
    cols: set[str] = set()
    create = re.search(
        rf"create table if not exists public\.{table}\s*\((.*?)\);",
        _ALL_MIGRATIONS,
        re.S,
    )
    if create:
        for line in create.group(1).splitlines():
            match = re.match(r"\s*([a-z_]+)\s+\S", line)
            if match and match.group(1) not in _NON_COLUMN_TOKENS:
                cols.add(match.group(1))
    for alter in re.finditer(rf"alter table public\.{table}\b(.*?);", _ALL_MIGRATIONS, re.S):
        for col in re.finditer(r"add column if not exists\s+([a-z_]+)", alter.group(1)):
            cols.add(col.group(1))
    return cols


def test_discovery_migration_defines_required_tables() -> None:
    """The foundation migration should create the durable candidate-layer tables."""
    sql = MIGRATION_PATH.read_text(encoding="utf-8")

    assert "create table if not exists public.discovery_runs" in sql
    assert "create table if not exists public.persistent_candidates" in sql
    assert "create table if not exists public.candidate_provenance" in sql
    assert "create table if not exists public.scrape_attempts" in sql


def test_discovery_migration_includes_core_columns() -> None:
    """The migration should cover the core queueing and retry columns from the design."""
    sql = MIGRATION_PATH.read_text(encoding="utf-8")

    assert "candidate_status text not null" in sql
    assert "content_basis text not null" in sql
    assert "next_scrape_attempt_at timestamptz" in sql
    assert "metadata jsonb not null default '{}'::jsonb" in sql
    assert "errors jsonb not null default '[]'::jsonb" in sql
    assert "diagnostics jsonb not null default '{}'::jsonb" in sql


def test_every_persisted_model_field_maps_to_a_table_column() -> None:
    """Every field of every persisted discovery model must have a matching column
    on its target table, so a writer's ``model_dump()`` POST never references an
    unknown column (the PGRST204 class of failure that issues #150/#152/#156/#184/
    #186 reported for discovery_runs.skipped_query_count).

    This is the real regression guard: it covers ALL five model<->table pairs and
    is scoped per table, so drift in any of them fails CI rather than waiting for
    the daily-review bot to file the next ticket.
    """
    failures: list[str] = []
    for model, table, exceptions in _MODEL_TABLES:
        columns = _table_columns(table)
        # A parsing failure would yield an empty set and (correctly) fail loudly.
        assert columns, f"could not parse any columns for public.{table}"
        missing = set(model.model_fields) - columns - exceptions
        if missing:
            failures.append(f"public.{table} is missing columns for {sorted(missing)}")
    assert not failures, "model<->schema drift detected:\n" + "\n".join(failures)


def test_skipped_query_count_column_is_added() -> None:
    """Regression for the missing-column persistence failure (issues #150/#152/
    #156/#184/#186): the column DiscoveryRun.skipped_query_count writes must exist."""
    sql = SKIPPED_QUERY_COUNT_MIGRATION.read_text(encoding="utf-8")
    assert "add column if not exists skipped_query_count integer not null default 0" in sql
    assert "alter table public.discovery_runs" in sql
