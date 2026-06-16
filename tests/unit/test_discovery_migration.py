"""Unit tests for the discovery-layer Supabase migration scaffold."""

from pathlib import Path

from denbust.discovery.models import DiscoveryRun

MIGRATION_PATH = Path("supabase/migrations/20260410_discovery_candidacy_foundation.sql")
SKIPPED_QUERY_COUNT_MIGRATION = Path(
    "supabase/migrations/20260616_discovery_runs_skipped_query_count.sql"
)


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


def test_discovery_runs_has_a_column_for_every_model_field() -> None:
    """Every DiscoveryRun scalar field must map to a discovery_runs column across
    the migrations, so write_run()'s model_dump() never posts an unknown column."""
    migrations = "\n".join(
        p.read_text(encoding="utf-8") for p in sorted(Path("supabase/migrations").glob("*.sql"))
    )
    # `errors` is the jsonb column; the rest are scalar columns on discovery_runs.
    for field in DiscoveryRun.model_fields:
        assert field in migrations, f"discovery_runs is missing a column for {field!r}"


def test_skipped_query_count_column_is_added() -> None:
    """Regression for the missing-column persistence failure (issues #150/#152/
    #156/#184/#186): the column DiscoveryRun.skipped_query_count writes must exist."""
    sql = SKIPPED_QUERY_COUNT_MIGRATION.read_text(encoding="utf-8")
    assert "add column if not exists skipped_query_count integer not null default 0" in sql
    assert "alter table public.discovery_runs" in sql
