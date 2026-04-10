"""Unit tests for the discovery-layer Supabase migration scaffold."""

from pathlib import Path

MIGRATION_PATH = Path("supabase/migrations/20260410_discovery_candidacy_foundation.sql")


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
    assert "metadata_json jsonb not null default '{}'::jsonb" in sql
    assert "errors_json jsonb not null default '[]'::jsonb" in sql
    assert "diagnostics_json jsonb not null default '{}'::jsonb" in sql
