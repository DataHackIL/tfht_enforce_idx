"""Unit tests for the backfill-batch Supabase migration."""

from pathlib import Path

MIGRATION_PATH = Path("supabase/migrations/20260419_backfill_batches.sql")


def test_backfill_migration_defines_required_table() -> None:
    """The migration should create the durable backfill-batch table."""
    sql = MIGRATION_PATH.read_text(encoding="utf-8")

    assert "create table if not exists public.backfill_batches" in sql
    assert "create index if not exists backfill_batches_status_idx" in sql


def test_backfill_migration_links_candidates_to_batches() -> None:
    """Persistent candidates should gain indexed batch linkage."""
    sql = MIGRATION_PATH.read_text(encoding="utf-8")

    assert "persistent_candidates_backfill_batch_idx" in sql
    assert "persistent_candidates_backfill_batch_fk" in sql
