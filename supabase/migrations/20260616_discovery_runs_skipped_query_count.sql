-- Add the discovery_runs.skipped_query_count column.
--
-- DiscoveryRun gained a `skipped_query_count` field (the rolling-window search
-- backstop counts queries it deliberately skips), and the Supabase-backed
-- DiscoveryStore.write_run() posts the full model_dump() to discovery_runs via
-- PostgREST. The column was never added to the table, so every run-metadata
-- write failed with "column skipped_query_count does not exist", which blocked
-- candidate persistence on the daily-review runs.

alter table public.discovery_runs
    add column if not exists skipped_query_count integer not null default 0;

-- Deploy note: the runtime error is PGRST204 ("not found in the schema cache"),
-- so this must be applied to the live project (supabase db push) AND PostgREST
-- must reload its schema cache before the write succeeds. PostgREST reloads
-- automatically on most managed deploys; if the 400 persists, force it with:
--   notify pgrst, 'reload schema';
