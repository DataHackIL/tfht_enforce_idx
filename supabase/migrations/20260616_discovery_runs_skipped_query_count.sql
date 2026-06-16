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
