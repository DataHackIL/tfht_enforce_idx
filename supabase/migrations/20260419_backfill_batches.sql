create table if not exists public.backfill_batches (
    batch_id text primary key,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    started_at timestamptz,
    finished_at timestamptz,
    dataset_name text not null,
    job_name text not null,
    status text not null,
    requested_date_from timestamptz not null,
    requested_date_to timestamptz not null,
    window_count integer not null default 0,
    query_count integer not null default 0,
    candidate_count integer not null default 0,
    merged_candidate_count integer not null default 0,
    queued_for_scrape_count integer not null default 0,
    scrape_attempt_count integer not null default 0,
    scraped_candidate_count integer not null default 0,
    warnings jsonb not null default '[]'::jsonb,
    errors jsonb not null default '[]'::jsonb,
    metadata jsonb not null default '{}'::jsonb
);

create index if not exists backfill_batches_status_idx
    on public.backfill_batches (status, requested_date_from asc);

create index if not exists persistent_candidates_backfill_batch_idx
    on public.persistent_candidates (backfill_batch_id, candidate_status, next_scrape_attempt_at)
    where backfill_batch_id is not null;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'persistent_candidates_backfill_batch_fk'
    ) then
        alter table public.persistent_candidates
            add constraint persistent_candidates_backfill_batch_fk
            foreign key (backfill_batch_id)
            references public.backfill_batches (batch_id)
            on delete set null;
    end if;
end $$;
