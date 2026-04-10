create table if not exists public.discovery_runs (
    run_id text primary key,
    started_at timestamptz not null,
    finished_at timestamptz,
    dataset_name text not null,
    job_name text not null,
    status text not null,
    query_count integer not null default 0,
    candidate_count integer not null default 0,
    merged_candidate_count integer not null default 0,
    queued_for_scrape_count integer not null default 0,
    errors jsonb not null default '[]'::jsonb
);

create index if not exists discovery_runs_dataset_job_idx
    on public.discovery_runs (dataset_name, job_name, started_at desc);

create table if not exists public.persistent_candidates (
    candidate_id text primary key,
    canonical_url text,
    current_url text not null,
    domain text,
    source_discovery_only boolean not null default false,
    first_seen_at timestamptz not null,
    last_seen_at timestamptz not null,
    candidate_status text not null,
    scrape_attempt_count integer not null default 0,
    last_scrape_attempt_at timestamptz,
    next_scrape_attempt_at timestamptz,
    last_scrape_error_code text,
    last_scrape_error_message text,
    content_basis text not null,
    retry_priority integer not null default 0,
    needs_review boolean not null default false,
    backfill_batch_id text,
    self_heal_eligible boolean not null default false,
    metadata jsonb not null default '{}'::jsonb
);

create index if not exists persistent_candidates_status_idx
    on public.persistent_candidates (candidate_status, next_scrape_attempt_at);

create index if not exists persistent_candidates_domain_idx
    on public.persistent_candidates (domain, last_seen_at desc);

create index if not exists persistent_candidates_canonical_url_idx
    on public.persistent_candidates (canonical_url)
    where canonical_url is not null;

create table if not exists public.candidate_provenance (
    provenance_id text primary key,
    run_id text not null references public.discovery_runs (run_id) on delete cascade,
    candidate_id text not null references public.persistent_candidates (candidate_id) on delete cascade,
    producer_name text not null,
    producer_kind text not null,
    query_text text,
    raw_url text not null,
    normalized_url text,
    title text,
    snippet text,
    publication_datetime_hint timestamptz,
    rank integer,
    domain text,
    discovered_at timestamptz not null,
    metadata jsonb not null default '{}'::jsonb
);

create index if not exists candidate_provenance_candidate_idx
    on public.candidate_provenance (candidate_id, discovered_at desc);

create index if not exists candidate_provenance_run_idx
    on public.candidate_provenance (run_id, producer_name);

create table if not exists public.scrape_attempts (
    attempt_id text primary key,
    candidate_id text not null references public.persistent_candidates (candidate_id) on delete cascade,
    started_at timestamptz not null,
    finished_at timestamptz,
    attempt_kind text not null,
    fetch_status text not null,
    source_adapter_name text,
    extracted_title text,
    extracted_publication_datetime timestamptz,
    extracted_body_hash text,
    error_code text,
    error_message text,
    diagnostics jsonb not null default '{}'::jsonb
);

create index if not exists scrape_attempts_candidate_idx
    on public.scrape_attempts (candidate_id, started_at desc);

create index if not exists scrape_attempts_status_idx
    on public.scrape_attempts (fetch_status, started_at desc);
