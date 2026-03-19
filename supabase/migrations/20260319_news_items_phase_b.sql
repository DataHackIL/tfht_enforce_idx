create extension if not exists pgcrypto;

create table if not exists public.news_items (
    id text primary key,
    source_name text not null,
    source_domain text not null,
    url text not null,
    canonical_url text not null unique,
    publication_datetime timestamptz not null,
    retrieval_datetime timestamptz not null,
    language text not null default 'he',
    title text not null,
    category text not null,
    sub_category text,
    summary_one_sentence text not null,
    geography_country text not null default 'Israel',
    geography_region text,
    geography_city text,
    organizations_mentioned jsonb not null default '[]'::jsonb,
    topic_tags jsonb not null default '[]'::jsonb,
    rights_class text not null,
    privacy_risk_level text not null,
    review_status text not null,
    publication_status text not null,
    takedown_status text not null default 'none',
    event_candidate_ids jsonb not null default '[]'::jsonb,
    source_urls jsonb not null default '[]'::jsonb,
    source_count integer not null default 1,
    classification_confidence text,
    suppression_reason text,
    summary_generation_model text,
    privacy_reason text,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now()),
    release_version text
);

create index if not exists news_items_publication_datetime_idx
    on public.news_items (publication_datetime desc);

create index if not exists news_items_publication_status_idx
    on public.news_items (publication_status);

create index if not exists news_items_takedown_status_idx
    on public.news_items (takedown_status);

create table if not exists public.ingestion_runs (
    run_timestamp timestamptz primary key,
    started_at timestamptz,
    finished_at timestamptz,
    dataset_name text not null,
    job_name text not null,
    config_name text not null,
    config_path text,
    days_searched integer,
    source_count integer not null default 0,
    output_formats jsonb not null default '[]'::jsonb,
    raw_article_count integer not null default 0,
    unseen_article_count integer not null default 0,
    relevant_article_count integer not null default 0,
    unified_item_count integer not null default 0,
    seen_count_before integer not null default 0,
    seen_count_after integer not null default 0,
    fatal boolean not null default false,
    warnings jsonb not null default '[]'::jsonb,
    errors jsonb not null default '[]'::jsonb,
    result_summary text,
    release_manifest jsonb,
    backup_manifest jsonb
);

create table if not exists public.release_runs (
    run_timestamp timestamptz primary key,
    started_at timestamptz,
    finished_at timestamptz,
    dataset_name text not null,
    job_name text not null,
    config_name text not null,
    config_path text,
    days_searched integer,
    source_count integer not null default 0,
    output_formats jsonb not null default '[]'::jsonb,
    raw_article_count integer not null default 0,
    unseen_article_count integer not null default 0,
    relevant_article_count integer not null default 0,
    unified_item_count integer not null default 0,
    seen_count_before integer not null default 0,
    seen_count_after integer not null default 0,
    fatal boolean not null default false,
    warnings jsonb not null default '[]'::jsonb,
    errors jsonb not null default '[]'::jsonb,
    result_summary text,
    release_manifest jsonb,
    backup_manifest jsonb
);

create table if not exists public.backup_runs (
    run_timestamp timestamptz primary key,
    started_at timestamptz,
    finished_at timestamptz,
    dataset_name text not null,
    job_name text not null,
    config_name text not null,
    config_path text,
    days_searched integer,
    source_count integer not null default 0,
    output_formats jsonb not null default '[]'::jsonb,
    raw_article_count integer not null default 0,
    unseen_article_count integer not null default 0,
    relevant_article_count integer not null default 0,
    unified_item_count integer not null default 0,
    seen_count_before integer not null default 0,
    seen_count_after integer not null default 0,
    fatal boolean not null default false,
    warnings jsonb not null default '[]'::jsonb,
    errors jsonb not null default '[]'::jsonb,
    result_summary text,
    release_manifest jsonb,
    backup_manifest jsonb
);

create table if not exists public.suppression_rules (
    id uuid primary key default gen_random_uuid(),
    dataset_name text not null,
    canonical_url text,
    record_id text,
    suppression_reason text not null,
    active boolean not null default true,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now())
);

create index if not exists suppression_rules_dataset_active_idx
    on public.suppression_rules (dataset_name, active);
