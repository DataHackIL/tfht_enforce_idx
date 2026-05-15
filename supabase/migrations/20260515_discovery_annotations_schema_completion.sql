alter table public.persistent_candidates
    add column if not exists titles jsonb not null default '[]'::jsonb,
    add column if not exists snippets jsonb not null default '[]'::jsonb,
    add column if not exists discovered_via jsonb not null default '[]'::jsonb,
    add column if not exists discovery_queries jsonb not null default '[]'::jsonb,
    add column if not exists source_hints jsonb not null default '[]'::jsonb;

create table if not exists public.news_items_corrections (
    dataset_name text not null,
    record_id text,
    canonical_url text,
    relevant boolean,
    enforcement_related boolean,
    taxonomy_version text,
    taxonomy_category_id text,
    taxonomy_subcategory_id text,
    category text,
    sub_category text,
    summary_one_sentence text,
    manual_city text,
    manual_address text,
    manual_event_label text,
    manual_status text,
    reviewer text,
    reviewed_at timestamptz,
    annotation_notes text,
    active boolean not null default true,
    annotation_source text not null default 'manual_correction',
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now()),
    constraint news_items_corrections_identity_chk
        check (record_id is not null or canonical_url is not null)
);

create unique index if not exists news_items_corrections_record_id_idx
    on public.news_items_corrections (dataset_name, record_id)
    where record_id is not null;

create unique index if not exists news_items_corrections_canonical_url_idx
    on public.news_items_corrections (dataset_name, canonical_url)
    where canonical_url is not null;

create index if not exists news_items_corrections_active_idx
    on public.news_items_corrections (dataset_name, active, reviewed_at desc);

create table if not exists public.news_items_missing_items (
    dataset_name text not null,
    annotation_id text not null,
    source_url text not null,
    canonical_url text,
    title text not null,
    event_date timestamptz not null,
    source_name text not null,
    taxonomy_version text,
    taxonomy_category_id text not null,
    taxonomy_subcategory_id text not null,
    category text,
    sub_category text,
    summary_one_sentence text,
    manual_city text,
    manual_address text,
    manual_event_label text,
    manual_status text,
    reviewer text,
    reviewed_at timestamptz,
    annotation_notes text,
    active boolean not null default true,
    promotion_status text not null default 'promoted',
    annotation_source text not null default 'missing_item_annotation',
    index_relevant boolean,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now()),
    primary key (dataset_name, annotation_id)
);

create index if not exists news_items_missing_items_active_idx
    on public.news_items_missing_items (dataset_name, active, event_date desc);
