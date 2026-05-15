alter table public.news_items
    add column if not exists taxonomy_version text,
    add column if not exists taxonomy_category_id text,
    add column if not exists taxonomy_subcategory_id text,
    add column if not exists index_relevant boolean not null default false,
    add column if not exists manual_address text,
    add column if not exists manual_event_label text,
    add column if not exists manual_status text,
    add column if not exists manually_overridden boolean not null default false,
    add column if not exists annotation_source text,
    add column if not exists manual_city text,
    add column if not exists manually_reviewed boolean not null default false,
    add column if not exists reviewer text,
    add column if not exists reviewed_at timestamptz,
    add column if not exists annotation_notes text;

create index if not exists news_items_taxonomy_idx
    on public.news_items (taxonomy_category_id, taxonomy_subcategory_id);

create index if not exists news_items_index_relevant_idx
    on public.news_items (index_relevant);
