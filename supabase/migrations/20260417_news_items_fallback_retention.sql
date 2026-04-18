alter table public.news_items
    add column if not exists content_basis text;

update public.news_items
set content_basis = 'full_article_page'
where content_basis is null;

alter table public.news_items
    alter column content_basis set default 'full_article_page';

alter table public.news_items
    alter column content_basis set not null;

alter table public.news_items
    add column if not exists record_confidence text;

update public.news_items
set record_confidence = coalesce(classification_confidence, 'medium')
where record_confidence is null;

alter table public.news_items
    alter column record_confidence set default 'medium';
