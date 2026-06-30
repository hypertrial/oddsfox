with raw_markets as (
    select
        id as market_id,
        question,
        category,
        description,
        outcomes,
        volume,
        cast(active as boolean) as is_active,
        cast(closed as boolean) as is_closed,
        created_at,
        scraped_at,
        end_date,
        slug,
        event_slug,
        event_id
    from {{ source('polymarket_raw', 'markets') }}
),

registry as (
    select market_id
    from {{ source('polymarket_ops', 'wc2026_market_registry') }}
),

allowlisted_slugs as (
    select unnest({{ var('wc2026_event_slugs') }}) as event_slug
),

allowlisted_prefixes as (
    select unnest({{ var('wc2026_event_slug_prefixes') }}) as slug_prefix
)

select
    markets.market_id,
    markets.question,
    markets.category,
    markets.description,
    markets.outcomes,
    markets.volume,
    markets.is_active,
    markets.is_closed,
    markets.created_at,
    markets.scraped_at,
    markets.end_date,
    markets.slug,
    markets.event_slug,
    markets.event_id,
    (
        wc_registry.market_id is not null
        or lower(coalesce(markets.event_slug, '')) in (
            select lower(allowlisted_slugs.event_slug) from allowlisted_slugs
        )
        or exists (
            select 1
            from allowlisted_prefixes as slug_prefixes
            where lower(coalesce(markets.event_slug, '')) like slug_prefixes.slug_prefix || '%'
        )
    ) as is_wc2026_target
from raw_markets as markets
left join registry as wc_registry
    on markets.market_id = wc_registry.market_id
