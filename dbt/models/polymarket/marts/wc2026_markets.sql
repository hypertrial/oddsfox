select
    market_id,
    question,
    description,
    category,
    outcomes,
    volume,
    is_active,
    is_closed,
    created_at,
    scraped_at,
    end_date,
    slug,
    event_slug
from {{ ref('int_polymarket_wc2026_markets') }}
