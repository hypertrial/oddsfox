select *
from {{ ref('wc2026_token_minutely_odds') }}
where coalesce(market_volume_usd, 0) >= {{ var('polymarket_whale_min_volume_usd', 100000) }}
