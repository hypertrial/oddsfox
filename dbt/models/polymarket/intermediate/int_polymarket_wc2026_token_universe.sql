select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.token_updated_at,
    t.question,
    t.event_slug,
    t.is_active,
    t.is_closed,
    t.market_volume_usd
from {{ ref('int_polymarket_token_universe') }} as t
inner join {{ ref('int_polymarket_wc2026_markets') }} as m
    on t.market_id = m.market_id
