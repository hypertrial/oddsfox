select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.question,
    t.event_slug,
    t.is_active,
    t.is_closed,
    o.odds_timestamp,
    o.odds_timestamp_epoch,
    o.price
from {{ ref('int_polymarket_token_universe') }} as t
left join {{ ref('stg_polymarket_odds') }} as o
    on t.clob_token_id = o.clob_token_id
