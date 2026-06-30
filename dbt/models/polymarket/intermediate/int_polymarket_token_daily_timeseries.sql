select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.question,
    t.event_slug,
    t.is_active,
    t.is_closed,
    o.odds_date_utc,
    o.open_price,
    o.high_price,
    o.low_price,
    o.close_price,
    o.avg_price,
    o.observed_points,
    o.first_timestamp,
    o.first_observed_at,
    o.last_timestamp,
    o.last_observed_at
from {{ ref('int_polymarket_token_universe') }} as t
left join {{ ref('stg_polymarket_odds_daily') }} as o
    on t.clob_token_id = o.clob_token_id
