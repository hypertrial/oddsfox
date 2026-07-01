select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.question,
    t.event_slug,
    t.is_active,
    t.is_closed,
    t.market_volume_usd,
    ts.odds_date_utc,
    ts.open_price,
    ts.high_price,
    ts.low_price,
    ts.close_price,
    ts.avg_price,
    ts.observed_points,
    ts.first_timestamp,
    ts.first_observed_at,
    ts.last_timestamp,
    ts.last_observed_at
from {{ ref('int_polymarket_token_daily_timeseries') }} as ts
inner join {{ ref('int_polymarket_wc2026_token_universe') }} as t
    on ts.clob_token_id = t.clob_token_id
where ts.odds_date_utc is not null
