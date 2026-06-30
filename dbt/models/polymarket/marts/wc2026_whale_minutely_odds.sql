select
    t.market_id,
    t.outcome_index,
    t.clob_token_id,
    t.question,
    t.event_slug,
    t.is_active,
    t.is_closed,
    t.market_volume_usd,
    ts.odds_timestamp,
    ts.odds_timestamp_epoch,
    ts.price
from {{ ref('int_polymarket_token_timeseries') }} as ts
inner join {{ ref('int_polymarket_wc2026_token_universe') }} as t
    on ts.clob_token_id = t.clob_token_id
where
    ts.price is not null
    and ts.odds_timestamp_epoch is not null
    and coalesce(t.market_volume_usd, 0) >= {{ var('polymarket_whale_min_volume_usd', 100000) }}
