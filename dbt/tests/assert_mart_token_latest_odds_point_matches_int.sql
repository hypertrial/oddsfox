-- Latest point-in-time price must match the newest non-null point row per WC2026 token.
with latest_key as (
    select
        t.clob_token_id,
        max(ts.odds_timestamp_epoch) as latest_point_odds_timestamp_epoch
    from {{ ref('int_polymarket_wc2026_token_universe') }} as t
    inner join {{ ref('int_polymarket_token_timeseries') }} as ts
        on t.clob_token_id = ts.clob_token_id
    where ts.price is not null
    group by t.clob_token_id
),

expected as (
    select
        ts.clob_token_id,
        ts.odds_timestamp_epoch,
        ts.price
    from {{ ref('int_polymarket_token_timeseries') }} as ts
    inner join latest_key as k
        on
            ts.clob_token_id = k.clob_token_id
            and ts.odds_timestamp_epoch = k.latest_point_odds_timestamp_epoch
)

select
    e.clob_token_id,
    e.odds_timestamp_epoch as expected_epoch,
    e.price as expected_price,
    m.latest_point_odds_timestamp_epoch,
    m.latest_point_price
from expected as e
left join {{ ref('token_latest_odds') }} as m
    on e.clob_token_id = m.clob_token_id
where
    m.clob_token_id is null
    or m.latest_point_odds_timestamp_epoch is distinct from e.odds_timestamp_epoch
    or m.latest_point_price is distinct from e.price
