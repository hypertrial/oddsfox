with latest_daily_key as (
    select
        clob_token_id,
        max(odds_date_utc) as latest_daily_odds_date_utc
    from {{ ref('int_polymarket_token_daily_timeseries') }}
    where odds_date_utc is not null
    group by clob_token_id
),

latest_daily as (
    select
        ts.clob_token_id,
        ts.odds_date_utc,
        ts.open_price,
        ts.high_price,
        ts.low_price,
        ts.close_price,
        ts.avg_price
    from {{ ref('int_polymarket_token_daily_timeseries') }} as ts
    inner join latest_daily_key as k
        on
            ts.clob_token_id = k.clob_token_id
            and ts.odds_date_utc = k.latest_daily_odds_date_utc
),

latest_point_key as (
    select
        clob_token_id,
        max(odds_timestamp_epoch) as latest_point_odds_timestamp_epoch
    from {{ ref('int_polymarket_token_timeseries') }}
    where price is not null
    group by clob_token_id
),

latest_point as (
    select
        ts.clob_token_id,
        ts.odds_timestamp,
        ts.odds_timestamp_epoch,
        ts.price
    from {{ ref('int_polymarket_token_timeseries') }} as ts
    inner join latest_point_key as k
        on
            ts.clob_token_id = k.clob_token_id
            and ts.odds_timestamp_epoch = k.latest_point_odds_timestamp_epoch
)

select
    t.clob_token_id,
    t.market_id,
    t.outcome_index,
    t.question,
    t.event_slug,
    t.is_active,
    t.is_closed,
    ld.odds_date_utc as latest_daily_odds_date_utc,
    ld.open_price as latest_daily_open_price,
    ld.high_price as latest_daily_high_price,
    ld.low_price as latest_daily_low_price,
    ld.close_price as latest_daily_close_price,
    ld.avg_price as latest_daily_avg_price,
    lp.price as latest_point_price,
    lp.odds_timestamp as latest_point_odds_timestamp,
    lp.odds_timestamp_epoch as latest_point_odds_timestamp_epoch
from {{ ref('int_polymarket_wc2026_token_universe') }} as t
left join latest_daily as ld
    on t.clob_token_id = ld.clob_token_id
left join latest_point as lp
    on t.clob_token_id = lp.clob_token_id
