-- Latest daily date and close must match the row at max(odds_date_utc) per WC2026 token.
with latest_key as (
    select
        t.clob_token_id,
        max(ts.odds_date_utc) as latest_daily_odds_date_utc
    from {{ ref('int_polymarket_wc2026_token_universe') }} as t
    inner join {{ ref('int_polymarket_token_daily_timeseries') }} as ts
        on t.clob_token_id = ts.clob_token_id
    where ts.odds_date_utc is not null
    group by t.clob_token_id
),

expected as (
    select
        ts.clob_token_id,
        ts.odds_date_utc,
        ts.close_price
    from {{ ref('int_polymarket_token_daily_timeseries') }} as ts
    inner join latest_key as k
        on
            ts.clob_token_id = k.clob_token_id
            and ts.odds_date_utc = k.latest_daily_odds_date_utc
)

select
    e.clob_token_id,
    e.odds_date_utc as expected_date,
    e.close_price as expected_close,
    m.latest_daily_odds_date_utc,
    m.latest_daily_close_price
from expected as e
left join {{ ref('token_latest_odds') }} as m
    on e.clob_token_id = m.clob_token_id
where
    m.clob_token_id is null
    or m.latest_daily_odds_date_utc is distinct from e.odds_date_utc
    or m.latest_daily_close_price is distinct from e.close_price
