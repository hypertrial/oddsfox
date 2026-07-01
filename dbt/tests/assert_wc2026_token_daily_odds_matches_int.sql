-- Mart must match int daily timeseries filtered to WC2026 tokens with non-null daily rows.
with expected as (
    select
        t.clob_token_id,
        ts.odds_date_utc,
        ts.open_price,
        ts.high_price,
        ts.low_price,
        ts.close_price,
        ts.avg_price,
        ts.observed_points
    from {{ ref('int_polymarket_token_daily_timeseries') }} as ts
    inner join {{ ref('int_polymarket_wc2026_token_universe') }} as t
        on ts.clob_token_id = t.clob_token_id
    where ts.odds_date_utc is not null
)

select
    'mart_only' as failure_type,
    m.clob_token_id,
    m.odds_date_utc
from {{ ref('wc2026_token_daily_odds') }} as m
left join expected as e
    on
        m.clob_token_id = e.clob_token_id
        and m.odds_date_utc = e.odds_date_utc
where e.clob_token_id is null

union all

select
    'expected_only' as failure_type,
    e.clob_token_id,
    e.odds_date_utc
from expected as e
left join {{ ref('wc2026_token_daily_odds') }} as m
    on
        e.clob_token_id = m.clob_token_id
        and e.odds_date_utc = m.odds_date_utc
where m.clob_token_id is null

union all

select
    'value_mismatch' as failure_type,
    m.clob_token_id,
    m.odds_date_utc
from {{ ref('wc2026_token_daily_odds') }} as m
inner join expected as e
    on
        m.clob_token_id = e.clob_token_id
        and m.odds_date_utc = e.odds_date_utc
where
    m.open_price is distinct from e.open_price
    or m.high_price is distinct from e.high_price
    or m.low_price is distinct from e.low_price
    or m.close_price is distinct from e.close_price
    or m.avg_price is distinct from e.avg_price
    or m.observed_points is distinct from e.observed_points
