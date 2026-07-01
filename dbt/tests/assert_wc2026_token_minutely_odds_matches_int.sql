-- Mart must match int timeseries filtered to WC2026 tokens with non-null minutely odds.
with expected as (
    select
        t.clob_token_id,
        ts.odds_timestamp_epoch,
        ts.price
    from {{ ref('int_polymarket_token_timeseries') }} as ts
    inner join {{ ref('int_polymarket_wc2026_token_universe') }} as t
        on ts.clob_token_id = t.clob_token_id
    where
        ts.price is not null
        and ts.odds_timestamp_epoch is not null
)

select
    'mart_only' as failure_type,
    m.clob_token_id,
    m.odds_timestamp_epoch,
    m.price
from {{ ref('wc2026_token_minutely_odds') }} as m
left join expected as e
    on
        m.clob_token_id = e.clob_token_id
        and m.odds_timestamp_epoch = e.odds_timestamp_epoch
where e.clob_token_id is null

union all

select
    'expected_only' as failure_type,
    e.clob_token_id,
    e.odds_timestamp_epoch,
    e.price
from expected as e
left join {{ ref('wc2026_token_minutely_odds') }} as m
    on
        e.clob_token_id = m.clob_token_id
        and e.odds_timestamp_epoch = m.odds_timestamp_epoch
where m.clob_token_id is null

union all

select
    'price_mismatch' as failure_type,
    m.clob_token_id,
    m.odds_timestamp_epoch,
    m.price
from {{ ref('wc2026_token_minutely_odds') }} as m
inner join expected as e
    on
        m.clob_token_id = e.clob_token_id
        and m.odds_timestamp_epoch = e.odds_timestamp_epoch
where m.price is distinct from e.price
