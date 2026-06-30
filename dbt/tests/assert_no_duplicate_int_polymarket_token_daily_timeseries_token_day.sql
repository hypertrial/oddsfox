-- Grain: at most one row per (clob_token_id, odds_date_utc), including at most one row with null odds_date_utc per token.
select
    clob_token_id,
    odds_date_utc,
    count(*) as row_count
from {{ ref('int_polymarket_token_daily_timeseries') }}
group by 1, 2
having count(*) > 1
