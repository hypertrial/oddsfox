-- Grain: at most one row per (clob_token_id, odds_date_utc).
select
    clob_token_id,
    odds_date_utc,
    count(*) as row_count
from {{ ref('stg_polymarket_odds_daily') }}
group by 1, 2
having count(*) > 1
