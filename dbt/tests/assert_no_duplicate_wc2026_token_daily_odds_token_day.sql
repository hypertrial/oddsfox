-- Grain: at most one row per (clob_token_id, odds_date_utc).
select
    clob_token_id,
    odds_date_utc,
    count(*) as row_count
from {{ ref('wc2026_token_daily_odds') }}
where
    clob_token_id is not null
    and odds_date_utc is not null
group by 1, 2
having count(*) > 1
