-- Grain: at most one row per (clob_token_id, odds_timestamp_epoch) when both are non-null.
select
    clob_token_id,
    odds_timestamp_epoch,
    count(*) as row_count
from {{ ref('stg_polymarket_odds') }}
where
    clob_token_id is not null
    and odds_timestamp_epoch is not null
group by 1, 2
having count(*) > 1
