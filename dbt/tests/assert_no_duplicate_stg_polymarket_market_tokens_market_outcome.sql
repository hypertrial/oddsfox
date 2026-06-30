-- Grain: at most one row per (market_id, outcome_index).
select
    market_id,
    outcome_index,
    count(*) as row_count
from {{ ref('stg_polymarket_market_tokens') }}
group by 1, 2
having count(*) > 1
