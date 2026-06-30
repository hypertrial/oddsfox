-- Raw grain: at most one row per (clobTokenId, timestamp) when both are non-null.
select
    CLOBTOKENID,
    TIMESTAMP,
    count(*) as ROW_COUNT
from {{ source('polymarket_raw', 'odds_history') }}
where
    CLOBTOKENID is not null
    and TIMESTAMP is not null
group by 1, 2
having count(*) > 1
