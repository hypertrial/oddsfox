-- Raw grain: at most one row per (clobTokenId, odds_date_utc) when both are non-null.
select
    CLOBTOKENID,
    ODDS_DATE_UTC,
    count(*) as ROW_COUNT
from {{ source('polymarket_raw', 'token_odds_daily') }}
where
    CLOBTOKENID is not null
    and ODDS_DATE_UTC is not null
group by 1, 2
having count(*) > 1
