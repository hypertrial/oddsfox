select
    CLOBTOKENID as CLOB_TOKEN_ID,
    ODDS_DATE_UTC,
    OPEN_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    CLOSE_PRICE,
    AVG_PRICE,
    OBSERVED_POINTS,
    FIRST_TIMESTAMP,
    LAST_TIMESTAMP,
    REFRESHED_AT,
    to_timestamp(FIRST_TIMESTAMP) as FIRST_OBSERVED_AT,
    to_timestamp(LAST_TIMESTAMP) as LAST_OBSERVED_AT
from {{ source('polymarket_raw', 'token_odds_daily') }}
