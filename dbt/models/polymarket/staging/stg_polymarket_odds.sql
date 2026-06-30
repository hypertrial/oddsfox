select
    CLOBTOKENID as CLOB_TOKEN_ID,
    TIMESTAMP as ODDS_TIMESTAMP_EPOCH,
    PRICE,
    INGESTED_AT,
    to_timestamp(TIMESTAMP) as ODDS_TIMESTAMP
from {{ source('polymarket_raw', 'odds_history') }}
