-- When price is present, odds timestamps must be present (join to odds did not drop fields).
select
    clob_token_id,
    price,
    odds_timestamp,
    odds_timestamp_epoch
from {{ ref('int_polymarket_token_timeseries') }}
where
    price is not null
    and (
        odds_timestamp is null
        or odds_timestamp_epoch is null
    )
