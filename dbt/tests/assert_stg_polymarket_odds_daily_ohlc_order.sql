-- Daily aggregate contract: counts/timestamps are ordered and OHLC values sit within low/high when comparable.
select
    clob_token_id,
    odds_date_utc,
    open_price,
    high_price,
    low_price,
    close_price,
    avg_price,
    observed_points,
    first_timestamp,
    last_timestamp
from {{ ref('stg_polymarket_odds_daily') }}
where
    observed_points < 1
    or (
        first_timestamp is not null
        and last_timestamp is not null
        and first_timestamp > last_timestamp
    )
    or (
        low_price is not null
        and high_price is not null
        and open_price is not null
        and (open_price < low_price or open_price > high_price)
    )
    or (
        low_price is not null
        and high_price is not null
        and close_price is not null
        and (close_price < low_price or close_price > high_price)
    )
    or (
        low_price is not null
        and high_price is not null
        and avg_price is not null
        and (avg_price < low_price or avg_price > high_price)
    )
