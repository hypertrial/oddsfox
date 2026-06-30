-- Daily OHLC/avg in [0, 1] when present; high >= low when both present.
select
    clob_token_id,
    odds_date_utc,
    open_price,
    high_price,
    low_price,
    close_price,
    avg_price
from {{ ref('stg_polymarket_odds_daily') }}
where (
    open_price is not null and (open_price < 0 or open_price > 1)
)
or (
    high_price is not null and (high_price < 0 or high_price > 1)
)
or (
    low_price is not null and (low_price < 0 or low_price > 1)
)
or (
    close_price is not null and (close_price < 0 or close_price > 1)
)
or (
    avg_price is not null and (avg_price < 0 or avg_price > 1)
)
or (
    high_price is not null
    and low_price is not null
    and high_price < low_price
)
