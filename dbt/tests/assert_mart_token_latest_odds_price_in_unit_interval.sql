-- CLOB prices are probabilities in [0, 1] when present on latest-odds mart columns.
select
    clob_token_id,
    'latest_daily_open_price' as price_column,
    latest_daily_open_price as price
from {{ ref('token_latest_odds') }}
where
    latest_daily_open_price is not null
    and (latest_daily_open_price < 0 or latest_daily_open_price > 1)

union all

select
    clob_token_id,
    'latest_daily_high_price' as price_column,
    latest_daily_high_price as price
from {{ ref('token_latest_odds') }}
where
    latest_daily_high_price is not null
    and (latest_daily_high_price < 0 or latest_daily_high_price > 1)

union all

select
    clob_token_id,
    'latest_daily_low_price' as price_column,
    latest_daily_low_price as price
from {{ ref('token_latest_odds') }}
where
    latest_daily_low_price is not null
    and (latest_daily_low_price < 0 or latest_daily_low_price > 1)

union all

select
    clob_token_id,
    'latest_daily_close_price' as price_column,
    latest_daily_close_price as price
from {{ ref('token_latest_odds') }}
where
    latest_daily_close_price is not null
    and (latest_daily_close_price < 0 or latest_daily_close_price > 1)

union all

select
    clob_token_id,
    'latest_daily_avg_price' as price_column,
    latest_daily_avg_price as price
from {{ ref('token_latest_odds') }}
where
    latest_daily_avg_price is not null
    and (latest_daily_avg_price < 0 or latest_daily_avg_price > 1)

union all

select
    clob_token_id,
    'latest_point_price' as price_column,
    latest_point_price as price
from {{ ref('token_latest_odds') }}
where
    latest_point_price is not null
    and (latest_point_price < 0 or latest_point_price > 1)
