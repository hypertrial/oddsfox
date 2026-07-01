-- CLOB prices are probabilities in [0, 1].
select
    clob_token_id,
    odds_timestamp_epoch,
    price
from {{ ref('wc2026_token_minutely_odds') }}
where
    price is not null
    and (price < 0 or price > 1)
