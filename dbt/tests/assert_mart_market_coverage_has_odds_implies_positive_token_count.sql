-- Markets with odds coverage must have at least one token in the mart grain.
select
    market_id,
    has_odds_data,
    token_count
from {{ ref('market_coverage') }}
where
    has_odds_data = 1
    and (token_count is null or token_count < 1)
