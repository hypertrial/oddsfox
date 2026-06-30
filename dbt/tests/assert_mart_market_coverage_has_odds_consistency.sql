-- When has_odds_data = 1, date bounds and token-day count must be populated.
select
    market_id,
    has_odds_data,
    first_odds_date,
    last_odds_date,
    token_days_observed
from {{ ref('market_coverage') }}
where
    has_odds_data = 1
    and (
        first_odds_date is null
        or last_odds_date is null
        or token_days_observed is null
        or token_days_observed <= 0
    )
