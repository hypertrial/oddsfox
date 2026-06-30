-- When odds exist, first_odds_date must not be after last_odds_date.
select
    market_id,
    first_odds_date,
    last_odds_date,
    has_odds_data
from {{ ref('market_coverage') }}
where
    has_odds_data = 1
    and first_odds_date is not null
    and last_odds_date is not null
    and first_odds_date > last_odds_date
