select
    market_id,
    question,
    event_slug,
    count(*) as token_count,
    sum(token_days_observed) as token_days_observed,
    min(first_odds_date) as first_odds_date,
    max(last_odds_date) as last_odds_date,
    max(has_daily_odds) as has_odds_data
from {{ ref('token_coverage') }}
group by 1, 2, 3
