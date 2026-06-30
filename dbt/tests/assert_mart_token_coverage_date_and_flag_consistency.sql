-- Token coverage dates, counts, and flags must agree.
select
    clob_token_id,
    token_days_observed,
    first_odds_date,
    last_odds_date,
    has_daily_odds,
    avg_gap_days
from {{ ref('token_coverage') }}
where
    avg_gap_days < 0
    or (
        token_days_observed = 0
        and (
            first_odds_date is not null
            or last_odds_date is not null
            or has_daily_odds is distinct from 0
        )
    )
    or (
        token_days_observed > 0
        and (
            first_odds_date is null
            or last_odds_date is null
            or first_odds_date > last_odds_date
            or has_daily_odds is distinct from 1
        )
    )
