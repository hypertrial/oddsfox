-- Public coverage marts must agree on market-level coverage rollups.
with expected as (
    select
        market_id,
        question,
        event_slug,
        count(*) as token_count,
        sum(token_days_observed) as token_days_observed,
        min(first_odds_date) as first_odds_date,
        max(last_odds_date) as last_odds_date,
        max(case when token_days_observed > 0 then 1 else 0 end) as has_odds_data
    from {{ ref('token_coverage') }}
    group by 1, 2, 3
),

actual as (
    select
        market_id,
        question,
        event_slug,
        token_count,
        token_days_observed,
        first_odds_date,
        last_odds_date,
        has_odds_data
    from {{ ref('market_coverage') }}
)

select
    a.token_count as actual_token_count,
    e.token_count as expected_token_count,
    a.token_days_observed as actual_token_days_observed,
    e.token_days_observed as expected_token_days_observed,
    a.first_odds_date as actual_first_odds_date,
    e.first_odds_date as expected_first_odds_date,
    a.last_odds_date as actual_last_odds_date,
    e.last_odds_date as expected_last_odds_date,
    a.has_odds_data as actual_has_odds_data,
    e.has_odds_data as expected_has_odds_data,
    coalesce(a.market_id, e.market_id) as market_id
from actual as a
full outer join expected as e
    on
        a.market_id = e.market_id
        and a.question is not distinct from e.question
        and a.event_slug is not distinct from e.event_slug
where
    a.market_id is null
    or e.market_id is null
    or a.token_count is distinct from e.token_count
    or a.token_days_observed is distinct from e.token_days_observed
    or a.first_odds_date is distinct from e.first_odds_date
    or a.last_odds_date is distinct from e.last_odds_date
    or a.has_odds_data is distinct from e.has_odds_data
