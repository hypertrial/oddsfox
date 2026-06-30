-- token_days_observed must match non-null daily rows per token in the intermediate model.
with expected as (
    select
        clob_token_id,
        count(*) as expected_days
    from {{ ref('int_polymarket_token_daily_timeseries') }}
    where odds_date_utc is not null
    group by clob_token_id
),

actual as (
    select
        clob_token_id,
        token_days_observed
    from {{ ref('token_coverage') }}
)

select
    expected.expected_days,
    actual.token_days_observed,
    coalesce(actual.clob_token_id, expected.clob_token_id) as clob_token_id
from actual
full outer join expected
    on actual.clob_token_id = expected.clob_token_id
where coalesce(expected.expected_days, 0) <> coalesce(actual.token_days_observed, 0)
