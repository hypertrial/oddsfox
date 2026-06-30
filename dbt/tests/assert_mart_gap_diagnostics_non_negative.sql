-- Token health should not report negative coverage or gaps.
select
    market_id,
    clob_token_id,
    token_days_observed,
    max_gap_days
from {{ ref('token_coverage') }}
where
    token_days_observed < 0
    or max_gap_days < 0
