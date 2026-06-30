-- token_latest_odds is intentionally scoped to expanded WC2026 market tokens.
with counts as (
    select
        (
            select count(*)
            from {{ ref('token_latest_odds') }}
        ) as actual_count,
        (
            select count(*)
            from {{ ref('int_polymarket_wc2026_token_universe') }}
        ) as expected_count
)

select
    'row_count_mismatch' as failure_type,
    cast(null as varchar) as market_id,
    actual_count,
    expected_count
from counts
where actual_count <> expected_count

union all

select
    'out_of_scope_market' as failure_type,
    m.market_id,
    cast(null as bigint) as actual_count,
    cast(null as bigint) as expected_count
from {{ ref('token_latest_odds') }} as m
left join {{ ref('int_polymarket_wc2026_token_universe') }} as t
    on m.clob_token_id = t.clob_token_id
where t.clob_token_id is null
