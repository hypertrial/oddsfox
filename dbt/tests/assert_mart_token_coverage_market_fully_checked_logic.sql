-- market_fully_checked iff the market has tokens and all are fully checked.
select
    market_id,
    market_token_count,
    market_fully_checked_tokens,
    market_fully_checked
from {{ ref('token_coverage') }}
where market_fully_checked <> (
    coalesce(market_token_count, 0) > 0
    and coalesce(market_token_count, 0) = coalesce(market_fully_checked_tokens, 0)
)
