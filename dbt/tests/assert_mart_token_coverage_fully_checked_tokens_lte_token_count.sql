-- Business rule: fully checked tokens cannot exceed market token count.
select
    market_id,
    market_token_count,
    market_fully_checked_tokens
from {{ ref('token_coverage') }}
where market_fully_checked_tokens > market_token_count
