-- rows_per_processed_token must match rows_fetched / processed_tokens when execution ran work.
select
    run_id,
    rows_per_processed_token,
    rows_fetched,
    processed_tokens,
    rows_fetched::double / processed_tokens as expected_ratio
from {{ ref('sync_run_observability') }}
where
    processed_tokens is not null
    and processed_tokens > 0
    and rows_fetched is not null
    and rows_per_processed_token is not null
    and abs(
        rows_per_processed_token - (rows_fetched::double / processed_tokens)
    ) > 1e-9
