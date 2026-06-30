select *
from {{ ref('sync_run_observability') }}
where
    coalesce(planned_tokens, 0) < 0
    or coalesce(processed_tokens, 0) < 0
    or coalesce(rows_fetched, 0) < 0
    or coalesce(empty_tokens, 0) < 0
    or coalesce(error_tokens, 0) < 0
    or coalesce(permanent_error_tokens, 0) < 0
    or coalesce(invalid_tokens, 0) < 0
