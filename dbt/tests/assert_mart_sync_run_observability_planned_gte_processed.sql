select *
from {{ ref('sync_run_observability') }}
where
    planned_tokens is not null
    and processed_tokens is not null
    and planned_tokens < processed_tokens
