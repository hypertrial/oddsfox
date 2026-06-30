-- Sub-counts of HTTP outcomes cannot exceed total HTTP requests when both are recorded.
select *
from {{ ref('sync_run_observability') }}
where
    http_requests is not null
    and (
        (http_429 is not null and http_429 > http_requests)
        or (http_errors is not null and http_errors > http_requests)
    )
