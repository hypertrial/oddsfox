select
    clobtokenid as clob_token_id,
    last_sync_timestamp,
    last_checked_at,
    next_check_at,
    cast(fully_checked as boolean) as is_fully_checked,
    to_timestamp(last_sync_timestamp) as last_sync_at,
    coalesce(empty_run_streak, 0) as empty_run_streak
from {{ source('polymarket_ops', 'token_sync_ledger') }}
