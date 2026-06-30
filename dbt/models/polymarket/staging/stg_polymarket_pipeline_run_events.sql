select
    run_id,
    task_name,
    recorded_at,
    metrics_json,
    try_cast(json_extract_string(metrics_json, '$.noop') as boolean) as is_noop,
    try_cast(json_extract_string(metrics_json, '$.duration_seconds') as double)
        as duration_seconds,
    try_cast(json_extract_string(metrics_json, '$.tokens') as bigint)
        as processed_tokens,
    try_cast(json_extract_string(metrics_json, '$.windows') as bigint)
        as windows_processed,
    try_cast(json_extract_string(metrics_json, '$.rows') as bigint) as rows_fetched,
    try_cast(json_extract_string(metrics_json, '$.empty') as bigint) as empty_tokens,
    try_cast(json_extract_string(metrics_json, '$.errors') as bigint) as error_tokens,
    try_cast(json_extract_string(metrics_json, '$.permanent_errors') as bigint)
        as permanent_error_tokens,
    try_cast(json_extract_string(metrics_json, '$.invalid_tokens') as bigint)
        as invalid_tokens,
    try_cast(json_extract_string(metrics_json, '$.saved_rows') as bigint) as saved_rows,
    try_cast(json_extract_string(metrics_json, '$.saved_daily_rows') as bigint)
        as saved_daily_rows,
    try_cast(json_extract_string(metrics_json, '$.sync_updates') as bigint)
        as sync_updates,
    try_cast(json_extract_string(metrics_json, '$.queue_high_watermark') as bigint)
        as queue_high_watermark,
    try_cast(json_extract_string(metrics_json, '$.http_requests') as bigint)
        as http_requests,
    try_cast(json_extract_string(metrics_json, '$.http_429') as bigint) as http_429,
    try_cast(json_extract_string(metrics_json, '$.http_errors') as bigint)
        as http_errors,
    try_cast(json_extract_string(metrics_json, '$.final_rps') as double) as final_rps,
    try_cast(json_extract_string(metrics_json, '$.token_rate') as double) as token_rate,
    try_cast(json_extract_string(metrics_json, '$.row_rate') as double) as row_rate,
    try_cast(json_extract_string(metrics_json, '$.planning.plans') as bigint)
        as planned_tokens,
    try_cast(
        json_extract_string(metrics_json, '$.planning.pre_clob_markets') as bigint
    ) as pre_clob_markets,
    try_cast(json_extract_string(metrics_json, '$.planning.invalid_token') as bigint)
        as invalid_tokens_from_planning,
    try_cast(json_extract_string(metrics_json, '$.planning.closed_done') as bigint)
        as closed_done_tokens,
    try_cast(json_extract_string(metrics_json, '$.planning.persisted_skip') as bigint)
        as persisted_skip_tokens,
    try_cast(json_extract_string(metrics_json, '$.planning.recent_skip') as bigint)
        as recent_skip_tokens,
    try_cast(
        json_extract_string(metrics_json, '$.planning.empty_cache_skip') as bigint
    ) as empty_cache_skip_tokens,
    try_cast(
        json_extract_string(metrics_json, '$.planning.already_current') as bigint
    ) as already_current_tokens,
    try_cast(json_extract_string(metrics_json, '$.planning.dup_token') as bigint)
        as duplicate_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.market_tokens_distinct_tokens'
        ) as bigint
    ) as market_tokens_distinct_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.odds_history_distinct_tokens'
        ) as bigint
    ) as odds_history_distinct_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.token_odds_daily_distinct_tokens'
        ) as bigint
    ) as token_odds_daily_distinct_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.ledger_distinct_tokens'
        ) as bigint
    ) as ledger_distinct_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.ledger_fully_checked_tokens'
        ) as bigint
    ) as ledger_fully_checked_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.token_sync_skips_distinct_tokens'
        ) as bigint
    ) as token_sync_skips_distinct_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.market_tokens_without_history'
        ) as bigint
    ) as market_tokens_without_history,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.history_tokens_without_market_tokens'
        ) as bigint
    ) as history_tokens_without_market_tokens,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.planned_vs_market_tokens'
        ) as double
    ) as planned_vs_market_tokens_ratio,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.history_coverage_vs_market_tokens'
        ) as double
    ) as history_coverage_vs_market_tokens_ratio,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.daily_coverage_vs_market_tokens'
        ) as double
    ) as daily_coverage_vs_market_tokens_ratio,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.ledger_coverage_vs_market_tokens'
        ) as double
    ) as ledger_coverage_vs_market_tokens_ratio,
    try_cast(
        json_extract_string(
            metrics_json, '$.planning_context.fully_checked_vs_market_tokens'
        ) as double
    ) as fully_checked_vs_market_tokens_ratio
from {{ source('polymarket_ops', 'pipeline_run_events') }}
