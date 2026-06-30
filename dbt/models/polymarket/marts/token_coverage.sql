with daily_agg as (
    select
        clob_token_id,
        min(odds_date_utc) as first_odds_date,
        max(odds_date_utc) as last_odds_date,
        count(*) as token_days_observed,
        sum(observed_points) as sum_observed_points
    from {{ ref('int_polymarket_token_daily_timeseries') }}
    where odds_date_utc is not null
    group by clob_token_id
),

gap_agg as (
    select
        clob_token_id,
        max(coalesce(date_diff('day', prev_odds_date_utc, odds_date_utc), 0))
            as max_gap_days,
        coalesce(
            avg(nullif(date_diff('day', prev_odds_date_utc, odds_date_utc), 0)),
            0
        ) as avg_gap_days
    from (
        select
            clob_token_id,
            odds_date_utc,
            lag(odds_date_utc) over (
                partition by clob_token_id order by odds_date_utc
            ) as prev_odds_date_utc
        from {{ ref('int_polymarket_token_daily_timeseries') }}
        where odds_date_utc is not null
    ) as ordered
    group by clob_token_id
),

market_health as (
    select
        t.market_id,
        count(*) as market_token_count,
        sum(case when l.is_fully_checked then 1 else 0 end)
            as market_fully_checked_tokens
    from {{ ref('int_polymarket_token_universe') }} as t
    left join {{ ref('stg_polymarket_sync_ledger') }} as l
        on t.clob_token_id = l.clob_token_id
    group by t.market_id
)

select
    t.clob_token_id,
    t.market_id,
    t.outcome_index,
    t.question,
    t.event_slug,
    t.is_active,
    t.is_closed,
    d.first_odds_date,
    d.last_odds_date,
    l.is_fully_checked,
    l.last_sync_timestamp,
    l.last_sync_at,
    l.last_checked_at,
    l.next_check_at,
    l.empty_run_streak,
    s.reason as skip_reason,
    s.created_at as skip_created_at,
    s.reason is not null as has_persisted_skip,
    coalesce(g.max_gap_days, 0) as max_gap_days,
    coalesce(g.avg_gap_days, 0) as avg_gap_days,
    coalesce(mh.market_token_count, 0) as market_token_count,
    coalesce(mh.market_fully_checked_tokens, 0) as market_fully_checked_tokens,
    coalesce(d.token_days_observed, 0) as token_days_observed,
    coalesce(d.sum_observed_points, 0) as sum_observed_points,
    case
        when coalesce(mh.market_token_count, 0) = 0 then false
        when mh.market_token_count = mh.market_fully_checked_tokens then true
        else false
    end as market_fully_checked,
    case when coalesce(d.token_days_observed, 0) > 0 then 1 else 0 end
        as has_daily_odds
from {{ ref('int_polymarket_token_universe') }} as t
left join daily_agg as d
    on t.clob_token_id = d.clob_token_id
left join {{ ref('stg_polymarket_sync_ledger') }} as l
    on t.clob_token_id = l.clob_token_id
left join {{ ref('stg_polymarket_token_sync_skips') }} as s
    on t.clob_token_id = s.clob_token_id
left join gap_agg as g
    on t.clob_token_id = g.clob_token_id
left join market_health as mh
    on t.market_id = mh.market_id
