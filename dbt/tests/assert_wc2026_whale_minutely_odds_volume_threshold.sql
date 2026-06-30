-- Whale minutely mart must only include rows at or above the configured market-volume threshold.
select
    market_id,
    clob_token_id,
    odds_timestamp_epoch,
    market_volume_usd
from {{ ref('wc2026_whale_minutely_odds') }}
where
    market_volume_usd is null
    or market_volume_usd < {{ var('polymarket_whale_min_volume_usd', 100000) }}
