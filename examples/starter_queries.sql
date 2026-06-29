-- oddsfox starter queries
-- Paste into: oddsfox duckdb --out ~/.oddsfox
-- Or run one line: oddsfox sql "SELECT COUNT(*) FROM bronze_markets" --out ~/.oddsfox

-- 1. Top active markets by 24h volume
SELECT question, volume_24h, liquidity, source
FROM bronze_markets
WHERE active = true
ORDER BY volume_24h DESC NULLS LAST
LIMIT 20;

-- 2. Market count by source
SELECT source, COUNT(*) AS markets
FROM bronze_markets
GROUP BY source
ORDER BY markets DESC;

-- 3. Price history for a token (replace token_id)
SELECT ts, price, fidelity_minutes
FROM bronze_prices
WHERE token_id = 'YOUR_TOKEN_ID'
ORDER BY ts;

-- 4. Latest price per outcome for a market (replace market_id)
SELECT o.outcome_name, p.ts, p.price
FROM bronze_outcomes o
JOIN bronze_prices p ON o.token_id = p.token_id
WHERE o.market_id = 'YOUR_MARKET_ID'
QUALIFY ROW_NUMBER() OVER (PARTITION BY o.token_id ORDER BY p.ts DESC) = 1;

-- 5. Join markets to events
SELECT e.title, m.question, m.volume_24h
FROM bronze_markets m
JOIN bronze_events e ON m.event_id = e.event_id
ORDER BY m.volume_24h DESC NULLS LAST
LIMIT 15;

-- 6. Resolved markets since a date
SELECT market_id, question, resolution_time
FROM bronze_markets
WHERE resolved = true
  AND resolution_time >= '2024-01-01'
ORDER BY resolution_time DESC
LIMIT 25;

-- 7. Winning outcomes
SELECT r.market_id, m.question, r.winning_outcome, r.resolved_at
FROM bronze_resolutions r
JOIN bronze_markets m ON r.market_id = m.market_id
ORDER BY r.resolved_at DESC
LIMIT 20;

-- 8. Liquidity metrics summary
SELECT metric_name, COUNT(*) AS n, AVG(value) AS avg_value
FROM gold_metric_points
GROUP BY metric_name
ORDER BY n DESC;

-- 9. Widest spreads among active markets
SELECT m.question, mp.value AS spread
FROM gold_metric_points mp
JOIN bronze_markets m ON mp.market_id = m.market_id
WHERE mp.metric_name = 'spread'
  AND m.active = true
ORDER BY mp.value DESC NULLS LAST
LIMIT 15;

-- 10. Calibration buckets
SELECT bucket_start, bucket_end, mean_prediction, observed_rate, sample_count
FROM gold_calibration
ORDER BY bucket_start;

-- 11. Forecast accuracy on resolved markets
SELECT market_id, brier_score, log_loss, price, outcome
FROM gold_accuracy
ORDER BY brier_score DESC
LIMIT 20;

-- 12. Kalshi markets only
SELECT market_id, question, volume_24h
FROM bronze_markets
WHERE market_id LIKE 'kalshi:%'
ORDER BY volume_24h DESC NULLS LAST
LIMIT 10;

-- 13. Cross-source user PnL
SELECT source, user_id, SUM(total_pnl) AS total
FROM gold_user_pnl
GROUP BY source, user_id
ORDER BY total DESC;

-- 14. Per-market user PnL detail
SELECT market_id, realized_pnl, unrealized_pnl, fees, total_pnl
FROM gold_user_pnl
WHERE user_id = 'YOUR_USER_ID'
ORDER BY total_pnl DESC;

-- 15. Recent Kalshi trades
SELECT market_id, ts, side, price, size
FROM bronze_trades
ORDER BY ts DESC
LIMIT 25;
