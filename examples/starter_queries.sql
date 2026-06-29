SELECT m.question, o.outcome_name, o.token_id
FROM bronze_markets m
JOIN bronze_outcomes o ON m.market_id = o.market_id
WHERE m.active = true
ORDER BY m.volume_24h DESC NULLS LAST
LIMIT 20;

SELECT source, user_id, market_id, total_pnl, realized_pnl, unrealized_pnl, fees
FROM gold_user_pnl
ORDER BY total_pnl DESC
LIMIT 20;
