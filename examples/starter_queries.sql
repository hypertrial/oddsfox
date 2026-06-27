SELECT m.question, o.outcome_name, o.token_id
FROM bronze_markets m
JOIN bronze_outcomes o ON m.market_id = o.market_id
WHERE m.active = true
ORDER BY m.volume_24h DESC NULLS LAST
LIMIT 20;
