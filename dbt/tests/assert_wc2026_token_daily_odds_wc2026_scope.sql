-- wc2026_token_daily_odds must only include WC2026-scoped tokens.
select
    m.clob_token_id,
    m.market_id
from {{ ref('wc2026_token_daily_odds') }} as m
left join {{ ref('int_polymarket_wc2026_token_universe') }} as t
    on m.clob_token_id = t.clob_token_id
where t.clob_token_id is null
