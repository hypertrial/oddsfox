select *
from {{ ref('stg_polymarket_markets') }}
where is_wc2026_target
