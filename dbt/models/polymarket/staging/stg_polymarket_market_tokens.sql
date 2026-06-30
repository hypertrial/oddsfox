select
    mt.market_id,
    cast(je.key as integer) as outcome_index,
    mt.updated_at,
    json_extract_string(je.value, '$') as clob_token_id
from {{ source('polymarket_raw', 'market_tokens') }} as mt,
    lateral json_each(mt.clobtokenids) as je
