-- Whale minutely mart must be a subset of the full WC2026 minutely mart.
select
    w.clob_token_id,
    w.odds_timestamp_epoch,
    w.price
from {{ ref('wc2026_whale_minutely_odds') }} as w
left join {{ ref('wc2026_token_minutely_odds') }} as m
    on
        w.clob_token_id = m.clob_token_id
        and w.odds_timestamp_epoch = m.odds_timestamp_epoch
where m.clob_token_id is null

union all

select
    w.clob_token_id,
    w.odds_timestamp_epoch,
    w.price
from {{ ref('wc2026_whale_minutely_odds') }} as w
inner join {{ ref('wc2026_token_minutely_odds') }} as m
    on
        w.clob_token_id = m.clob_token_id
        and w.odds_timestamp_epoch = m.odds_timestamp_epoch
where w.price is distinct from m.price
