{{ config(engine='MergeTree()', order_by=['event_date', 'country']) }}

select
    event_date,
    country,
    count()                                     as requests,
    countIf(bot_result = 'bad')                 as bad_bot_requests,
    round(bad_bot_requests / requests, 4)       as bad_bot_share,
    round(countIf(is_vpn) / requests, 4)        as vpn_share
from {{ ref('stg_identification_events') }}
group by event_date, country
