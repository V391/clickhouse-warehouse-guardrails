{#
  Requests and unique visitors per customer (subscription) per day: the numbers a usage
  dashboard or an invoice check reads.

  Incremental by month: a run rebuilds every month that staging changed in since the last run.
  A month of this mart is small, so rebuilding it whole is cheaper than reasoning about which
  customer days moved.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='MergeTree()',
    partition_by='toYYYYMM(event_date)',
    order_by=['subscription_id', 'event_date'],
) }}

select
    subscription_id,
    event_date,
    count()                               as requests,
    uniqExact(visitor_id)                 as unique_visitors,
    countIf(bot_result = 'bad')           as bad_bot_requests,
    countIf(is_vpn)                       as vpn_requests,
    countIf(is_incognito)                 as incognito_requests,
    round(avg(confidence), 4)             as avg_confidence,
    max(_ingested_at)                     as max_ingested_at
from {{ ref('stg_identification_events') }}
{% if is_incremental() %}
where toStartOfMonth(event_date) in (
    select distinct toStartOfMonth(event_date)
    from {{ ref('stg_identification_events') }}
    where _ingested_at > (select max(max_ingested_at) from {{ this }})
)
{% endif %}
group by subscription_id, event_date
