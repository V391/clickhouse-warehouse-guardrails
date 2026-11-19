{#
  One row per identification request: deduplicated, typed, partitioned by event day.

  Incremental strategy: insert_overwrite. A run finds every event day touched by rows that landed
  since the last run (late events can touch days long past) and rebuilds those whole days from raw,
  then swaps them in with REPLACE PARTITION. Re-reading a whole day makes the step idempotent:
  a replayed batch or a late event produces the same day it would have in a full refresh.

  Sort key: (subscription_id, event_date, visitor_id, event_time). Almost every question asked of
  this table is scoped to one customer and a date range, and fraud investigations then narrow to
  one visitor. See bench/ for the measurements behind that choice.

  The dead letter table is declared as an input below. Nothing reads it here, but dbt build only
  lets a test block downstream models when everything the test reads is upstream of them, so
  without this line gate_dead_letter would fail and the marts would still publish.
#}
-- depends_on: {{ source('raw', 'dead_letter') }}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='MergeTree()',
    partition_by='toYYYYMMDD(event_date)',
    order_by=['subscription_id', 'event_date', 'visitor_id', 'event_time'],
) }}

with landed as (
    select *
    from {{ source('raw', 'identification_events') }}
    where visitor_id is not null
      and event_time <= _ingested_at + interval 5 minute
    {% if is_incremental() %}
      and toDate(event_time) in (
          select distinct toDate(event_time)
          from {{ source('raw', 'identification_events') }}
          where _ingested_at > (select max(_ingested_at) from {{ this }})
      )
    {% endif %}
)

select
    request_id,
    assumeNotNull(visitor_id)          as visitor_id,
    toLowCardinality(subscription_id)  as subscription_id,
    event_time,
    toDate(event_time)                 as event_date,
    toIPv4OrDefault(ip)                as ip,
    toLowCardinality(country)          as country,
    toLowCardinality(browser)          as browser,
    toLowCardinality(os)               as os,
    incognito = 1                      as is_incognito,
    confidence,
    toLowCardinality(bot_result)       as bot_result,
    vpn = 1                            as is_vpn,
    linked_id,
    toLowCardinality(url)              as url_path,
    toLowCardinality(sdk_version)      as sdk_version,
    _ingested_at
from landed
-- At-least-once delivery: keep one row per request (the latest delivery; copies are identical).
order by request_id, _ingested_at desc
limit 1 by request_id
