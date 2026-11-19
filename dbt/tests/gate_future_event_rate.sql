-- depends_on: {{ ref('stg_identification_events') }}
{#
  Gate: no more than 1% of this load's requests may carry an event_time in the future.
  Single bad device clocks are quarantined. A burst means a client or SDK is stamping times
  wrongly, and partitions keyed on event day would start filling with garbage dates.
#}
with this_load as (
    select request_id, event_time, _ingested_at
    from {{ source('raw', 'identification_events') }}
    where _ingested_at >= '{{ var("audit_since") }}'
    order by request_id
    limit 1 by request_id
)
select
    count()                                                         as requests,
    countIf(event_time > _ingested_at + interval 5 minute)          as future_events,
    round(future_events / requests, 4)                              as future_share
from this_load
having requests > 0 and future_share > 0.01
