{#
  Requests held out of the warehouse, with the reason. Kept, not dropped, so the rate can be
  watched and the rows replayed once the cause is fixed.
#}
{{ config(materialized='table', engine='MergeTree()', order_by=['reason', 'request_id']) }}

select
    request_id,
    subscription_id,
    sdk_version,
    event_time,
    _ingested_at,
    multiIf(visitor_id is null, 'null_visitor_id', 'event_time_in_future') as reason
from {{ source('raw', 'identification_events') }}
where visitor_id is null
   or event_time > _ingested_at + interval 5 minute
order by request_id, _ingested_at desc
limit 1 by request_id
