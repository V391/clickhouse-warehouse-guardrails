-- depends_on: {{ ref('stg_identification_events') }}
{#
  Gate: in the rows this load brought in, no SDK version may fail to return a visitor id for more
  than 2% of its requests on any event day (with at least 1,000 requests to judge by).
  A handful of null visitor ids is normal and is quarantined row by row. A spike concentrated in
  one SDK version is a release bug, and publishing that day would show a fake traffic drop.
#}
with this_load as (
    select request_id, visitor_id, sdk_version, event_time
    from {{ source('raw', 'identification_events') }}
    where _ingested_at >= '{{ var("audit_since") }}'
    order by request_id
    limit 1 by request_id
)
select
    sdk_version,
    toDate(event_time)                        as event_date,
    count()                                   as requests,
    countIf(visitor_id is null)               as null_visitor_ids,
    round(null_visitor_ids / requests, 4)     as null_share
from this_load
group by sdk_version, event_date
having requests >= 1000 and null_share > 0.02
