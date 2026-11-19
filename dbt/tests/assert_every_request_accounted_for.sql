{#
  Every distinct request that landed in raw is either in staging or in quarantine, exactly once.
  Catches an incremental run that skipped a day, and a dedupe that dropped real requests.
#}
with raw_ids as (
    select uniqExact(request_id) as n from {{ source('raw', 'identification_events') }}
),
kept as (select count() as n from {{ ref('stg_identification_events') }}),
held as (select count() as n from {{ ref('stg_identification_events_quarantine') }})
select raw_ids.n as raw_requests, kept.n as staged, held.n as quarantined
from raw_ids cross join kept cross join held
where raw_ids.n != kept.n + held.n
