{#
  The customer daily mart adds up to staging, day by day. Catches a month that the incremental
  mart forgot to rebuild after a late event changed it.
#}
with s as (
    select event_date, count() as requests
    from {{ ref('stg_identification_events') }}
    group by event_date
),
m as (
    select event_date, sum(requests) as requests
    from {{ ref('mart_customer_daily') }}
    group by event_date
)
select s.event_date, s.requests as staged, m.requests as in_mart
from s full outer join m on s.event_date = m.event_date
where s.requests != m.requests
settings join_use_nulls = 1
