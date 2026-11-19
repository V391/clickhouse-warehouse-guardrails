select subscription_id, event_date, count() as rows
from {{ ref('mart_customer_daily') }}
group by subscription_id, event_date
having rows > 1
