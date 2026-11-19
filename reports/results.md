# Results

Written by `run_all.py`. Every number below comes from that run.

## Pipeline

1,705,193 Kafka messages over 14 days: 1,674,082 valid requests, 25,193 duplicate deliveries, 31,121 late events, 5,112 null visitor ids, 806 future timestamps.

| Load | Messages | Landed in raw | dbt build | Seconds |
|---|---:|---:|---|---:|
| backfill_days_01_10 | 1,211,893 | 1,211,893 | pass (4 success, 14 pass) | 62 |
| day_11 | 121,873 | 121,873 | pass (4 success, 14 pass) | 33 |
| day_12 | 121,793 | 121,793 | pass (4 success, 14 pass) | 35 |
| day_13 | 121,796 | 121,796 | pass (4 success, 14 pass) | 29 |
| day_14 | 127,838 | 127,838 | pass (4 success, 14 pass) | 34 |

**Incremental equals full refresh**

| Table | Rows | Differing rows |
|---|---:|---:|
| stg_identification_events | 1,674,082 | 0 |
| stg_identification_events_quarantine | 5,918 | 0 |
| mart_customer_daily | 5,600 | 0 |
| mart_bot_traffic_daily | 210 | 0 |

**Answer key**

- Customer days matching the generator exactly (requests, unique visitors, bad bot requests): **5,600 of 5,600**
- Staged requests: 1,674,082 (expected 1,674,082)
- Quarantined: {'null_visitor_id': 5112, 'event_time_in_future': 806} (expected {'null_visitor_id': 5112, 'event_time_in_future': 806})

## Injected failures

| Scenario | Expected | Caught by | Marts unchanged |
|---|---|---|---|
| Consumer group reset: yesterday's batch delivered again | absorbed: dedupe keeps marts identical | nothing (absorbed) | yes |
| SDK release fails to identify 25% of its visitors | blocked by gate_null_visitor_rate | gate_null_visitor_rate | yes |
| Client clock skew: 3% of events stamped a year ahead | blocked by gate_future_event_rate | gate_future_event_rate | yes |
| Poison message: a word where a number belongs | dead lettered, blocked by gate_dead_letter | gate_dead_letter | yes |
| Consumer stalled (materialized views detached) | blocked before dbt: not every message landed | wait_until_landed (load completeness) | yes |

After the stall, the views were reattached: 40,600 messages landed from the committed offset, the build passed: True, and the day was published: True.

## Benchmark

100,000,000 sample rows, ClickHouse 24.8.14.39, local Docker. Median of 5 runs after a warm up. Every variant returned the same answer as naive.

| Query | Layout | Median ms | Rows read | Data read | Peak memory |
|---|---|---:|---:|---:|---:|
| Q1 customer dashboard: requests and unique visitors per day, one customer, 30 days | naive | 2,885 | 100,000,000 | 3.50 GB | 43.3 MB |
| Q1 customer dashboard: requests and unique visitors per day, one customer, 30 days | tuned | 155 | 981,701 | 37.3 MB | 17.6 MB |
| Q1 customer dashboard: requests and unique visitors per day, one customer, 30 days | tuned + daily rollup (unique visitors within 0.0%) | 219 | 8,192 | 0.0 MB | 39.5 MB |
| Q2 fraud investigation: every request from one visitor of one customer | naive | 3,929 | 100,000,000 | 5.35 GB | 93.7 MB |
| Q2 fraud investigation: every request from one visitor of one customer | tuned | 187 | 2,217,493 | 72.7 MB | 0.5 MB |
| Q3 support lookup: one request by request_id | naive | 8 | 8,192 | 0.8 MB | 0.0 MB |
| Q3 support lookup: one request by request_id | tuned | 2,159 | 100,000,000 | 2.90 GB | 0.4 MB |
| Q3 support lookup: one request by request_id | tuned + bloom index | 57 | 775,464 | 22.7 MB | 0.1 MB |
| Q4 bot share by country, all customers, last 7 days | naive | 539 | 100,000,000 | 1.04 GB | 6.8 MB |
| Q4 bot share by country, all customers, last 7 days | tuned | 132 | 7,776,124 | 77.8 MB | 5.7 MB |

| Storage | Compressed | Uncompressed |
|---|---:|---:|
| naive table | 7.42 GB | 14.37 GB |
| tuned table | 3.90 GB | 7.99 GB |
| bloom filter index on request_id | 125.5 MB | |
| daily rollup (36,000 rows) | 298.1 MB | 297.2 MB |
