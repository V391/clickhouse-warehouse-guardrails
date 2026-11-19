"""Naive vs tuned ClickHouse layout for identification events, measured.

Builds the same N sample rows (default 100M, 90 days, 400 customers with skewed traffic) into:
  naive  : String everywhere, Nullable visitor id, ORDER BY request_id (the "primary key is the id"
           habit from row stores), no partitioning
  tuned  : LowCardinality / IPv4 / Bool / Float32 types, Delta+ZSTD on time, PARTITION BY day,
           ORDER BY (subscription_id, day, visitor_id, event_time)
then adds two optional structures to tuned and measures what each costs and buys:
  + bloom filter skip index on request_id   (support lookups by request id)
  + daily rollup (AggregatingMergeTree, filled by a materialized view) for dashboards

Each query runs once to warm up, then 5 times; numbers are medians from system.query_log.
Every variant must return the same answer as naive (the rollup's approximate unique count is
reported with its error instead).
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.infra import ch  # noqa: E402

ROWS = int(os.environ.get("BENCH_ROWS", 100_000_000))
RUNS = 5
REPORTS = ROOT / "reports"

COLUMNS_NAIVE = """
    request_id String, visitor_id Nullable(String), subscription_id String,
    event_time DateTime64(3, 'UTC'), ip String, country String, browser String, os String,
    incognito UInt8, confidence Float64, bot_result String, vpn UInt8, linked_id String,
    url String, sdk_version String"""

COLUMNS_TUNED = """
    request_id String CODEC(ZSTD(3)), visitor_id String CODEC(ZSTD(3)),
    subscription_id LowCardinality(String), event_time DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
    ip IPv4, country LowCardinality(String), browser LowCardinality(String), os LowCardinality(String),
    incognito Bool, confidence Float32, bot_result LowCardinality(String), vpn Bool,
    linked_id String CODEC(ZSTD(3)), url LowCardinality(String), sdk_version LowCardinality(String)"""

# Deterministic sample rows. Customer traffic is skewed (a cubed uniform), visitors repeat
# (a power law over a pool per customer), bot traffic about 7%.
SOURCE = f"""
SELECT
    concat(toString(toUnixTimestamp64Milli(t)), '.', substring(hex(cityHash64(number, 3)), 1, 6)) AS request_id,
    substring(concat(hex(cityHash64(sub, vk)), hex(cityHash64(vk, sub))), 1, 20)                AS visitor_id,
    concat('sub_', leftPad(toString(sub), 4, '0'))                                                AS subscription_id,
    t                                                                                             AS event_time,
    IPv4NumToString(toUInt32(cityHash64(number, 6) % 4294967295))                                 AS ip,
    ['US','GB','DE','IN','BR','FR','CA','NL','ES','PL','JP','AU','NG','VN','ID'][1 + cityHash64(number, 5) % 15] AS country,
    ['Chrome','Safari','Edge','Firefox','Samsung Internet','Headless Chrome'][1 + cityHash64(number, 7) % 6]      AS browser,
    ['Windows','iOS','Android','macOS','Linux'][1 + cityHash64(number, 8) % 5]                    AS os,
    toUInt8(cityHash64(number, 9) % 100 < 8)                                                      AS incognito,
    round(0.5 + (cityHash64(number, 10) % 500) / 1000, 3)                                         AS confidence,
    multiIf(cityHash64(number, 11) % 100 < 7, 'bad', cityHash64(number, 11) % 100 < 10, 'good', 'notDetected') AS bot_result,
    toUInt8(cityHash64(number, 12) % 100 < 9)                                                     AS vpn,
    concat('acct_', toString(cityHash64(number, 13) % 5000000))                                   AS linked_id,
    ['/login','/signup','/checkout','/password-reset','/account','/promo'][1 + cityHash64(number, 14) % 6] AS url,
    ['3.8.2','3.9.0','3.10.1','3.11.0'][1 + cityHash64(number, 15) % 4]                            AS sdk_version
FROM (
    SELECT number,
           toUInt32(floor(400 * pow((cityHash64(number, 1) % 1000000) / 1e6, 3)))                AS sub,
           toUInt64(floor(200000 * pow((cityHash64(number, 2) % 1000000) / 1e6, 4)))              AS vk,
           addMilliseconds(toDateTime64('2026-07-03 00:00:00', 3, 'UTC'),
                           toInt64(cityHash64(number, 4) % 7776000000))                          AS t
    FROM numbers({ROWS})
)"""

CUSTOMER = "sub_0003"
QUERIES = {
    "Q1 customer dashboard: requests and unique visitors per day, one customer, 30 days": """
        SELECT toDate(event_time) AS day, count() AS requests, uniqExact(visitor_id) AS visitors
        FROM {t} WHERE subscription_id = '{customer}'
          AND event_time >= '2026-09-01 00:00:00' AND event_time < '2026-10-01 00:00:00'
        GROUP BY day ORDER BY day""",
    "Q2 fraud investigation: every request from one visitor of one customer": """
        SELECT event_time, request_id, ip, country, bot_result, url
        FROM {t} WHERE subscription_id = '{customer}' AND visitor_id = '{visitor}'
        ORDER BY event_time, request_id""",
    "Q3 support lookup: one request by request_id": """
        SELECT request_id, visitor_id, subscription_id, event_time, bot_result
        FROM {t} WHERE request_id = '{request}'""",
    "Q4 bot share by country, all customers, last 7 days": """
        SELECT country, count() AS requests, round(countIf(bot_result = 'bad') / requests, 4) AS bad_share
        FROM {t} WHERE event_time >= '2026-09-24 00:00:00' AND event_time < '2026-10-01 00:00:00'
        GROUP BY country ORDER BY country""",
}
ROLLUP_Q1 = """
    SELECT event_date AS day, sum(requests) AS requests, uniqMerge(visitors) AS visitors
    FROM bench.customer_daily_rollup WHERE subscription_id = '{customer}'
      AND event_date >= '2026-09-01' AND event_date < '2026-10-01'
    GROUP BY day ORDER BY day"""


def build(c) -> dict:
    c.command("DROP DATABASE IF EXISTS bench SYNC")
    c.command("CREATE DATABASE bench")
    c.command(f"CREATE TABLE bench.events_naive ({COLUMNS_NAIVE}) ENGINE = MergeTree ORDER BY request_id")
    c.command(f"""CREATE TABLE bench.events_tuned ({COLUMNS_TUNED}) ENGINE = MergeTree
                  PARTITION BY toDate(event_time)
                  ORDER BY (subscription_id, toDate(event_time), visitor_id, event_time)""")
    c.command("""CREATE TABLE bench.customer_daily_rollup (
                    subscription_id LowCardinality(String), event_date Date,
                    requests SimpleAggregateFunction(sum, UInt64),
                    visitors AggregateFunction(uniq, String))
                 ENGINE = AggregatingMergeTree PARTITION BY toYYYYMM(event_date)
                 ORDER BY (subscription_id, event_date)""")
    c.command("""CREATE MATERIALIZED VIEW bench.customer_daily_rollup_mv TO bench.customer_daily_rollup AS
                 SELECT subscription_id, toDate(event_time) AS event_date, count() AS requests,
                        uniqState(visitor_id) AS visitors
                 FROM bench.events_tuned GROUP BY subscription_id, event_date""")
    timings = {}
    s = {"max_insert_threads": 8, "max_threads": 16}
    t0 = time.time()
    c.command(f"INSERT INTO bench.events_naive {SOURCE}", settings=s)
    timings["naive_insert_s"] = round(time.time() - t0, 1)
    t0 = time.time()
    c.command("INSERT INTO bench.events_tuned SELECT * FROM bench.events_naive", settings=s)
    timings["tuned_insert_s"] = round(time.time() - t0, 1)
    for t in ("events_naive", "events_tuned", "customer_daily_rollup"):
        c.command(f"OPTIMIZE TABLE bench.{t} FINAL")
    return timings


def storage(c) -> dict:
    rows = c.query("""SELECT table, sum(rows), sum(data_compressed_bytes), sum(data_uncompressed_bytes),
                             sum(primary_key_bytes_in_memory), count()
                      FROM system.parts WHERE database = 'bench' AND active GROUP BY table""").result_rows
    out = {r[0]: {"rows": r[1], "compressed": r[2], "uncompressed": r[3], "pk_mem": r[4], "parts": r[5]}
           for r in rows}
    idx = c.query("""SELECT sum(data_compressed_bytes) FROM system.data_skipping_indices
                     WHERE database = 'bench' AND table = 'events_tuned'""").result_rows
    out["bloom_index_bytes"] = int(idx[0][0] or 0)
    return out


def measure(c, sql: str) -> dict:
    tag = f"bench-{uuid.uuid4().hex[:10]}"
    settings = {"log_comment": tag, "use_query_cache": 0, "use_uncompressed_cache": 0}
    # Warm up, and keep the answer as text so String and IPv4 columns compare equal.
    result = [tuple(str(v) for v in row) for row in c.query(sql, settings=settings).result_rows]
    for _ in range(RUNS):
        c.query(sql, settings=settings)
    c.command("SYSTEM FLUSH LOGS")
    log = c.query(f"""SELECT query_duration_ms, read_rows, read_bytes, memory_usage FROM system.query_log
                      WHERE log_comment = '{tag}' AND type = 'QueryFinish' ORDER BY event_time_microseconds""").result_rows[1:]
    return {"ms": statistics.median(r[0] for r in log), "rows_read": statistics.median(r[1] for r in log),
            "bytes_read": statistics.median(r[2] for r in log), "memory": statistics.median(r[3] for r in log),
            "result": result}


def already_built(c) -> bool:
    try:
        n = c.command("SELECT count() FROM bench.events_naive")
        t = c.command("SELECT count() FROM bench.events_tuned")
        return int(n) == ROWS and int(t) == ROWS
    except Exception:
        return False


def main() -> None:
    c = ch(timeout=7200)
    REPORTS.mkdir(exist_ok=True)
    if "--reuse" in sys.argv and already_built(c):
        print(f"reusing the {ROWS:,} rows already in bench", flush=True)
        c.command("ALTER TABLE bench.events_tuned DROP INDEX IF EXISTS idx_request_id")
        for t in ("events_naive", "events_tuned", "customer_daily_rollup"):
            c.command(f"OPTIMIZE TABLE bench.{t} FINAL")
        prev = REPORTS / "benchmark.json"
        timings = json.loads(prev.read_text())["timings"] if prev.exists() else {}
    else:
        print(f"building {ROWS:,} rows into naive and tuned tables...", flush=True)
        timings = build(c)
    visitor = c.command(f"""SELECT visitor_id FROM bench.events_tuned WHERE subscription_id = '{CUSTOMER}'
                            GROUP BY visitor_id ORDER BY count() DESC, visitor_id LIMIT 1 OFFSET 50""")
    request = c.command("SELECT request_id FROM bench.events_naive ORDER BY cityHash64(request_id) LIMIT 1")
    params = {"customer": CUSTOMER, "visitor": visitor, "request": request}

    results: dict[str, dict] = {}
    for name, q in QUERIES.items():
        results[name] = {}
        for t in ("events_naive", "events_tuned"):
            results[name][t] = measure(c, q.format(t=f"bench.{t}", **params))
            print(f"  {name[:2]} {t}: {results[name][t]['ms']} ms", flush=True)
    store_before_index = storage(c)

    c.command("ALTER TABLE bench.events_tuned ADD INDEX idx_request_id request_id TYPE bloom_filter(0.01) GRANULARITY 4")
    c.command("ALTER TABLE bench.events_tuned MATERIALIZE INDEX idx_request_id", settings={"mutations_sync": 2})
    q3 = next(k for k in QUERIES if k.startswith("Q3"))
    results[q3]["events_tuned+bloom"] = measure(c, QUERIES[q3].format(t="bench.events_tuned", **params))
    q1 = next(k for k in QUERIES if k.startswith("Q1"))
    results[q1]["rollup"] = measure(c, ROLLUP_Q1.format(**params))
    store = storage(c)

    # Same answers everywhere (rollup: same request counts, approximate visitors).
    for name, variants in results.items():
        base = variants["events_naive"]["result"]
        for v, r in variants.items():
            if v == "rollup":
                assert [x[:2] for x in r["result"]] == [x[:2] for x in base], "rollup request counts differ"
                errs = [abs(int(a[2]) - int(b[2])) / int(b[2]) for a, b in zip(r["result"], base)]
                r["visitor_error_max"] = max(errs)
            else:
                assert r["result"] == base, f"{name}: {v} returned a different answer"
    for variants in results.values():
        for r in variants.values():
            r["result_rows"] = len(r.pop("result"))

    out = {"rows": ROWS, "customer": CUSTOMER, "timings": timings, "storage": store,
           "storage_before_index": store_before_index, "queries": results,
           "clickhouse": c.command("SELECT version()")}
    (REPORTS / "benchmark.json").write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps({k: {v: (r["ms"], r["rows_read"]) for v, r in vs.items()} for k, vs in results.items()}, indent=1))


if __name__ == "__main__":
    main()
