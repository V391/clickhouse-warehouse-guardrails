"""End to end proof. Needs `docker compose up -d` first.

1. Reset Redpanda and ClickHouse, generate 14 days of sample identification events.
2. Load them through the Prefect flow: a 10 day backfill, then one load per day for 4 days.
3. Rebuild everything from scratch in a second schema and compare row for row (incremental == full).
4. Compare the customer daily mart with the generator's answer key on every customer day.
5. Inject five failures, one load each, and record what the guardrails did.
6. Run the naive vs tuned benchmark (skip with --skip-bench).
7. Write reports/results.md.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from generator.events import START, generate  # noqa: E402
from pipeline import infra  # noqa: E402
from pipeline.flow import load_batch  # noqa: E402

DATA = ROOT / "data"
REPORTS = ROOT / "reports"
BACKFILL_DAYS = 10
MARTS = ("mart_customer_daily", "mart_bot_traffic_daily")


def write_batch(name: str, messages: list[str]) -> Path:
    DATA.mkdir(exist_ok=True)
    p = DATA / f"{name}.jsonl"
    p.write_text("\n".join(messages), encoding="utf-8")
    return p


def run_load(name: str, messages: list[str]) -> dict:
    path = write_batch(name, messages)
    t0 = time.time()
    state = load_batch(str(path), len(messages), return_state=True)
    res = state.result(raise_on_failure=False)
    out = {"load": name, "messages": len(messages), "ok": state.is_completed(), "seconds": round(time.time() - t0)}
    if isinstance(res, Exception):
        out["error"] = f"{type(res).__name__}: {res}"
    else:
        out.update(res)
    print(f"[{name}] ok={out['ok']} {out.get('error', '')}", flush=True)
    return out


def mart_fingerprint(c) -> dict:
    """Row count and content hash of what dashboards read (load bookkeeping columns left out)."""
    return {m: c.query(f"SELECT count(), groupBitXor(cityHash64(*)) FROM "
                       f"(SELECT * EXCEPT (max_ingested_at) FROM analytics.{m})" if m == "mart_customer_daily"
                       else f"SELECT count(), groupBitXor(cityHash64(*)) FROM analytics.{m}").result_rows[0]
            for m in MARTS}


def compare_schemas(c) -> dict:
    out = {}
    for t in ("stg_identification_events", "stg_identification_events_quarantine", *MARTS):
        a = c.command(f"SELECT count() FROM (SELECT * FROM analytics.{t} EXCEPT SELECT * FROM analytics_full.{t})")
        b = c.command(f"SELECT count() FROM (SELECT * FROM analytics_full.{t} EXCEPT SELECT * FROM analytics.{t})")
        out[t] = {"rows": int(c.command(f"SELECT count() FROM analytics.{t}")), "differing_rows": int(a) + int(b)}
    return out


def compare_answer_key(c, key: dict) -> dict:
    rows = c.query("""SELECT subscription_id, toString(event_date), requests, unique_visitors, bad_bot_requests
                      FROM analytics.mart_customer_daily""").result_rows
    got = {f"{s}|{d}": (r, v, b) for s, d, r, v, b in rows}
    want = key["customer_days"]
    mismatched = [k for k in want if got.get(k) != tuple(want[k])] + [k for k in got if k not in want]
    q = dict(c.query("SELECT reason, count() FROM analytics.stg_identification_events_quarantine GROUP BY reason").result_rows)
    return {"customer_days": len(want), "matching": len(want) - len(mismatched), "mismatched": mismatched[:5],
            "staged": int(c.command("SELECT count() FROM analytics.stg_identification_events")),
            "expected_staged": key["valid_events"],
            "quarantined": q, "expected_quarantined": {"null_visitor_id": key["quarantined_null_visitor"],
                                                      "event_time_in_future": key["quarantined_future_time"]}}


def dbt(*args: str) -> int:
    exe = Path(sys.executable).parent / "dbt.exe"
    exe = str(exe) if exe.exists() else "dbt"
    d = str(ROOT / "dbt")
    return subprocess.run([exe, *args, "--project-dir", d, "--profiles-dir", d], cwd=d,
                          capture_output=True, text=True).returncode


def rollback(c, since: str) -> None:
    """Take a blocked load back out, so the next scenario starts from the published state."""
    c.command(f"DELETE FROM raw.identification_events WHERE _ingested_at >= '{since}'")
    c.command(f"DELETE FROM raw.dead_letter WHERE _ingested_at >= '{since}'")
    c.command(f"DELETE FROM analytics.stg_identification_events WHERE _ingested_at >= '{since}'")


def defects(c, last_batch: list[str]) -> list[dict]:
    out = []
    day = START + timedelta(days=14)

    def scenario(name, kind, expect, seed, messages=None, stall=False):
        nonlocal day
        before = mart_fingerprint(c)
        if messages is None:
            messages = generate(seed=seed, days=1, events_per_day=40_000, scenario={0: kind},
                                start=day).batches[0].messages
            day += timedelta(days=1)
        if stall:
            c.command("DETACH TABLE raw.identification_events_mv")
            c.command("DETACH TABLE raw.dead_letter_mv")
        since = infra.server_now()
        r = run_load(f"defect_{kind}", messages)
        err = r.get("error", "")
        if "LoadIncomplete" in err:
            caught_by = "wait_until_landed (load completeness)"
        else:
            caught_by = ", ".join(sorted(set(re.findall(r"gate_\w+", err.split("not published")[0])))) or None
        row = {"scenario": name, "expected": expect, "load_ok": r["ok"], "caught_by": caught_by,
               "marts_unchanged": before == mart_fingerprint(c), "error": r.get("error")}
        if stall:
            # Recovery: reattach, let the consumer catch up from its committed offset, publish.
            c.command("ATTACH TABLE raw.identification_events_mv")
            c.command("ATTACH TABLE raw.dead_letter_mv")
            for _ in range(120):
                landed = sum(infra.landed_since(since))
                if landed >= len(messages):
                    break
                time.sleep(1)
            row["recovered_messages"] = landed
            row["recovered_build_ok"] = dbt("build", "--vars", json.dumps({"audit_since": since})) == 0
            row["published_after_recovery"] = mart_fingerprint(c) != before
        elif not r["ok"]:
            rollback(c, since)
        out.append(row)
        print(f"  -> {row}", flush=True)

    scenario("Consumer group reset: yesterday's batch delivered again", "replay",
             "absorbed: dedupe keeps marts identical", None, messages=last_batch)
    scenario("SDK release fails to identify 25% of its visitors", "null_spike",
             "blocked by gate_null_visitor_rate", 101)
    scenario("Client clock skew: 3% of events stamped a year ahead", "clock_skew",
             "blocked by gate_future_event_rate", 102)
    scenario("Poison message: a word where a number belongs", "poison",
             "dead lettered, blocked by gate_dead_letter", 103)
    scenario("Consumer stalled (materialized views detached)", "stalled",
             "blocked before dbt: not every message landed", 104, stall=True)
    return out


def main() -> None:
    skip_bench = "--skip-bench" in sys.argv
    REPORTS.mkdir(exist_ok=True)
    infra.reset()
    c = infra.ch()
    print("generating sample stream...", flush=True)
    stream = generate()
    key = stream.answer_key

    loads = []
    backfill = [m for b in stream.batches[:BACKFILL_DAYS] for m in b.messages]
    loads.append(run_load("backfill_days_01_10", backfill))
    for b in stream.batches[BACKFILL_DAYS:]:
        loads.append(run_load(f"day_{b.day + 1:02d}", b.messages))
    assert all(l["ok"] for l in loads), "a clean load failed"

    print("full refresh into analytics_full...", flush=True)
    full_ok = dbt("build", "--target", "full", "--full-refresh") == 0
    incremental_vs_full = compare_schemas(c)
    answer = compare_answer_key(c, key)
    print(json.dumps({"incremental_vs_full": incremental_vs_full, "answer_key": answer}, indent=1), flush=True)

    defect_rows = defects(c, stream.batches[-1].messages)

    results = {
        "stream": {k: v for k, v in key.items() if k != "customer_days"} | {
            "messages_sent": sum(len(b.messages) for b in stream.batches), "days": len(stream.batches)},
        "loads": loads, "full_refresh_ok": full_ok, "incremental_vs_full": incremental_vs_full,
        "answer_key": answer, "defects": defect_rows,
        "dbt_checks": loads[-1]["build"]["statuses"],
    }
    (REPORTS / "results.json").write_text(json.dumps(results, indent=2, default=str))

    if not skip_bench:
        subprocess.run([sys.executable, str(ROOT / "bench" / "benchmark.py")], check=True)
    subprocess.run([sys.executable, str(ROOT / "report.py")], check=True)

    ok = (full_ok and all(v["differing_rows"] == 0 for v in incremental_vs_full.values())
          and answer["matching"] == answer["customer_days"] and answer["staged"] == answer["expected_staged"]
          and all(d["caught_by"] or d["expected"].startswith("absorbed") for d in defect_rows)
          and all(d["marts_unchanged"] for d in defect_rows))
    print("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
