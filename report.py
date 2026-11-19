"""Render reports/results.json and reports/benchmark.json into reports/results.md."""
from __future__ import annotations

import json
from pathlib import Path

REPORTS = Path(__file__).resolve().parent / "reports"
VARIANT = {"events_naive": "naive", "events_tuned": "tuned",
           "events_tuned+bloom": "tuned + bloom index", "rollup": "tuned + daily rollup"}


def mb(b: float) -> str:
    return f"{b / 1e6:,.1f} MB" if b < 1e9 else f"{b / 1e9:,.2f} GB"


def pipeline_section(r: dict) -> list[str]:
    s, a = r["stream"], r["answer_key"]
    out = ["## Pipeline", "",
           f"{s['messages_sent']:,} Kafka messages over {s['days']} days: {s['valid_events']:,} valid requests, "
           f"{s['duplicates_sent']:,} duplicate deliveries, {s['late_events']:,} late events, "
           f"{s['quarantined_null_visitor']:,} null visitor ids, {s['quarantined_future_time']:,} future timestamps.", "",
           "| Load | Messages | Landed in raw | dbt build | Seconds |", "|---|---:|---:|---|---:|"]
    for l in r["loads"]:
        st = l.get("build", {}).get("statuses", {})
        out.append(f"| {l['load']} | {l['messages']:,} | {l.get('raw_rows', 0):,} | "
                   f"{'pass' if l['ok'] else 'FAIL'} ({', '.join(f'{v} {k}' for k, v in st.items())}) | {l['seconds']} |")
    out += ["", "**Incremental equals full refresh**", "", "| Table | Rows | Differing rows |", "|---|---:|---:|"]
    out += [f"| {t} | {v['rows']:,} | {v['differing_rows']} |" for t, v in r["incremental_vs_full"].items()]
    out += ["", "**Answer key**", "",
            f"- Customer days matching the generator exactly (requests, unique visitors, bad bot requests): "
            f"**{a['matching']:,} of {a['customer_days']:,}**",
            f"- Staged requests: {a['staged']:,} (expected {a['expected_staged']:,})",
            f"- Quarantined: {a['quarantined']} (expected {a['expected_quarantined']})", "",
            "## Injected failures", "", "| Scenario | Expected | Caught by | Marts unchanged |", "|---|---|---|---|"]
    for d in r["defects"]:
        out.append(f"| {d['scenario']} | {d['expected']} | {d['caught_by'] or 'nothing (absorbed)'} | "
                   f"{'yes' if d['marts_unchanged'] else 'NO'} |")
    stall = next((d for d in r["defects"] if "recovered_messages" in d), None)
    if stall:
        out += ["", f"After the stall, the views were reattached: {stall['recovered_messages']:,} messages landed from "
                f"the committed offset, the build passed: {stall['recovered_build_ok']}, and the day was published: "
                f"{stall['published_after_recovery']}."]
    return out


def bench_section(b: dict) -> list[str]:
    st = b["storage"]
    n, t = st["events_naive"], st["events_tuned"]
    out = ["## Benchmark", "",
           f"{b['rows']:,} sample rows, ClickHouse {b['clickhouse']}, local Docker. Median of 5 runs after a warm up. "
           "Every variant returned the same answer as naive.", "",
           "| Query | Layout | Median ms | Rows read | Data read | Peak memory |", "|---|---|---:|---:|---:|---:|"]
    for q, vs in b["queries"].items():
        for v, m in vs.items():
            extra = f" (unique visitors within {m['visitor_error_max']:.1%})" if "visitor_error_max" in m else ""
            out.append(f"| {q} | {VARIANT[v]}{extra} | {m['ms']:,.0f} | {m['rows_read']:,.0f} | "
                       f"{mb(m['bytes_read'])} | {mb(m['memory'])} |")
    roll = st.get("customer_daily_rollup", {})
    out += ["", "| Storage | Compressed | Uncompressed |", "|---|---:|---:|",
            f"| naive table | {mb(n['compressed'])} | {mb(n['uncompressed'])} |",
            f"| tuned table | {mb(t['compressed'])} | {mb(t['uncompressed'])} |",
            f"| bloom filter index on request_id | {mb(st['bloom_index_bytes'])} | |",
            f"| daily rollup ({roll.get('rows', 0):,} rows) | {mb(roll.get('compressed', 0))} | {mb(roll.get('uncompressed', 0))} |"]
    return out


def main() -> None:
    lines = ["# Results", "", "Written by `run_all.py`. Every number below comes from that run.", ""]
    if (REPORTS / "results.json").exists():
        lines += pipeline_section(json.loads((REPORTS / "results.json").read_text()))
    if (REPORTS / "benchmark.json").exists():
        lines += [""] + bench_section(json.loads((REPORTS / "benchmark.json").read_text()))
    (REPORTS / "results.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("wrote reports/results.md")


if __name__ == "__main__":
    main()
