"""Put the benchmark results in ClickHouse and chart them in Superset.

    docker compose --profile bi up -d superset
    python superset/bootstrap.py
    open http://localhost:8088 (admin / admin, local demo only)
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.infra import ch  # noqa: E402

URL = "http://localhost:8088"
LAYOUT = {"events_naive": "naive", "events_tuned": "tuned",
          "events_tuned+bloom": "tuned + bloom index", "rollup": "tuned + daily rollup"}


def load_results() -> None:
    b = json.loads((ROOT / "reports" / "benchmark.json").read_text())
    c = ch()
    c.command("DROP TABLE IF EXISTS bench.results")
    c.command("""CREATE TABLE bench.results (query String, layout String, median_ms Float64,
                 rows_read UInt64, mb_read Float64) ENGINE = MergeTree ORDER BY (query, layout)""")
    rows = [(q.split(":")[0], LAYOUT[v], float(m["ms"]), int(m["rows_read"]), m["bytes_read"] / 1e6)
            for q, vs in b["queries"].items() for v, m in vs.items()]
    c.insert("bench.results", rows, column_names=["query", "layout", "median_ms", "rows_read", "mb_read"])


def api() -> httpx.Client:
    for _ in range(60):
        try:
            if httpx.get(f"{URL}/health", timeout=5).text == "OK":
                break
        except httpx.HTTPError:
            pass
        time.sleep(5)
    token = httpx.post(f"{URL}/api/v1/security/login", json={"username": "admin", "password": "admin",
                                                             "provider": "db", "refresh": True}).json()["access_token"]
    return httpx.Client(base_url=URL, headers={"Authorization": f"Bearer {token}"}, timeout=60)


def get_or_create(s: httpx.Client, kind: str, filter_col: str, value: str, body: dict) -> int:
    q = {"filters": [{"col": filter_col, "opr": "eq", "value": value}]}
    found = s.get(f"/api/v1/{kind}/", params={"q": json.dumps(q)}).json().get("result", [])
    if found:
        return found[0]["id"]
    r = s.post(f"/api/v1/{kind}/", json=body)
    r.raise_for_status()
    return r.json()["id"]


def main() -> None:
    load_results()
    s = api()
    db = get_or_create(s, "database", "database_name", "ClickHouse (local)", {
        "database_name": "ClickHouse (local)",
        "sqlalchemy_uri": "clickhousedb://demo:demo@clickhouse:8123/bench"})
    ds = get_or_create(s, "dataset", "table_name", "results", {"database": db, "schema": "bench", "table_name": "results"})
    metric = {"expressionType": "SQL", "sqlExpression": "sum(mb_read)", "label": "MB read"}
    params = {
        "datasource": f"{ds}__table", "viz_type": "echarts_timeseries_bar",
        "x_axis": "query", "metrics": [metric], "groupby": ["layout"], "adhoc_filters": [],
        "orientation": "horizontal", "logAxis": True, "show_value": True, "show_legend": True,
        "x_axis_sort_asc": True, "row_limit": 100, "truncate_metric": True,
        "y_axis_title": "MB read per query (log scale)", "y_axis_title_position": "Left",
    }
    chart = get_or_create(s, "chart", "slice_name", "Data read per query: naive vs tuned layout", {
        "slice_name": "Data read per query: naive vs tuned layout", "viz_type": "echarts_timeseries_bar",
        "datasource_id": ds, "datasource_type": "table", "params": json.dumps(params)})
    print(f"chart: {URL}/explore/?slice_id={chart}")


if __name__ == "__main__":
    main()
