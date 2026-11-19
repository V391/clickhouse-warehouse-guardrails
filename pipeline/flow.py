"""Prefect flow: one Kafka batch in, one audited dbt build out.

    send batch -> wait until every message has landed (raw or dead letter) -> source freshness
    -> dbt build with this load's audit window -> fail the run if any gate fails

dbt build runs each model's tests before anything downstream of it, so a failed gate on staging
leaves the marts (what dashboards read) exactly as they were before this load.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

from pipeline import infra

DBT_DIR = infra.ROOT / "dbt"


class LoadIncomplete(Exception):
    pass


class GateFailed(Exception):
    pass


@task(retries=2, retry_delay_seconds=3)
def send_batch(path: str) -> int:
    messages = Path(path).read_text(encoding="utf-8").splitlines()
    return infra.produce(messages)


@task(retries=6, retry_delay_seconds=5)
def wait_until_landed(since: str, expected: int) -> dict:
    raw, dead = infra.landed_since(since)
    if raw + dead < expected:
        raise LoadIncomplete(f"{raw + dead:,} of {expected:,} messages landed in ClickHouse")
    return {"raw_rows": raw, "dead_letter": dead}


def _dbt_exe() -> str:
    scripts = Path(sys.executable).parent
    return next((str(scripts / n) for n in ("dbt.exe", "dbt") if (scripts / n).exists()), "dbt")


@task
def dbt(args: list[str], target: str = "dev") -> dict:
    log = get_run_logger()
    cmd = [_dbt_exe(), *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "--target", target]
    r = subprocess.run(cmd, cwd=DBT_DIR, capture_output=True, text=True)
    results = json.loads((DBT_DIR / "target" / ("sources.json" if args[:2] == ["source", "freshness"]
                                                 else "run_results.json")).read_text())["results"]
    by_status: dict[str, list[str]] = {}
    for res in results:
        by_status.setdefault(res["status"], []).append(res["unique_id"].split(".")[-1]
                                                         if not res["unique_id"].startswith("test.")
                                                         else res["unique_id"].split(".")[2])
    summary = {"ok": r.returncode == 0, "statuses": {k: len(v) for k, v in by_status.items()},
               "failed": sorted(set(by_status.get("fail", []) + by_status.get("error", []))),
               "skipped": sorted(set(by_status.get("skipped", [])))}
    log.info("dbt %s -> %s", " ".join(args[:1]), summary["statuses"])
    if r.returncode and not summary["failed"]:
        log.error(r.stdout[-3000:])
    return summary


@flow(name="identification-events-load")
def load_batch(batch_path: str, expected_messages: int, target: str = "dev") -> dict:
    since = infra.server_now()
    sent = send_batch(batch_path)
    landed = wait_until_landed(since, expected_messages or sent)
    freshness = dbt(["source", "freshness"], target)
    build = dbt(["build", "--vars", json.dumps({"audit_since": since})], target)
    out = {"since": since, "sent": sent, **landed, "freshness_ok": freshness["ok"], "build": build}
    if not build["ok"]:
        raise GateFailed(f"gates failed: {build['failed']}; not published: {build['skipped']}")
    return out
