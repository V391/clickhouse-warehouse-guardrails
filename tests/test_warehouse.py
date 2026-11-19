"""Checks against the warehouse that run_all.py built. Skipped if the stack is not running.

They read analytics_full (the from-scratch build), which the failure scenarios never touch, and
compare it with an answer key regenerated here from the same seed.
"""
import pytest

from generator.events import generate

infra = pytest.importorskip("pipeline.infra")


@pytest.fixture(scope="module")
def c():
    try:
        client = infra.ch()
        client.command("SELECT count() FROM analytics_full.mart_customer_daily")
    except Exception:
        pytest.skip("ClickHouse with a completed run_all.py is not available")
    return client


@pytest.fixture(scope="module")
def key():
    return generate().answer_key


def test_staging_has_every_valid_request_once(c, key):
    assert c.command("SELECT count() FROM analytics_full.stg_identification_events") == key["valid_events"]
    assert c.command("SELECT count() - uniqExact(request_id) FROM analytics_full.stg_identification_events") == 0


def test_quarantine_matches_the_planted_problems(c, key):
    got = dict(c.query("SELECT reason, count() FROM analytics_full.stg_identification_events_quarantine "
                       "GROUP BY reason").result_rows)
    assert got == {"null_visitor_id": key["quarantined_null_visitor"],
                   "event_time_in_future": key["quarantined_future_time"]}


def test_customer_daily_mart_matches_answer_key(c, key):
    rows = c.query("SELECT subscription_id, toString(event_date), requests, unique_visitors, bad_bot_requests "
                   "FROM analytics_full.mart_customer_daily").result_rows
    got = {f"{s}|{d}": (r, v, b) for s, d, r, v, b in rows}
    assert got == {k: tuple(v) for k, v in key["customer_days"].items()}


def test_staging_is_partitioned_by_day_and_sorted_for_customer_queries(c):
    ddl = c.command("SHOW CREATE TABLE analytics.stg_identification_events")
    assert "PARTITION BY toYYYYMMDD(event_date)" in ddl
    assert "ORDER BY (subscription_id, event_date, visitor_id, event_time)" in ddl
