"""Connections and setup for the local stack (docker compose: Redpanda + ClickHouse)."""
from __future__ import annotations

import os
import time
from pathlib import Path

import clickhouse_connect
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
TOPIC = "identification_events"
GROUP = "clickhouse_raw_loader"
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
CH = dict(host=os.environ.get("CH_HOST", "localhost"), port=int(os.environ.get("CH_PORT", "8123")),
          username=os.environ.get("CH_USER", "demo"), password=os.environ.get("CH_PASSWORD", "demo"))


def ch(timeout: int = 300):
    return clickhouse_connect.get_client(**CH, settings={"mutations_sync": 2}, send_receive_timeout=timeout)


def run_sql_file(path: Path) -> None:
    client = ch()
    for stmt in path.read_text(encoding="utf-8").split(";"):
        body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip()
        if body:
            client.command(body)


def reset() -> None:
    """Drop everything this demo created and start from an empty topic."""
    client = ch()
    for db in ("raw", "analytics", "analytics_full"):
        client.command(f"DROP DATABASE IF EXISTS {db} SYNC")
    admin = AdminClient({"bootstrap.servers": KAFKA})
    if TOPIC in admin.list_topics(timeout=10).topics:
        admin.delete_topics([TOPIC])[TOPIC].result()
        time.sleep(2)
    try:
        admin.delete_consumer_groups([GROUP])[GROUP].result()
    except Exception:
        pass  # group did not exist yet
    for _ in range(10):
        try:
            admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])[TOPIC].result()
            break
        except Exception:
            time.sleep(1)
    run_sql_file(ROOT / "warehouse" / "raw.sql")


def produce(messages: list[str]) -> int:
    p = Producer({"bootstrap.servers": KAFKA, "linger.ms": 20, "batch.num.messages": 10000,
                  "compression.type": "lz4", "enable.idempotence": True})
    for i, m in enumerate(messages):
        while True:
            try:
                p.produce(TOPIC, m.encode())
                break
            except BufferError:
                p.poll(0.05)
        if i % 10000 == 0:
            p.poll(0)
    left = p.flush(60)
    if left:
        raise RuntimeError(f"{left} messages not acknowledged by the broker")
    return len(messages)


def server_now() -> str:
    return str(ch().command("SELECT toString(now64(3, 'UTC'))"))


def landed_since(since: str) -> tuple[int, int]:
    """Rows written to raw and to the dead letter table since a server timestamp."""
    c = ch()
    raw = c.command(f"SELECT count() FROM raw.identification_events WHERE _ingested_at >= '{since}'")
    dead = c.command(f"SELECT count() FROM raw.dead_letter WHERE _ingested_at >= '{since}'")
    return int(raw), int(dead)
