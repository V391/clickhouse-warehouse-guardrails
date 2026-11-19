"""Sample device identification events, shaped like a fraud prevention API's request log.

Everything here is invented and seeded. Field names follow the shape of a public identification
API response (visitor id, request id, confidence, bot result, incognito, VPN, linked id), not any
company's internal schema.

The stream has the problems a real at-least-once pipeline has:
  * duplicates: about 1.5% of messages are delivered twice (producer retries)
  * late events: about 2% of events arrive 1 to 4 days after they happened (SDK offline buffer)
  * null visitor ids: about 0.3% of events, spread across SDK versions (identification failed)
  * clock skew: about 0.05% of events carry a device clock far in the future

The answer key (what the warehouse should end up with) is computed here, in plain Python,
independently of ClickHouse and dbt.
"""
from __future__ import annotations

import json
import string
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np

START = datetime(2026, 10, 1, tzinfo=timezone.utc)
DAYS = 14
EVENTS_PER_DAY = 120_000
SUBSCRIPTIONS = 400
DUP_RATE = 0.015
LATE_RATE = 0.02
NULL_VISITOR_RATE = 0.003
FUTURE_RATE = 0.0005

COUNTRIES = ["US", "GB", "DE", "IN", "BR", "FR", "CA", "NL", "ES", "PL", "JP", "AU", "NG", "VN", "ID"]
COUNTRY_P = [0.30, 0.09, 0.08, 0.10, 0.07, 0.05, 0.04, 0.03, 0.04, 0.03, 0.03, 0.03, 0.04, 0.04, 0.03]
BROWSERS = ["Chrome", "Safari", "Edge", "Firefox", "Samsung Internet", "Headless Chrome"]
BROWSER_P = [0.58, 0.24, 0.07, 0.05, 0.04, 0.02]
OSES = ["Windows", "iOS", "Android", "macOS", "Linux"]
OS_P = [0.32, 0.25, 0.28, 0.11, 0.04]
SDK_VERSIONS = ["3.8.2", "3.9.0", "3.10.1", "3.11.0"]
SDK_P = [0.15, 0.30, 0.40, 0.15]
URLS = ["/login", "/signup", "/checkout", "/password-reset", "/account", "/promo"]
URL_P = [0.40, 0.15, 0.20, 0.05, 0.15, 0.05]
ALNUM = np.array(list(string.ascii_letters + string.digits))


@dataclass
class Batch:
    day: int
    messages: list[str]          # JSON lines, in send order, duplicates included
    unique_events: int
    duplicates: int
    late_events: int


@dataclass
class Stream:
    batches: list[Batch]
    answer_key: dict = field(default_factory=dict)


def _ids(rng: np.random.Generator, n: int, length: int) -> np.ndarray:
    chars = ALNUM[rng.integers(0, len(ALNUM), size=(n, length))]
    return np.array(["".join(row) for row in chars.tolist()], dtype=object)


def _subscription_weights(rng: np.random.Generator) -> np.ndarray:
    # A few customers send most of the traffic (Zipf-like), as with any usage based API.
    w = 1.0 / np.arange(1, SUBSCRIPTIONS + 1) ** 1.1
    rng.shuffle(w)
    return w / w.sum()


def generate(seed: int = 7, days: int = DAYS, events_per_day: int = EVENTS_PER_DAY,
             scenario: dict[int, str] | None = None, start: datetime = START) -> Stream:
    """Build the full stream, then cut it into one Kafka batch per delivery day.

    scenario maps a delivery day to a defect to inject into that day's batch:
      "null_spike" : SDK 3.11.0 fails to identify 25% of its visitors that day
      "clock_skew" : 3% of that day's events carry a timestamp 400 days in the future
      "replay"     : the consumer group is reset and the previous day's batch is delivered again
      "poison"     : one malformed message (a word where a number belongs) is added to the batch
    """
    scenario = scenario or {}
    rng = np.random.default_rng(seed)
    sub_ids = np.array([f"sub_{i:04d}" for i in range(SUBSCRIPTIONS)], dtype=object)
    sub_w = _subscription_weights(rng)
    # Visitor pools: bigger customers have more distinct visitors; visitors return across days.
    pool_size = np.maximum(50, (sub_w * events_per_day * days * 0.35).astype(int))
    pool_offset = np.concatenate([[0], np.cumsum(pool_size)[:-1]])
    visitor_pool = _ids(rng, int(pool_size.sum()), 20)

    n = days * events_per_day
    event_day = np.repeat(np.arange(days), events_per_day)
    ms_in_day = rng.integers(0, 86_400_000, size=n)
    sub_idx = rng.choice(SUBSCRIPTIONS, size=n, p=sub_w)
    # Returning visitors: low ranks in each pool are much more likely.
    rank = np.minimum((rng.pareto(1.2, size=n) * 3).astype(int), pool_size[sub_idx] - 1)
    visitor = visitor_pool[pool_offset[sub_idx] + rank]

    sdk = rng.choice(len(SDK_VERSIONS), size=n, p=SDK_P)
    null_visitor = rng.random(n) < NULL_VISITOR_RATE
    future = rng.random(n) < FUTURE_RATE
    lateness = np.where(rng.random(n) < LATE_RATE, rng.integers(1, 5, size=n), 0)
    delivery_day = np.minimum(event_day + lateness, days - 1)

    for day, kind in scenario.items():
        on_day = event_day == day
        if kind == "null_spike":
            null_visitor |= on_day & (sdk == SDK_VERSIONS.index("3.11.0")) & (rng.random(n) < 0.25)
        elif kind == "clock_skew":
            future |= on_day & (rng.random(n) < 0.03)

    epoch_ms = int(start.timestamp() * 1000) + event_day.astype(np.int64) * 86_400_000 + ms_in_day
    epoch_ms = np.where(future, epoch_ms + 400 * 86_400_000, epoch_ms)
    request_id = np.array([f"{t}.{s}" for t, s in zip(epoch_ms, _ids(rng, n, 6))], dtype=object)

    bot = rng.choice(["notDetected", "good", "bad"], size=n, p=[0.90, 0.03, 0.07])
    browser = rng.choice(BROWSERS, size=n, p=BROWSER_P)
    bot = np.where(browser == "Headless Chrome", "bad", bot)
    country = rng.choice(COUNTRIES, size=n, p=COUNTRY_P)
    os_ = rng.choice(OSES, size=n, p=OS_P)
    incognito = (rng.random(n) < 0.08).astype(int)
    vpn = (rng.random(n) < np.where(bot == "bad", 0.45, 0.06)).astype(int)
    confidence = np.round(np.clip(rng.beta(30, 1.2, size=n), 0.5, 1.0), 3)
    url = rng.choice(URLS, size=n, p=URL_P)
    ip = np.array([f"{a}.{b}.{c}.{d}" for a, b, c, d in rng.integers(1, 255, size=(n, 4))], dtype=object)
    linked = rng.integers(0, 5_000_000, size=n)

    def to_json(i: int) -> str:
        ts = datetime.fromtimestamp(epoch_ms[i] / 1000, tz=timezone.utc)
        return json.dumps({
            "request_id": request_id[i],
            "visitor_id": None if null_visitor[i] else visitor[i],
            "subscription_id": sub_ids[sub_idx[i]],
            "event_time": ts.strftime("%Y-%m-%d %H:%M:%S.") + f"{epoch_ms[i] % 1000:03d}",
            "ip": ip[i], "country": country[i], "browser": browser[i], "os": os_[i],
            "incognito": int(incognito[i]), "confidence": float(confidence[i]),
            "bot_result": bot[i], "vpn": int(vpn[i]), "linked_id": f"acct_{linked[i]}",
            "url": url[i], "sdk_version": SDK_VERSIONS[sdk[i]],
        }, separators=(",", ":"))

    batches: list[Batch] = []
    previous: list[str] = []
    for d in range(days):
        idx = np.flatnonzero(delivery_day == d)
        msgs = [to_json(i) for i in idx]
        dup_idx = rng.choice(len(msgs), size=int(len(msgs) * DUP_RATE), replace=False)
        dups = [msgs[i] for i in dup_idx]
        if scenario.get(d) == "replay":
            dups += previous
        out = msgs + dups
        if scenario.get(d) == "poison":
            # One producer ships a record whose confidence is a word, not a number.
            bad = json.loads(msgs[0])
            bad["request_id"] += "x"
            bad["confidence"] = "high"
            out.append(json.dumps(bad, separators=(",", ":")))
        order = rng.permutation(len(out))
        batches.append(Batch(day=d, messages=[out[i] for i in order], unique_events=len(msgs),
                             duplicates=len(dups),
                             late_events=int((delivery_day[idx] > event_day[idx]).sum())))
        previous = msgs

    # Answer key: what staging and the daily customer mart must contain.
    valid = ~null_visitor & ~future
    key: dict[tuple[str, str], dict] = defaultdict(lambda: {"requests": 0, "visitors": set(), "bad_bot": 0})
    for i in np.flatnonzero(valid):
        day = (start + timedelta(days=int(event_day[i]))).strftime("%Y-%m-%d")
        k = key[(sub_ids[sub_idx[i]], day)]
        k["requests"] += 1
        k["visitors"].add(visitor[i])
        k["bad_bot"] += bot[i] == "bad"
    answer = {
        "valid_events": int(valid.sum()),
        "quarantined_null_visitor": int(null_visitor.sum()),
        "quarantined_future_time": int((future & ~null_visitor).sum()),
        "late_events": int((delivery_day > event_day).sum()),
        "duplicates_sent": sum(b.duplicates for b in batches),
        "customer_days": {f"{s}|{d}": (v["requests"], len(v["visitors"]), v["bad_bot"])
                          for (s, d), v in key.items()},
    }
    return Stream(batches=batches, answer_key=answer)
