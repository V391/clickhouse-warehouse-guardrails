import json
from collections import Counter

from generator.events import SDK_VERSIONS, generate

SMALL = dict(days=4, events_per_day=6000)


def test_same_seed_same_stream():
    a, b = generate(seed=3, **SMALL), generate(seed=3, **SMALL)
    assert [x.messages for x in a.batches] == [x.messages for x in b.batches]


def test_answer_key_accounts_for_every_unique_event():
    s = generate(**SMALL)
    k = s.answer_key
    unique = sum(b.unique_events for b in s.batches)
    assert k["valid_events"] + k["quarantined_null_visitor"] + k["quarantined_future_time"] == unique
    assert sum(v[0] for v in k["customer_days"].values()) == k["valid_events"]


def test_duplicates_are_exact_copies_within_the_batch():
    s = generate(**SMALL)
    for b in s.batches:
        counts = Counter(b.messages)
        assert sum(c - 1 for c in counts.values()) == b.duplicates
        assert len(counts) == b.unique_events


def test_late_events_arrive_after_their_day():
    s = generate(**SMALL)
    assert s.answer_key["late_events"] > 0
    first = s.batches[0]
    assert first.late_events == 0  # nothing can be late on the first day


def test_replay_resends_the_previous_day():
    s = generate(scenario={2: "replay"}, **SMALL)
    assert set(s.batches[1].messages) <= set(s.batches[2].messages)
    assert s.batches[2].duplicates >= s.batches[1].unique_events


def test_poison_message_has_a_word_for_a_number():
    s = generate(scenario={1: "poison"}, **SMALL)
    bad = [m for m in s.batches[1].messages if '"confidence":"high"' in m]
    assert len(bad) == 1


def test_null_spike_is_concentrated_in_one_sdk_version():
    s = generate(scenario={1: "null_spike"}, **SMALL)
    rows = [json.loads(m) for m in set(s.batches[1].messages)]
    day = [r for r in rows if r["event_time"].startswith("2026-10-02")]
    share = {v: sum(r["visitor_id"] is None for r in day if r["sdk_version"] == v) /
             max(1, sum(r["sdk_version"] == v for r in day)) for v in SDK_VERSIONS}
    assert share["3.11.0"] > 0.15
    assert all(share[v] < 0.02 for v in SDK_VERSIONS if v != "3.11.0")
