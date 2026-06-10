
"""The log is the foundation, so test it hardest.

These run against BOTH backends via parametrization — proving the SQLite and
in-memory implementations are behaviorally identical (the ports & adapters
contract).
"""

import pytest

from app.event_log import InMemoryEventLog, SqliteEventLog
from app.schemas import make_event


@pytest.fixture(params=["memory", "sqlite"])
def log(request, tmp_path):
    if request.param == "memory":
        return InMemoryEventLog()
    return SqliteEventLog(tmp_path / "events.db")


async def test_seq_is_monotonic_from_one(log):
    for i in range(5):
        stored = await log.append("run_a", make_event("run_a", "progress", {"i": i}))
        assert stored["seq"] == i + 1          # starts at 1, strictly increasing
        assert stored["received_at"] > 0
    assert await log.latest_seq("run_a") == 5


async def test_read_since_returns_only_the_tail(log):
    for i in range(5):
        await log.append("run_a", make_event("run_a", "progress", {"i": i}))

    tail = await log.read_since("run_a", 3)
    assert [e["seq"] for e in tail] == [4, 5]   # only seq > 3
    assert await log.read_since("run_a", 5) == []  # nothing past the head


async def test_runs_have_independent_sequences(log):
    await log.append("run_a", make_event("run_a", "progress"))
    await log.append("run_b", make_event("run_b", "progress"))
    await log.append("run_a", make_event("run_a", "progress"))

    assert await log.latest_seq("run_a") == 2
    assert await log.latest_seq("run_b") == 1


async def test_payload_roundtrips(log):
    payload = {"stage": "load", "rows": 1234, "ok": True}
    await log.append("run_a", make_event("run_a", "stage_completed", payload))
    [event] = await log.read_since("run_a", 0)
    assert event["payload"] == payload
