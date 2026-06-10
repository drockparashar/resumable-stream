"""Backpressure: a slow consumer must degrade itself, never the producer or
the other consumers.

We attach two subscribers to a session with a tiny queue. One we never read
(the laggard); one we drain eagerly. We emit far more events than the queue
holds. Expectations:

* emit never blocks or raises (the producer runs at full speed);
* the laggard ends up holding a `resync_required` sentinel (told to recover);
* the attentive consumer receives every event.
"""

import asyncio

import pytest

from app.event_log import InMemoryEventLog
from app.run_store import InMemoryRunStore
from app.schemas import MSG_RESYNC, make_event
from app.session import RunSession


async def test_slow_consumer_gets_resync_and_fast_consumer_is_unharmed():
    event_log = InMemoryEventLog()
    run_store = InMemoryRunStore()
    run_id = "run_bp"
    await run_store.create_run(run_id, "scripted")
    session = RunSession(run_id, event_log, run_store, job=None)

    slow_q = session.subscribe(maxsize=4)       # tiny buffer → overflows fast
    fast_q = session.subscribe(maxsize=1000)    # roomy buffer → keeps up

    received_fast: list[dict] = []

    async def drain_fast():
        # pull until we've seen the last event
        while True:
            ev = await fast_q.get()
            received_fast.append(ev)
            if ev.get("type") == "progress" and ev.get("payload", {}).get("i") == 49:
                return

    drainer = asyncio.create_task(drain_fast())

    # Emit 50 events directly through the pipeline (no job needed).
    for i in range(50):
        await session.emit(make_event(run_id, "progress", {"i": i}))

    await asyncio.wait_for(drainer, timeout=2.0)

    # Producer was never blocked: all 50 are durably logged in order.
    logged = await event_log.read_since(run_id, 0)
    assert [e["payload"]["i"] for e in logged] == list(range(50))

    # Fast consumer saw everything.
    assert [e["payload"]["i"] for e in received_fast] == list(range(50))

    # Slow consumer was handed a resync sentinel instead of stalling anyone.
    drained_slow = []
    while not slow_q.empty():
        drained_slow.append(slow_q.get_nowait())
    assert any(e.get("type") == MSG_RESYNC for e in drained_slow)
    sentinel = next(e for e in drained_slow if e.get("type") == MSG_RESYNC)
    assert "since_seq" in sentinel

