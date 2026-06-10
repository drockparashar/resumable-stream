"""The headline property: disconnect mid-run, reconnect, lose nothing and
double-apply nothing.

We drive a RunSession directly (no HTTP needed) with a tiny scripted job, watch
through a subscriber, simulate a disconnect, then "reconnect" by replaying from
the durable log at the last seq we saw — applying the same `seq > lastSeen`
guard the real client uses. The reconstructed view must equal the full,
in-order event history exactly once.
"""

import asyncio
from collections.abc import AsyncIterator

import pytest

from app.event_log import InMemoryEventLog
from app.run_store import InMemoryRunStore
from app.schemas import MSG_RESYNC, is_ephemeral
from app.session import RunSession


class ScriptedJob:
    """Emits N progress events with no delay; deterministic for tests."""

    name = "scripted"

    def __init__(self, n: int = 6):
        self.n = n

    async def run(self) -> AsyncIterator[dict]:
        for i in range(self.n):
            yield {"type": "progress", "payload": {"i": i}}


def apply_with_guard(view: dict, event: dict) -> None:
    """Mimic the client: dedupe non-ephemeral events on seq > lastSeen."""
    if is_ephemeral(event) or event.get("type", "").startswith("hydrated"):
        return
    seq = event.get("seq", 0)
    if seq and seq <= view["lastSeen"]:
        return  # duplicate — drop
    if seq:
        view["lastSeen"] = seq
        view["events"].append(event)


async def test_reconnect_has_no_gaps_or_duplicates():
    event_log = InMemoryEventLog()
    run_store = InMemoryRunStore()
    run_id = "run_resume"
    await run_store.create_run(run_id, "scripted")
    session = RunSession(run_id, event_log, run_store, ScriptedJob(n=6))

    # --- client A connects and watches the first part of the run ---
    view = {"lastSeen": 0, "events": []}
    q = session.subscribe()
    await session.start()

    # read a few events, then "disconnect"
    for _ in range(4):
        apply_with_guard(view, await q.get())
    session.unsubscribe(q)
    seen_before = view["lastSeen"]
    assert seen_before > 0

    # let the run finish while the client is away
    await session.interrupt() if False else None
    while session.running:
        await asyncio.sleep(0.01)

    # --- reconnect: replay everything after the last seq we saw ---
    for event in await event_log.read_since(run_id, seen_before):
        apply_with_guard(view, event)

    # the reconstructed stream is the full history, in order, no dups
    seqs = [e["seq"] for e in view["events"]]
    full = await event_log.read_since(run_id, 0)
    assert seqs == [e["seq"] for e in full]            # no gaps
    assert seqs == sorted(set(seqs))                   # strictly increasing, unique


async def test_overlap_between_replay_and_live_is_deduped():
    """If replay and the live queue both deliver the same event, the guard
    keeps exactly one."""
    event_log = InMemoryEventLog()
    run_store = InMemoryRunStore()
    run_id = "run_overlap"
    await run_store.create_run(run_id, "scripted")
    session = RunSession(run_id, event_log, run_store, ScriptedJob(n=3))

    view = {"lastSeen": 0, "events": []}
    q = session.subscribe()
    await session.start()
    while session.running:
        await asyncio.sleep(0.01)

    # Replay from 0 (overlaps everything still sitting in the queue) ...
    for event in await event_log.read_since(run_id, 0):
        apply_with_guard(view, event)
    # ... then drain whatever the live queue also delivered.
    while not q.empty():
        apply_with_guard(view, q.get_nowait())

    seqs = [e["seq"] for e in view["events"]]
    assert seqs == sorted(set(seqs))  # each seq applied at most once
