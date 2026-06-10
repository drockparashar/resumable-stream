"""Crash recovery: a run left in `streaming` by a dead process is reconciled
to a clean, recoverable terminal state on the next startup.

We simulate a crash by creating a run, flipping it to `streaming`, and then
NOT finishing it — exactly the state a process that died mid-turn leaves
behind. Then we run the same recovery scan the server runs at boot.
"""

import pytest

from app.event_log import InMemoryEventLog
from app.registry import recover_orphans
from app.run_store import InMemoryRunStore
from app.schemas import (
    EV_RUN_INTERRUPTED,
    STATUS_COMPLETED,
    STATUS_INTERRUPTED,
    STATUS_STREAMING,
)


async def test_orphaned_streaming_run_is_recovered():
    event_log = InMemoryEventLog()
    run_store = InMemoryRunStore()

    # A run that "crashed" mid-stream: status stuck at streaming, no terminal event.
    await run_store.create_run("run_crashed", "simulated-pipeline")
    await run_store.set_status("run_crashed", STATUS_STREAMING)

    # A run that finished cleanly: must NOT be touched by recovery.
    await run_store.create_run("run_ok", "simulated-pipeline")
    await run_store.set_status("run_ok", STATUS_COMPLETED)

    recovered = await recover_orphans(event_log, run_store)

    assert recovered == ["run_crashed"]
    assert (await run_store.get_run("run_crashed"))["status"] == STATUS_INTERRUPTED
    assert (await run_store.get_run("run_ok"))["status"] == STATUS_COMPLETED

    # A reconnecting client will now see a terminal interrupted event.
    events = await event_log.read_since("run_crashed", 0)
    assert events[-1]["type"] == EV_RUN_INTERRUPTED
    assert events[-1]["payload"]["reason"] == "server_restart_recovery"


async def test_recovery_is_idempotent_for_clean_runs():
    event_log = InMemoryEventLog()
    run_store = InMemoryRunStore()
    await run_store.create_run("run_ok", "simulated-pipeline")
    await run_store.set_status("run_ok", STATUS_COMPLETED)

    assert await recover_orphans(event_log, run_store) == []
    assert await event_log.read_since("run_ok", 0) == []
