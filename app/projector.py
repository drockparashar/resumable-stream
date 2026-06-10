"""The read model: derive a compact snapshot from the event stream.

This is the CQRS-flavored split. The event log is the **write model** (the
append-only source of truth). The snapshot is the **read model** — the small
shape a UI actually wants: current status, per-stage progress, an event count.

Because the snapshot is *derived*, it never needs its own durability story: if
it's ever lost or stale, you rebuild it by replaying ``read_since(run_id, 0)``.
That's what :func:`project` does. (The original production system debounced
persisting this during streaming and force-flushed at turn boundaries; here we
just project on demand, which is enough to show the concept.)
"""

from __future__ import annotations

from typing import Any

from .schemas import (
    EV_PROGRESS,
    EV_RUN_COMPLETED,
    EV_RUN_ERROR,
    EV_RUN_INTERRUPTED,
    EV_RUN_STARTED,
    EV_STAGE_COMPLETED,
    EV_STAGE_STARTED,
    STATUS_COMPLETED,
    STATUS_ERROR,
    STATUS_INTERRUPTED,
    STATUS_PENDING,
    STATUS_STREAMING,
)


def project(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Fold an ordered list of (durable) events into a snapshot."""
    snapshot: dict[str, Any] = {
        "status": STATUS_PENDING,
        "latest_seq": 0,
        "event_count": 0,
        "stages": {},          # stage name -> "started" | "completed"
        "progress": None,      # last progress payload seen
    }
    for e in events:
        # Ephemeral events are never in the durable log, so they never reach
        # the projector — the read model is built only from durable truth.
        snapshot["latest_seq"] = max(snapshot["latest_seq"], e.get("seq", 0))
        snapshot["event_count"] += 1
        etype = e["type"]
        payload = e.get("payload") or {}

        if etype == EV_RUN_STARTED:
            snapshot["status"] = STATUS_STREAMING
        elif etype == EV_RUN_COMPLETED:
            snapshot["status"] = STATUS_COMPLETED
        elif etype == EV_RUN_ERROR:
            snapshot["status"] = STATUS_ERROR
        elif etype == EV_RUN_INTERRUPTED:
            snapshot["status"] = STATUS_INTERRUPTED
        elif etype == EV_STAGE_STARTED:
            snapshot["stages"][payload.get("stage")] = "started"
        elif etype == EV_STAGE_COMPLETED:
            snapshot["stages"][payload.get("stage")] = "completed"
        elif etype == EV_PROGRESS:
            snapshot["progress"] = payload

    return snapshot