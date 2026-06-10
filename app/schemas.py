"""Event shapes, type constants, and small helpers.

Everything that flows through the system is a plain ``dict`` shaped like an
**event**. Keeping it a dict (rather than a class) is deliberate: events are
serialized to JSON for the WebSocket and to a column in SQLite, so a dict is
the natural lowest-common-denominator and avoids (de)serialization glue.

An event looks like this once it has been through the log::

    {
        "seq":         4187,            # monotonic, per-run, assigned on append
        "type":        "stage_started", # see the EV_* / MSG_* constants below
        "run_id":      "run_9f2c...",
        "received_at": 1733829461201,   # epoch ms, assigned on append
        "payload":     {...} | None,    # type-specific body
        "ephemeral":   false            # if true: streamed live, never logged
    }

Before it reaches the log it has no ``seq`` / ``received_at`` yet — those are
assigned at append time by the EventLog. ``make_event`` builds that pre-append
shape.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

# ── lifecycle event types (these are produced by RunSession itself) ──────────
EV_RUN_STARTED = "run_started"
EV_RUN_COMPLETED = "run_completed"
EV_RUN_INTERRUPTED = "run_interrupted"
EV_RUN_ERROR = "run_error"

# ── job event types (produced by a Job while it runs) ────────────────────────
EV_STAGE_STARTED = "stage_started"
EV_STAGE_COMPLETED = "stage_completed"
EV_PROGRESS = "progress"
EV_THINKING = "thinking"  # demo of an ephemeral event (see EPHEMERAL_TYPES)

# ── control messages (sent to clients only; never job/agent output) ──────────
MSG_HYDRATED = "hydrated"          # "you are caught up; latest_seq is N"
MSG_RESYNC = "resync_required"     # "you fell behind; re-fetch from since_seq"

# Ephemeral events are fanned out to live subscribers but never written to the
# durable log and never folded into the snapshot. They are only meaningful
# while a run is actively streaming (think: a model's "thinking" tokens) — there
# is no value in replaying them on reconnect, and they can be large.
EPHEMERAL_TYPES: frozenset[str] = frozenset({EV_THINKING})

# ── run status values (stored in the RunStore) ───────────────────────────────
STATUS_PENDING = "pending"
STATUS_STREAMING = "streaming"
STATUS_COMPLETED = "completed"
STATUS_INTERRUPTED = "interrupted"
STATUS_ERROR = "error"


def now_ms() -> int:
    """Current wall-clock time in epoch milliseconds."""
    return int(time.time() * 1000)


def new_run_id() -> str:
    """A short, sortable-ish, collision-resistant run id."""
    return "run_" + uuid.uuid4().hex[:12]


def make_event(
    run_id: str,
    type: str,
    payload: dict[str, Any] | None = None,
    *,
    ephemeral: bool = False,
) -> dict[str, Any]:
    """Build the pre-append event shape.

    ``seq`` and ``received_at`` are intentionally absent here — the EventLog
    assigns them atomically at append time so they are always consistent with
    the durable order.
    """
    return {
        "type": type,
        "run_id": run_id,
        "payload": payload,
        "ephemeral": ephemeral or type in EPHEMERAL_TYPES,
    }


def is_ephemeral(event: dict[str, Any]) -> bool:
    return bool(event.get("ephemeral")) or event.get("type") in EPHEMERAL_TYPES
