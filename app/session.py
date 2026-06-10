"""``RunSession`` — owns one run independently of any client connection.

This is the central inversion in the whole design: a run is a **durable
process**, and a WebSocket is a **disposable view** over it. The agent/job task
runs to completion regardless of how many clients are watching, or none.

Each event the job produces flows through one pipeline::

    job -> RunSession.emit ->  EventLog.append   (durable; assigns seq)
                            ->  fan out to every subscriber queue

Subscribers are bounded ``asyncio.Queue``s. A slow consumer must never be able
to slow the producer or the other consumers, so when its queue overflows we do
*not* block — we drain it and drop a ``resync_required`` sentinel carrying a
cursor. The client re-fetches from the log and reconnects. Degrade the slow
consumer, never the producer.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from .event_log import EventLog
from .jobs import Job
from .run_store import RunStore
from .schemas import (
    EV_RUN_COMPLETED,
    EV_RUN_ERROR,
    EV_RUN_INTERRUPTED,
    EV_RUN_STARTED,
    MSG_RESYNC,
    STATUS_COMPLETED,
    STATUS_ERROR,
    STATUS_INTERRUPTED,
    STATUS_STREAMING,
    is_ephemeral,
    make_event,
    now_ms,
)

log = logging.getLogger("resumable-stream.session")

# Per-subscriber buffer. Below this it's just a buffer; on overflow we treat the
# subscriber as too slow and hand it a resync sentinel instead of blocking.
DEFAULT_QUEUE_SIZE = 256


class RunSession:
    def __init__(
        self,
        run_id: str,
        event_log: EventLog,
        run_store: RunStore,
        job: Job,
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> None:
        self.run_id = run_id
        self.event_log = event_log
        self.run_store = run_store
        self.job = job
        self.queue_size = queue_size
        self.subscribers: set[asyncio.Queue] = set()
        self._task: asyncio.Task | None = None
        self._start_lock = asyncio.Lock()
        self._latest_seq = 0

    # ── subscribers ──────────────────────────────────────────────────────
    def subscribe(self, maxsize: int | None = None) -> asyncio.Queue:
        # Per-subscriber buffer. A larger buffer tolerates a burstier consumer;
        # a smaller one trades memory for earlier resync. Defaults to the
        # session-wide size.
        q: asyncio.Queue = asyncio.Queue(maxsize=maxsize or self.queue_size)
        self.subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self.subscribers.discard(q)

    @property
    def latest_seq(self) -> int:
        return self._latest_seq

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    # ── the emit pipeline ────────────────────────────────────────────────
    async def emit(self, event: dict[str, Any]) -> dict[str, Any]:
        """Persist (unless ephemeral) then fan out to all subscribers."""
        if is_ephemeral(event):
            # Live-only: never touches the durable log; seq=0 so the client's
            # `seq > lastSeen` guard never suppresses it.
            stored = {**event, "seq": 0, "received_at": now_ms()}
        else:
            stored = await self.event_log.append(self.run_id, event)
            if stored["seq"] > self._latest_seq:
                self._latest_seq = stored["seq"]

        for q in list(self.subscribers):
            self._offer(q, stored)
        return stored

    def _offer(self, q: asyncio.Queue, item: dict[str, Any]) -> None:
        """Non-blocking hand-off. On overflow, resync the laggard."""
        try:
            q.put_nowait(item)
        except asyncio.QueueFull:
            # Drain everything the slow consumer hasn't read — those events are
            # still in the durable log, so it loses nothing it can't recover.
            while True:
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    break
            sentinel = {
                "type": MSG_RESYNC,
                "since_seq": max(0, self._latest_seq - 1),
                "reason": "subscriber_lag",
            }
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(sentinel)

    # ── lifecycle ────────────────────────────────────────────────────────
    async def start(self) -> bool:
        """Start the job task. Returns False if it's already running."""
        async with self._start_lock:
            if self.running:
                return False
            self._task = asyncio.create_task(self._run())
            return True

    async def interrupt(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        await self.run_store.set_status(self.run_id, STATUS_STREAMING)
        final_status = STATUS_COMPLETED
        try:
            await self.emit(make_event(self.run_id, EV_RUN_STARTED,
                                       {"job": self.job.name}))
            async for ev in self.job.run():
                await self.emit(
                    make_event(
                        self.run_id,
                        ev["type"],
                        ev.get("payload"),
                        ephemeral=ev.get("ephemeral", False),
                    )
                )
            await self.emit(make_event(self.run_id, EV_RUN_COMPLETED))
        except asyncio.CancelledError:
            final_status = STATUS_INTERRUPTED
            await self.emit(make_event(self.run_id, EV_RUN_INTERRUPTED,
                                       {"reason": "cancelled"}))
            raise
        except Exception as exc:  # noqa: BLE001 - surface as an event, don't crash
            final_status = STATUS_ERROR
            log.exception("job failed for run %s", self.run_id)
            await self.emit(make_event(self.run_id, EV_RUN_ERROR,
                                       {"message": str(exc)}))
        finally:
            # Flip the status OUT of "streaming" so the crash-recovery scan on
            # the next startup knows this run exited cleanly.
            await self.run_store.set_status(self.run_id, final_status)
