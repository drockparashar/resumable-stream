"""Session registry and the startup crash-recovery scan.

The registry holds the live :class:`RunSession` objects (which only exist
in-memory) and knows how to create runs from persisted metadata. Crucially it
also runs :func:`recover_orphans` at startup.

Crash-recovery model (and its honest limits):
    A run's *execution* is in-memory — if the process dies, the job's progress
    is gone. What survives is the durable log + the run's status row. On the
    next boot we find any run still marked ``streaming`` (the fingerprint of a
    process that died mid-turn), append a terminal ``run_interrupted`` event,
    and flip the status to ``interrupted``. So we reconcile the run to a clean,
    *recoverable* state and tell reconnecting clients about it — we do not
    auto-resume the work itself. That's the same trade-off real systems make:
    recover state, let the operator re-trigger.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from .event_log import EventLog
from .jobs import Job, build_job
from .run_store import RunStore
from .schemas import (
    EV_RUN_INTERRUPTED,
    STATUS_INTERRUPTED,
    STATUS_STREAMING,
    make_event,
    new_run_id,
)
from .session import RunSession

log = logging.getLogger("resumable-stream.registry")

JobFactory = Callable[[str, dict[str, Any] | None], Job]


class RunRegistry:
    def __init__(
        self,
        event_log: EventLog,
        run_store: RunStore,
        job_factory: JobFactory = build_job,
    ) -> None:
        self.event_log = event_log
        self.run_store = run_store
        self.job_factory = job_factory
        self._sessions: dict[str, RunSession] = {}

    async def create_run(
        self,
        job_type: str = "simulated-pipeline",
        params: dict[str, Any] | None = None,
    ) -> str:
        run_id = new_run_id()
        await self.run_store.create_run(run_id, job_type, params)
        return run_id

    def get(self, run_id: str) -> RunSession | None:
        return self._sessions.get(run_id)

    async def get_or_create_session(self, run_id: str) -> RunSession | None:
        """Return the live session for ``run_id``, creating it if the run
        exists in the store but has no in-memory session yet.

        Returns None if the run id is unknown entirely.
        """
        if run_id in self._sessions:
            return self._sessions[run_id]
        meta = await self.run_store.get_run(run_id)
        if meta is None:
            return None
        job = self.job_factory(meta["job_type"], meta.get("params"))
        session = RunSession(run_id, self.event_log, self.run_store, job)
        self._sessions[run_id] = session
        return session

    async def start_run(self, run_id: str) -> bool:
        session = await self.get_or_create_session(run_id)
        if session is None:
            raise KeyError(run_id)
        return await session.start()


async def recover_orphans(event_log: EventLog, run_store: RunStore) -> list[str]:
    """Reconcile any run left in ``streaming`` by a crashed previous process.

    Called once at startup. Returns the list of recovered run ids.
    """
    orphans = await run_store.list_by_status(STATUS_STREAMING)
    for run_id in orphans:
        await event_log.append(
            run_id,
            make_event(run_id, EV_RUN_INTERRUPTED,
                       {"reason": "server_restart_recovery"}),
        )
        await run_store.set_status(run_id, STATUS_INTERRUPTED)
        log.info("recovered orphaned run %s", run_id)
    return orphans
