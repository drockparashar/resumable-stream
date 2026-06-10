"""The append-only event log — the spine of the whole system.

Every non-ephemeral event is appended here and gets a **monotonic, per-run
sequence number** assigned atomically at write time. That single number is the
foundation for:

* **Resume** — a reconnecting client says "I have up to seq N", the server
  returns only ``seq > N`` via :meth:`read_since`. No gap, no full replay.
* **Dedupe** — because the stream is at-least-once, a client may see an event
  twice; it keeps the highest seq it has applied and ignores anything
  ``<= lastSeen``. At-least-once + idempotent apply = effectively exactly-once.
* **Snapshot rebuild** — the log is the source of truth, so the read-model can
  always be reconstructed by replaying ``read_since(run_id, 0)``.

Two backends sit behind one interface (ports & adapters):

* :class:`SqliteEventLog` — the real one. Zero setup; a single file. seq is
  allocated inside a locked transaction so concurrent appends stay monotonic.
* :class:`InMemoryEventLog` — used by the tests. Same contract, no I/O.

Swapping persistence (e.g. to Postgres) is a new subclass, not a rewrite — the
rest of the system depends only on the :class:`EventLog` interface.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from .schemas import now_ms


class EventLog(ABC):
    """Append-only, per-run, monotonically sequenced event store."""

    @abstractmethod
    async def append(self, run_id: str, event: dict[str, Any]) -> dict[str, Any]:
        """Append ``event`` to ``run_id``'s log.

        Returns the stored event with ``seq`` and ``received_at`` filled in.
        ``seq`` is strictly greater than every previously appended event for
        the same ``run_id``.
        """

    @abstractmethod
    async def read_since(self, run_id: str, since_seq: int) -> list[dict[str, Any]]:
        """Return all events for ``run_id`` with ``seq > since_seq``, in order."""

    @abstractmethod
    async def latest_seq(self, run_id: str) -> int:
        """Highest seq for ``run_id`` (0 if the run has no events yet)."""


# ─────────────────────────────────────────────────────────────────────────────
# SQLite backend
# ─────────────────────────────────────────────────────────────────────────────

class SqliteEventLog(EventLog):
    """Single-file SQLite implementation.

    sqlite calls are synchronous, so each public coroutine offloads its work to
    a thread (``asyncio.to_thread``) and a lock serializes access to the shared
    connection. That keeps the event loop responsive without one-connection
    threading hazards. For a single-process demo this is plenty; the same
    interface over Postgres would allocate seq with row-level locking instead.
    """

    def __init__(self, db_path: str | Path):
        self._path = str(db_path)
        # check_same_thread=False because to_thread may run on worker threads;
        # the lock below provides the actual mutual exclusion.
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    run_id      TEXT    NOT NULL,
                    seq         INTEGER NOT NULL,
                    type        TEXT    NOT NULL,
                    received_at INTEGER NOT NULL,
                    payload     TEXT,
                    PRIMARY KEY (run_id, seq)
                )
                """
            )
            self._conn.commit()

    # -- sync workers (run inside to_thread) ---------------------------------

    def _append_sync(self, run_id: str, event: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            cur = self._conn.cursor()
            # Allocate the next seq for THIS run inside the same lock so two
            # concurrent appends can never collide on a seq value.
            cur.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM events WHERE run_id = ?",
                (run_id,),
            )
            seq = int(cur.fetchone()[0]) + 1
            received_at = now_ms()
            cur.execute(
                "INSERT INTO events (run_id, seq, type, received_at, payload) "
                "VALUES (?, ?, ?, ?, ?)",
                (run_id, seq, event["type"], received_at,
                 json.dumps(event.get("payload"))),
            )
            self._conn.commit()
        return {
            "seq": seq,
            "type": event["type"],
            "run_id": run_id,
            "received_at": received_at,
            "payload": event.get("payload"),
            "ephemeral": False,
        }

    def _read_since_sync(self, run_id: str, since_seq: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, type, received_at, payload FROM events "
                "WHERE run_id = ? AND seq > ? ORDER BY seq ASC",
                (run_id, since_seq),
            ).fetchall()
        return [
            {
                "seq": r[0],
                "type": r[1],
                "run_id": run_id,
                "received_at": r[2],
                "payload": json.loads(r[3]) if r[3] is not None else None,
                "ephemeral": False,
            }
            for r in rows
        ]

    def _latest_seq_sync(self, run_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM events WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        return int(row[0])

    # -- async interface ------------------------------------------------------

    async def append(self, run_id: str, event: dict[str, Any]) -> dict[str, Any]:
        return await asyncio.to_thread(self._append_sync, run_id, event)

    async def read_since(self, run_id: str, since_seq: int) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._read_since_sync, run_id, since_seq)

    async def latest_seq(self, run_id: str) -> int:
        return await asyncio.to_thread(self._latest_seq_sync, run_id)

    def close(self) -> None:
        with self._lock:
            self._conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# In-memory backend (tests / ephemeral runs)
# ─────────────────────────────────────────────────────────────────────────────

class InMemoryEventLog(EventLog):
    """Dict-backed implementation with the identical contract.

    Demonstrates the ports-and-adapters seam: the rest of the code can't tell
    which backend it's talking to, so the tests use this one with zero I/O.
    """

    def __init__(self) -> None:
        self._events: dict[str, list[dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    async def append(self, run_id: str, event: dict[str, Any]) -> dict[str, Any]:
        async with self._lock:
            log = self._events.setdefault(run_id, [])
            seq = len(log) + 1
            stored = {
                "seq": seq,
                "type": event["type"],
                "run_id": run_id,
                "received_at": now_ms(),
                "payload": event.get("payload"),
                "ephemeral": False,
            }
            log.append(stored)
            return dict(stored)

    async def read_since(self, run_id: str, since_seq: int) -> list[dict[str, Any]]:
        async with self._lock:
            log = self._events.get(run_id, [])
            return [dict(e) for e in log if e["seq"] > since_seq]

    async def latest_seq(self, run_id: str) -> int:
        async with self._lock:
            log = self._events.get(run_id, [])
            return log[-1]["seq"] if log else 0