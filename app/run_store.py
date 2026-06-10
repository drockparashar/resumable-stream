"""Run metadata + the ``live_status`` flag that powers crash recovery.

The EventLog stores *what happened*; the RunStore stores *the run's current
disposition* — its status, job type, and when it was created.

The load-bearing field is ``status``. It is flipped to ``streaming`` when a run
starts and to a terminal value (``completed`` / ``error`` / ``interrupted``)
when it ends. If the process dies mid-run, the row is left stuck at
``streaming`` — and that stuck flag is exactly the fingerprint the startup
recovery scan looks for (see ``registry.recover_orphans``).

Same two-backend pattern as the EventLog: SQLite for real use, in-memory for
tests.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from .schemas import STATUS_PENDING, now_ms


class RunStore(ABC):
    @abstractmethod
    async def create_run(self, run_id: str, job_type: str,
                         params: dict[str, Any] | None = None) -> None: ...

    @abstractmethod
    async def set_status(self, run_id: str, status: str) -> None: ...

    @abstractmethod
    async def get_run(self, run_id: str) -> dict[str, Any] | None: ...

    @abstractmethod
    async def list_by_status(self, status: str) -> list[str]:
        """Return run_ids currently in ``status`` (used by crash recovery)."""


class SqliteRunStore(RunStore):
    def __init__(self, db_path: str | Path):
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id     TEXT PRIMARY KEY,
                    status     TEXT NOT NULL,
                    job_type   TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    params     TEXT
                )
                """
            )
            self._conn.commit()

    def _create_sync(self, run_id, job_type, params):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO runs (run_id, status, job_type, created_at, params) "
                "VALUES (?, ?, ?, ?, ?)",
                (run_id, STATUS_PENDING, job_type, now_ms(), json.dumps(params or {})),
            )
            self._conn.commit()

    def _set_status_sync(self, run_id, status):
        with self._lock:
            self._conn.execute(
                "UPDATE runs SET status = ? WHERE run_id = ?", (status, run_id)
            )
            self._conn.commit()

    def _get_sync(self, run_id):
        with self._lock:
            row = self._conn.execute(
                "SELECT run_id, status, job_type, created_at, params FROM runs "
                "WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "status": row[1],
            "job_type": row[2],
            "created_at": row[3],
            "params": json.loads(row[4]) if row[4] else {},
        }

    def _list_by_status_sync(self, status):
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id FROM runs WHERE status = ?", (status,)
            ).fetchall()
        return [r[0] for r in rows]

    async def create_run(self, run_id, job_type, params=None):
        await asyncio.to_thread(self._create_sync, run_id, job_type, params)

    async def set_status(self, run_id, status):
        await asyncio.to_thread(self._set_status_sync, run_id, status)

    async def get_run(self, run_id):
        return await asyncio.to_thread(self._get_sync, run_id)

    async def list_by_status(self, status):
        return await asyncio.to_thread(self._list_by_status_sync, status)

    def close(self):
        with self._lock:
            self._conn.close()


class InMemoryRunStore(RunStore):
    def __init__(self) -> None:
        self._runs: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def create_run(self, run_id, job_type, params=None):
        async with self._lock:
            self._runs[run_id] = {
                "run_id": run_id,
                "status": STATUS_PENDING,
                "job_type": job_type,
                "created_at": now_ms(),
                "params": params or {},
            }

    async def set_status(self, run_id, status):
        async with self._lock:
            if run_id in self._runs:
                self._runs[run_id]["status"] = status

    async def get_run(self, run_id):
        async with self._lock:
            run = self._runs.get(run_id)
            return dict(run) if run else None

    async def list_by_status(self, status):
        async with self._lock:
            return [rid for rid, r in self._runs.items() if r["status"] == status]
