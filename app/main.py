"""FastAPI wiring: REST to create/inspect runs, WebSocket to stream + resume.

Endpoints
---------
POST /runs                         -> create a run and start it; returns {run_id}
GET  /runs/{run_id}                -> snapshot (read model) rebuilt from the log
WS   /runs/{run_id}/stream?since_seq=N
        The resumable stream. The handler:
          1. subscribes to the live session BEFORE replaying — so any event
             produced during the handover is captured in the queue, never lost;
          2. replays the gap via read_since(since_seq);
          3. sends a `hydrated` marker with the latest seq;
          4. forwards live events until the client disconnects.
        Duplicates between the replay and the queue are expected and harmless —
        the client dedupes on `seq > lastSeen`.
GET  /                             -> the minimal vanilla web client
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

from .event_log import SqliteEventLog
from .projector import project
from .registry import RunRegistry, recover_orphans
from .run_store import SqliteRunStore
from .schemas import MSG_HYDRATED

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("resumable-stream")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
WEB_DIR = Path(__file__).resolve().parent.parent / "web"


@asynccontextmanager
async def lifespan(app: FastAPI):
    DATA_DIR.mkdir(exist_ok=True)
    event_log = SqliteEventLog(DATA_DIR / "events.db")
    run_store = SqliteRunStore(DATA_DIR / "runs.db")

    # Crash recovery: reconcile any run a previous process left mid-stream.
    recovered = await recover_orphans(event_log, run_store)
    if recovered:
        log.info("startup recovered %d orphaned run(s): %s", len(recovered), recovered)

    app.state.event_log = event_log
    app.state.run_store = run_store
    app.state.registry = RunRegistry(event_log, run_store)
    try:
        yield
    finally:
        event_log.close()
        run_store.close()


app = FastAPI(title="resumable-stream", lifespan=lifespan)


@app.post("/runs")
async def create_run(body: dict | None = None):
    body = body or {}
    registry: RunRegistry = app.state.registry
    run_id = await registry.create_run(
        job_type=body.get("job_type", "simulated-pipeline"),
        params=body.get("params"),
    )
    await registry.start_run(run_id)
    return {"run_id": run_id}


@app.get("/runs/{run_id}")
async def get_run(run_id: str):
    run_store = app.state.run_store
    event_log = app.state.event_log
    meta = await run_store.get_run(run_id)
    if meta is None:
        return JSONResponse({"error": "unknown run_id"}, status_code=404)
    events = await event_log.read_since(run_id, 0)
    return {"run": meta, "snapshot": project(events)}


@app.websocket("/runs/{run_id}/stream")
async def stream(ws: WebSocket, run_id: str, since_seq: int = 0):
    await ws.accept()
    registry: RunRegistry = app.state.registry
    event_log = app.state.event_log

    session = await registry.get_or_create_session(run_id)

    # --- Step 1: subscribe BEFORE replay (only if there's a live session). ---
    # Subscribing first means anything emitted between now and the replay below
    # lands in the queue; the client's dedupe guard removes the overlap.
    queue = session.subscribe() if session is not None else None

    try:
        # --- Step 2: replay the gap from the durable log. ---
        history = await event_log.read_since(run_id, since_seq)
        for event in history:
            await ws.send_json(event)

        # --- Step 3: tell the client it's caught up. ---
        latest = history[-1]["seq"] if history else since_seq
        await ws.send_json({"type": MSG_HYDRATED, "latest_seq": latest})

        # --- Step 4: forward live events until disconnect. ---
        if queue is None:
            # No live session (e.g. run finished before the server restarted,
            # or unknown run). History is all there is; keep the socket open
            # briefly is pointless, so we just return.
            return
        while True:
            event = await queue.get()
            await ws.send_json(event)
    except WebSocketDisconnect:
        pass
    finally:
        if session is not None and queue is not None:
            session.unsubscribe(queue)


@app.get("/")
async def index():
    return FileResponse(WEB_DIR / "index.html")
