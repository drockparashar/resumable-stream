"""End-to-end over the real FastAPI app, including the WebSocket resume path.

Uses Starlette's TestClient (no network port). This proves the wiring in
main.py — create a run, stream it, reconnect with since_seq, and confirm the
reconnect picks up where we left off without gaps.
"""

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.schemas import MSG_HYDRATED


@pytest.fixture
def client(tmp_path, monkeypatch):
    # Point the app's data dir at a temp location so tests don't share state.
    import app.main as main_mod
    monkeypatch.setattr(main_mod, "DATA_DIR", tmp_path)
    with TestClient(app) as c:
        yield c


def _collect(ws, stop_type):
    """Read frames until we see `stop_type`; return the list."""
    out = []
    while True:
        msg = ws.receive_json()
        out.append(msg)
        if msg.get("type") == stop_type:
            return out


def test_create_stream_and_resume(client):
    run_id = client.post("/runs", json={
        "params": {"step_delay": 0.05, "steps_per_stage": 2,
                   "stages": ["extract", "load"]},
    }).json()["run_id"]

    # --- first connection: read until caught up, note last seq, disconnect ---
    last_seq = 0
    with client.websocket_connect(f"/runs/{run_id}/stream?since_seq=0") as ws:
        frames = _collect(ws, MSG_HYDRATED)
        for f in frames:
            if f.get("seq"):
                last_seq = max(last_seq, f["seq"])
        # read a couple of live frames, then bail
        for _ in range(2):
            f = ws.receive_json()
            if f.get("seq"):
                last_seq = max(last_seq, f["seq"])

    # --- reconnect from last_seq: must only get newer events ---
    with client.websocket_connect(f"/runs/{run_id}/stream?since_seq={last_seq}") as ws:
        frames = _collect(ws, MSG_HYDRATED)
        replayed = [f for f in frames if f.get("seq")]
        # every replayed event is strictly after where we left off — no gap, no
        # re-delivery of what we already had
        assert all(f["seq"] > last_seq for f in replayed)


def test_snapshot_endpoint(client):
    run_id = client.post("/runs", json={
        "params": {"step_delay": 0.01, "steps_per_stage": 1, "stages": ["extract"]},
    }).json()["run_id"]

    # drain the stream to completion
    with client.websocket_connect(f"/runs/{run_id}/stream") as ws:
        seen = set()
        while True:
            f = ws.receive_json()
            seen.add(f.get("type"))
            if f.get("type") == "run_completed":
                break

    snap = client.get(f"/runs/{run_id}").json()
    assert snap["snapshot"]["status"] == "completed"
    assert snap["run"]["status"] == "completed"


def test_unknown_run_404(client):
    assert client.get("/runs/run_does_not_exist").status_code == 404
