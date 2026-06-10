"""Scripted CLI demo of the headline feature: disconnect mid-run, reconnect,
catch up exactly.

Run the server first (in another terminal):

    uvicorn app.main:app --port 8000

Then:

    python scripts/demo.py

It will: start a run, stream a few events, "disconnect", let the run keep going
server-side, then reconnect from the last seq it saw and show that it receives
only the events it missed — no gaps, no duplicates.
"""

import asyncio
import json
import urllib.request

import websockets

BASE = "http://localhost:8000"
WS = "ws://localhost:8000"


def create_run() -> str:
    req = urllib.request.Request(
        f"{BASE}/runs",
        data=b"{}",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)["run_id"]


async def read_until_hydrated_then(n_live, ws, applied, stop_on_terminal=False):
    """Read the replay + hydrated marker, then live events.

    Stops after `n_live` durable live events, or immediately when a terminal
    lifecycle event arrives if `stop_on_terminal` is set.
    """
    live = 0
    terminal = {"run_completed", "run_interrupted", "run_error"}
    while True:
        msg = json.loads(await ws.recv())
        t = msg.get("type")
        if t == "hydrated":
            print(f"    ↳ hydrated (caught up to seq {msg['latest_seq']})")
            continue
        seq = msg.get("seq", 0)
        if seq and seq <= applied["last"]:
            print(f"    · dup seq {seq} dropped by guard")
            continue
        tag = f"seq {seq}" if seq else "ephemeral"
        print(f"    · {tag:>12}  {t}")
        if seq:
            applied["last"] = seq
            applied["seqs"].append(seq)
        if stop_on_terminal and t in terminal:
            return
        if seq:  # count only durable live events toward the budget
            live += 1
            if live >= n_live:
                return


async def main():
    run_id = create_run()
    print(f"created {run_id}\n")
    applied = {"last": 0, "seqs": []}

    print("[1] connect and watch a few events, then disconnect:")
    async with websockets.connect(f"{WS}/runs/{run_id}/stream?since_seq=0") as ws:
        await read_until_hydrated_then(4, ws, applied)
    left_at = applied["last"]
    print(f"\n    disconnected at seq {left_at}. run keeps going server-side...\n")

    await asyncio.sleep(2.5)  # let the run advance while we're away

    print(f"[2] reconnect from since_seq={left_at} — expect only newer events:")
    async with websockets.connect(f"{WS}/runs/{run_id}/stream?since_seq={left_at}") as ws:
        await read_until_hydrated_then(99, ws, applied, stop_on_terminal=True)

    seqs = applied["seqs"]
    print("\n[3] verification:")
    print(f"    applied seqs: {seqs}")
    print(f"    no gaps:      {seqs == list(range(1, len(seqs) + 1))}")
    print(f"    no dupes:     {len(seqs) == len(set(seqs))}")


if __name__ == "__main__":
    asyncio.run(main())
