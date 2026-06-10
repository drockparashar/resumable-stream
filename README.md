# resumable-stream

A small, from-scratch server that streams a long-running job to the browser and
**stays correct when things go wrong** — clients disconnect and reconnect, slow
viewers fall behind, and the server itself restarts mid-run.

It's a focused, clean-room implementation of a streaming-reliability pattern:
one **durable, monotonically-sequenced event log** as the source of truth, with
cursor-based resume, pub/sub fan-out, backpressure handling, and crash recovery
built on top of it.

---

## The 60-second demo

```bash
pip install -r requirements.txt
uvicorn app.main:app --port 8000
# open http://localhost:8000  →  click "New run", then "Disconnect" mid-run,
# then "Reconnect" — it catches up exactly from the last seq it saw.
```

Or run the scripted version (needs the dev deps: `pip install -r requirements-dev.txt`):

```bash
python scripts/demo.py
```

It starts a run, reads a few events, "disconnects", lets the run keep going
server-side, then reconnects from the last sequence number it saw and shows it
receives only the missed events — **no gaps, no duplicates.**

---

## Why this is interesting

Streaming a long job to a browser is easy until the run outlives the
connection. A 40-minute job hits all of these in practice:

| Failure | Detection | Recovery |
|---|---|---|
| Client drops & reconnects | client sends `since_seq` | server replays only `seq > N` from the log — no gap, no full replay |
| Duplicate delivery | `seq <= lastSeen` | client guard drops it before it touches UI state |
| Slow / stuck consumer | bounded queue overflow | server sends a `resync_required` sentinel; the laggard re-hydrates while the producer runs on |
| Server crash mid-run | `status == streaming` on boot | startup scan reconciles the orphan to a clean, recoverable state |
| Snapshot lost / stale | derived read-model | rebuild by replaying `read_since(0)` — the log is authoritative |
| Long silent job | (extensible) | application-level signalling instead of relying on transport pings |

Every recovery leans on the same idea: **the log is the single source of truth,
and a sequence number is the cursor into it.**

---

## Architecture

```
Browser ──WS /runs/{id}/stream?since_seq=N──►  FastAPI
                                                  │
                                          RunSession (one per run,
                                          independent of any socket)
                                                  │ emit()
                                   ┌──────────────┴───────────────┐
                                   ▼                              ▼
                            EventLog.append              fan-out to subscriber
                          (monotonic seq, durable)        queues (bounded)
                                   │
                            Projector → snapshot (read model)
```

**The one inversion that makes everything else simple:** a run is a *durable
process*; a WebSocket is a *disposable view* over it. Closing a tab never
touches the run; reconnecting is just "replay from my cursor, then stream live."

### Components

| File | Responsibility |
|---|---|
| `app/event_log.py` | Append-only log; assigns per-run monotonic `seq`. `EventLog` interface + `Sqlite` and `InMemory` backends (ports & adapters). |
| `app/run_store.py` | Run metadata + the `status` flag that crash recovery keys off. Same two-backend pattern. |
| `app/session.py` | `RunSession`: owns the job task, fans out events, handles backpressure (drain + resync sentinel). |
| `app/registry.py` | Holds live sessions; `recover_orphans()` reconciles runs left `streaming` by a crash. |
| `app/projector.py` | Derives the read-model snapshot from the event stream (CQRS-flavored split). |
| `app/jobs.py` | Pluggable `Job` interface + a `SimulatedPipeline` so a run lasts long enough to demo resume. |
| `app/schemas.py` | Event shape, type constants, ephemeral-event rules. |
| `app/main.py` | FastAPI wiring: `POST /runs`, `GET /runs/{id}`, `WS /runs/{id}/stream`, and the web client. |
| `web/index.html` | ~150-line vanilla client with the dedupe guard and disconnect/reconnect buttons. |

### Two ideas carry most of the correctness

**1. Events are sequenced at append time.** `seq` is allocated atomically per
run, so the order in the log is the order of truth.

**2. The client dedupes on `seq > lastSeen`.** The stream is *at-least-once*
(reconnect replays may overlap the live queue), and the guard makes applying it
*idempotent* — so the perceived delivery is effectively exactly-once. The same
`lastSeen` value is the cursor handed back on reconnect: one variable does both
jobs.

Ephemeral events (e.g. a model's "thinking") are streamed live but never
logged and never replayed — they carry `seq = 0` so the guard never suppresses
them and the read model never sees them.

---

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

The suite proves the reliability properties directly, not just the happy path:

| Test | Proves |
|---|---|
| `test_event_log.py` | monotonic seq from 1, `read_since` returns only the tail, runs are independent — run against **both** backends |
| `test_resume.py` | reconnect reconstructs the full history with **no gaps and no duplicates**, even when replay overlaps the live queue |
| `test_backpressure.py` | a slow consumer gets a resync sentinel and **never blocks** the producer or other consumers |
| `test_recovery.py` | an orphaned `streaming` run is reconciled to `interrupted` on startup; clean runs are left untouched |
| `test_integration.py` | end-to-end over the real FastAPI WebSocket: create → stream → reconnect from `since_seq` picks up only newer events |

---

## What this deliberately does *not* do

Scope was kept tight on purpose; these are conscious cuts, not oversights:

- **No auth.** Orthogonal to the streaming/reliability story.
- **No real Postgres/S3.** The ports-and-adapters seam is shown with a SQLite
  backend plus an in-memory one used by the tests — a Postgres `EventLog` would
  be a new subclass, not a rewrite.
- **Crash recovery reconciles state, it does not re-run the job.** A run's
  execution lives in memory; on restart we recover its *status* to a clean,
  recoverable terminal state and notify clients — we don't auto-resume the work.
  That's the same trade-off most systems make.
- **No React.** A small vanilla client demos resume more directly than a
  framework would.

### Possible next steps

- A Postgres `EventLog` / `RunStore` backend behind the existing interfaces.
- Event-log compaction (snapshot + truncate) so long runs don't grow unbounded.
- A real second `Job` type (e.g. streaming a subprocess's stdout).
- Structured metrics + tracing across request → session → job.

---

## Background

This is a personal, from-scratch implementation of a streaming-reliability
pattern I built into a production platform. It reproduces the *concepts and
architecture* — durable sequenced event log, cursor resume, backpressure, crash
recovery — on a generic job-runner domain. It contains no proprietary code.
