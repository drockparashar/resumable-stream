"""The pluggable unit of work.

A :class:`Job` is anything that, when run, *yields a stream of events over
time*. The platform doesn't care what the work is — it cares that it's
long-running and observable. Decoupling the job behind a tiny async-iterator
interface is what lets the same streaming/resume/recovery machinery wrap any
workload.

:class:`SimulatedPipeline` is the default: a multi-stage "data pipeline" that
sleeps between steps so a run lasts several seconds — long enough to actually
demonstrate disconnect-and-resume in a browser. It also emits one *ephemeral*
``thinking`` event per stage to show how live-only events differ from durable
ones.

A real second job type (e.g. streaming a subprocess's stdout line by line)
would implement the same ``run`` method and drop straight in.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, Protocol

from .schemas import (
    EV_PROGRESS,
    EV_STAGE_COMPLETED,
    EV_STAGE_STARTED,
    EV_THINKING,
)


class Job(Protocol):
    name: str

    def run(self) -> AsyncIterator[dict[str, Any]]:
        """Yield events (``{"type", "payload", ...}``) until the work is done."""
        ...


class SimulatedPipeline:
    """A staged pipeline that emits progress events with delays.

    Total wall-clock ≈ ``len(stages) * steps_per_stage * step_delay``.
    The defaults give ~7s, comfortably long enough to disconnect mid-run.
    """

    def __init__(
        self,
        stages: list[str] | None = None,
        steps_per_stage: int = 3,
        step_delay: float = 0.6,
    ) -> None:
        self.name = "simulated-pipeline"
        self.stages = stages or ["extract", "transform", "validate", "load"]
        self.steps_per_stage = steps_per_stage
        self.step_delay = step_delay

    async def run(self) -> AsyncIterator[dict[str, Any]]:
        total = len(self.stages)
        for index, stage in enumerate(self.stages, start=1):
            yield {
                "type": EV_STAGE_STARTED,
                "payload": {"stage": stage, "index": index, "total": total},
            }
            # An ephemeral event: streamed to live viewers, never logged, never
            # replayed on reconnect. Reconnecting clients won't see this line —
            # that's the point.
            yield {
                "type": EV_THINKING,
                "payload": {"note": f"planning {stage}..."},
                "ephemeral": True,
            }
            for step in range(1, self.steps_per_stage + 1):
                await asyncio.sleep(self.step_delay)
                yield {
                    "type": EV_PROGRESS,
                    "payload": {
                        "stage": stage,
                        "step": step,
                        "of": self.steps_per_stage,
                    },
                }
            yield {
                "type": EV_STAGE_COMPLETED,
                "payload": {"stage": stage, "index": index, "total": total},
            }


def build_job(job_type: str, params: dict[str, Any] | None = None) -> Job:
    """Factory used by the registry to turn a stored job_type into a Job."""
    params = params or {}
    if job_type == "simulated-pipeline":
        return SimulatedPipeline(
            stages=params.get("stages"),
            steps_per_stage=params.get("steps_per_stage", 3),
            step_delay=params.get("step_delay", 0.6),
        )
    raise ValueError(f"unknown job_type: {job_type!r}")
