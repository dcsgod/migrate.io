"""
observability/step_tracer.py
Captures every pipeline step for real-time UI visibility.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Callable, Iterator

import structlog

logger = structlog.get_logger(__name__)


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class PipelineStep:
    step_id: str
    run_id: str
    name: str
    node_id: str | None = None
    status: StepStatus = StepStatus.PENDING
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    input_summary: dict[str, Any] = field(default_factory=dict)
    output_summary: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "run_id": self.run_id,
            "name": self.name,
            "node_id": self.node_id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_ms": self.duration_ms,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "input_summary": self.input_summary,
            "output_summary": self.output_summary,
            "error": self.error,
            "metadata": self.metadata,
        }


class StepTracer:
    """
    Tracks pipeline execution steps and broadcasts events to subscribers
    (UI panels, WebSocket connections, audit log).

    Usage:
        tracer = StepTracer(run_id)
        with tracer.trace("Build Graph", node_id="graph"):
            graph = builder.build()
            tracer.current.rows_out = len(graph.nodes)
    """

    def __init__(self, run_id: str | None = None) -> None:
        self.run_id = run_id or str(uuid.uuid4())
        self.steps: list[PipelineStep] = []
        self.current: PipelineStep | None = None
        self._subscribers: list[Callable[[PipelineStep], None]] = []
        self._async_queues: list[asyncio.Queue] = []

    # ── Sync context manager ──────────────────────────────────

    @contextmanager
    def trace(
        self,
        name: str,
        node_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Iterator[PipelineStep]:
        step = PipelineStep(
            step_id=str(uuid.uuid4()),
            run_id=self.run_id,
            name=name,
            node_id=node_id,
            status=StepStatus.RUNNING,
            started_at=datetime.utcnow(),
            metadata=metadata or {},
        )
        self.steps.append(step)
        self.current = step
        self._emit(step)
        t0 = time.monotonic()
        try:
            yield step
            step.status = StepStatus.DONE
        except Exception as exc:
            step.status = StepStatus.ERROR
            step.error = str(exc)
            self._emit(step)
            raise
        finally:
            step.finished_at = datetime.utcnow()
            step.duration_ms = int((time.monotonic() - t0) * 1000)
            self._emit(step)

    # ── Async context manager ─────────────────────────────────

    @asynccontextmanager
    async def atrace(
        self,
        name: str,
        node_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AsyncIterator[PipelineStep]:
        step = PipelineStep(
            step_id=str(uuid.uuid4()),
            run_id=self.run_id,
            name=name,
            node_id=node_id,
            status=StepStatus.RUNNING,
            started_at=datetime.utcnow(),
            metadata=metadata or {},
        )
        self.steps.append(step)
        self.current = step
        await self._aemit(step)
        t0 = time.monotonic()
        try:
            yield step
            step.status = StepStatus.DONE
        except Exception as exc:
            step.status = StepStatus.ERROR
            step.error = str(exc)
            await self._aemit(step)
            raise
        finally:
            step.finished_at = datetime.utcnow()
            step.duration_ms = int((time.monotonic() - t0) * 1000)
            await self._aemit(step)

    # ── Subscriptions ─────────────────────────────────────────

    def subscribe(self, callback: Callable[[PipelineStep], None]) -> None:
        self._subscribers.append(callback)

    def subscribe_async(self, queue: asyncio.Queue) -> None:
        self._async_queues.append(queue)

    def _emit(self, step: PipelineStep) -> None:
        for cb in self._subscribers:
            try:
                cb(step)
            except Exception:
                pass

    async def _aemit(self, step: PipelineStep) -> None:
        self._emit(step)
        for q in self._async_queues:
            await q.put(step.to_dict())

    # ── Summary ───────────────────────────────────────────────

    def summary(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "total_steps": len(self.steps),
            "done": sum(1 for s in self.steps if s.status == StepStatus.DONE),
            "errors": sum(1 for s in self.steps if s.status == StepStatus.ERROR),
            "warnings": sum(1 for s in self.steps if s.status == StepStatus.WARNING),
            "steps": [s.to_dict() for s in self.steps],
        }
