"""
api/routes/preview.py
Staged run execution + preview DataFrame + WebSocket log streaming.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

router = APIRouter()

_RUNS: dict[str, dict[str, Any]] = {}


class RunRequest(BaseModel):
    dag_id: str
    dry_run: bool = False
    preview_rows: int = 100


@router.post("/run")
async def start_run(body: RunRequest):
    """
    Start a staged run of the compiled DAG.
    Returns a run_id immediately. Poll /runs/{run_id} for status,
    or connect to WebSocket /preview/{run_id}/log for real-time logs.
    """
    from api.routes.dag import _DAGS

    dag = _DAGS.get(body.dag_id)
    if not dag:
        raise HTTPException(status_code=404, detail="DAG not found")

    run_id = str(uuid.uuid4())
    _RUNS[run_id] = {
        "run_id": run_id,
        "dag_id": body.dag_id,
        "status": "pending",
        "dry_run": body.dry_run,
        "preview_rows": body.preview_rows,
        "steps": [],
        "preview": None,
        "compiled_code": None,
    }

    # Compile code
    from compiler.spark_compiler import SparkCompiler
    compiler = SparkCompiler()
    code = compiler.compile(dag, plan_id=run_id)
    _RUNS[run_id]["compiled_code"] = code
    _RUNS[run_id]["status"] = "compiled"

    # In production: submit to Databricks Jobs API / local spark-submit
    # For now: return the compiled code and mark as ready for approval
    _RUNS[run_id]["status"] = "staged"

    return {
        "run_id": run_id,
        "status": "staged",
        "compiled_code": code,
        "dry_run": body.dry_run,
        "message": "Staged run ready. Review the preview and approve or reject.",
    }


@router.get("/runs/{run_id}")
async def get_run_status(run_id: str):
    run = _RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@router.get("/runs/{run_id}/preview")
async def get_preview(run_id: str, limit: int = 100):
    """Return the preview DataFrame from the staged run."""
    run = _RUNS.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    # In production: read from staging Delta path
    # For now: return mock preview
    return {
        "run_id": run_id,
        "rows": [],
        "schema": [],
        "schema_diff": {"added": [], "removed": [], "type_changed": []},
        "row_count": 0,
        "message": "Preview available after real Spark execution.",
    }


@router.websocket("/runs/{run_id}/log")
async def log_stream(websocket: WebSocket, run_id: str):
    """WebSocket endpoint for real-time execution log streaming."""
    await websocket.accept()
    run = _RUNS.get(run_id)
    if not run:
        await websocket.send_json({"error": "Run not found"})
        await websocket.close()
        return
    try:
        # In production: subscribe to StepTracer events for this run_id
        # For now: send current steps and close
        await websocket.send_json({"run_id": run_id, "steps": run.get("steps", [])})
        await websocket.send_json({"type": "done", "run_id": run_id})
    except WebSocketDisconnect:
        pass
