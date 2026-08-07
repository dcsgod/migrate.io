"""
api/routes/plans.py
Plan versioning — list, load, and rollback approved migration plans.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

_PLANS_DIR = Path("/tmp/migrate_io/plans")
_PLANS: dict[str, dict[str, Any]] = {}


class PlanSaveRequest(BaseModel):
    run_id: str
    dag_id: str
    label: str = ""
    tags: list[str] = []


@router.post("/")
async def save_plan(body: PlanSaveRequest):
    """Save an approved plan as a versioned artifact."""
    from api.routes.preview import _RUNS
    from api.routes.dag import _DAGS

    run = _RUNS.get(body.run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    dag = _DAGS.get(body.dag_id)
    if not dag:
        raise HTTPException(status_code=404, detail="DAG not found")

    plan_id = str(uuid.uuid4())
    plan = {
        "plan_id": plan_id,
        "run_id": body.run_id,
        "dag_id": body.dag_id,
        "dag": dag.to_dict(),
        "compiled_code": run.get("compiled_code", ""),
        "label": body.label,
        "tags": body.tags,
        "saved_at": datetime.utcnow().isoformat(),
        "status": run.get("status"),
    }
    _PLANS[plan_id] = plan

    # Persist to disk
    _PLANS_DIR.mkdir(parents=True, exist_ok=True)
    (_PLANS_DIR / f"{plan_id}.json").write_text(json.dumps(plan, indent=2))

    return {"plan_id": plan_id, "saved_at": plan["saved_at"]}


@router.get("/")
async def list_plans(limit: int = 50):
    """List all saved plans, newest first."""
    plans = sorted(
        _PLANS.values(),
        key=lambda p: p["saved_at"],
        reverse=True,
    )[:limit]
    return [
        {
            "plan_id": p["plan_id"],
            "label": p["label"],
            "tags": p["tags"],
            "saved_at": p["saved_at"],
            "status": p["status"],
        }
        for p in plans
    ]


@router.get("/{plan_id}")
async def get_plan(plan_id: str):
    plan = _PLANS.get(plan_id)
    if not plan:
        # Try loading from disk
        path = _PLANS_DIR / f"{plan_id}.json"
        if path.exists():
            plan = json.loads(path.read_text())
            _PLANS[plan_id] = plan
        else:
            raise HTTPException(status_code=404, detail="Plan not found")
    return plan


@router.post("/{plan_id}/rerun")
async def rerun_plan(plan_id: str):
    """Re-execute a past plan version (one-click re-run)."""
    plan = _PLANS.get(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    # In production: reconstruct DAG from plan["dag"] and submit
    return {
        "plan_id": plan_id,
        "message": "Plan re-run submitted. Monitor via /preview/runs endpoint.",
        "new_run_id": str(uuid.uuid4()),
    }
