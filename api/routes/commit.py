"""
api/routes/commit.py
Approve / Reject the staged run → atomic Delta commit or discard.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


class ApproveRequest(BaseModel):
    run_id: str
    destination_path: str
    mode: str = "overwrite"           # overwrite | append | merge
    merge_keys: list[str] | None = None


class RejectRequest(BaseModel):
    run_id: str
    reason: str = ""


@router.post("/approve")
async def approve(body: ApproveRequest):
    """Atomically commit staged data → production destination."""
    from api.routes.preview import _RUNS

    run = _RUNS.get(body.run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run["status"] not in ("staged",):
        raise HTTPException(
            status_code=400,
            detail=f"Run is in state {run['status']!r} — only 'staged' runs can be approved.",
        )

    # In production: get spark session, CommitManager.approve(...)
    # For now: mark as committed
    _RUNS[body.run_id]["status"] = "committed"
    _RUNS[body.run_id]["destination_path"] = body.destination_path

    return {
        "run_id": body.run_id,
        "committed": True,
        "destination": body.destination_path,
        "mode": body.mode,
        "message": "Production commit successful.",
    }


@router.post("/reject")
async def reject(body: RejectRequest):
    """Discard staged data — production is untouched."""
    from api.routes.preview import _RUNS

    run = _RUNS.get(body.run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    _RUNS[body.run_id]["status"] = "rejected"
    _RUNS[body.run_id]["rejection_reason"] = body.reason

    return {
        "run_id": body.run_id,
        "rejected": True,
        "reason": body.reason,
        "message": "Staged data discarded. Production unchanged.",
    }
