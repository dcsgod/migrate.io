"""
dag/versioning.py
Plan version history — store, list, and rollback approved plans.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from dag.nodes import DAG

logger = structlog.get_logger(__name__)


class PlanVersion:
    def __init__(
        self,
        plan_id: str,
        dag: DAG,
        compiled_code: str,
        label: str = "",
        approved_by: str = "",
        tags: list[str] | None = None,
    ) -> None:
        self.plan_id = plan_id
        self.version = hashlib.sha1(compiled_code.encode()).hexdigest()[:12]
        self.dag = dag
        self.compiled_code = compiled_code
        self.label = label
        self.approved_by = approved_by
        self.tags = tags or []
        self.created_at = datetime.utcnow()

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "version": self.version,
            "label": self.label,
            "approved_by": self.approved_by,
            "tags": self.tags,
            "created_at": self.created_at.isoformat(),
            "dag_id": self.dag.id,
            "node_count": len(self.dag.nodes),
        }


class PlanVersionStore:
    """
    Stores approved plan snapshots (JSON) on local disk.
    In production: swap storage_dir for S3 / DBFS / Azure Blob.
    """

    def __init__(self, storage_dir: str = "/tmp/migrate_io/plan_versions") -> None:
        self._dir = Path(storage_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._versions: list[PlanVersion] = []
        self._load_existing()

    def _load_existing(self) -> None:
        for path in sorted(self._dir.glob("*.json")):
            try:
                data = json.loads(path.read_text())
                # Minimal re-hydration for listing
                pv = PlanVersion.__new__(PlanVersion)
                pv.__dict__.update(data)
                pv.dag = DAG(id=data.get("dag_id", ""))
                self._versions.append(pv)
            except Exception:
                pass

    def save(
        self,
        dag: DAG,
        compiled_code: str,
        label: str = "",
        approved_by: str = "",
        tags: list[str] | None = None,
    ) -> PlanVersion:
        plan_id = str(uuid.uuid4())
        version = PlanVersion(plan_id, dag, compiled_code, label, approved_by, tags)
        self._versions.append(version)

        payload = {
            **version.to_dict(),
            "compiled_code": compiled_code,
            "dag": dag.to_dict(),
        }
        (self._dir / f"{plan_id}.json").write_text(json.dumps(payload, indent=2))
        logger.info("plan_version.saved", plan_id=plan_id, version=version.version)
        return version

    def list_versions(
        self,
        limit: int = 50,
        tags: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        versions = self._versions
        if tags:
            versions = [v for v in versions if any(t in getattr(v, "tags", []) for t in tags)]
        versions = sorted(versions, key=lambda v: getattr(v, "created_at", ""), reverse=True)
        return [v.to_dict() for v in versions[:limit]]

    def load(self, plan_id: str) -> dict[str, Any] | None:
        path = self._dir / f"{plan_id}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def rollback(self, plan_id: str) -> dict[str, Any]:
        """Load a past plan version for re-execution."""
        plan = self.load(plan_id)
        if not plan:
            raise KeyError(f"Plan version {plan_id!r} not found")
        logger.info("plan_version.rollback", plan_id=plan_id)
        return plan
