"""
api/routes/dag.py
DAG build, validate, patch, and Spark code compilation.
"""
from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from dag.builder import DAGBuilder
from dag.nodes import DAG
from dag.optimizer import DAGOptimizer
from dag.validator import DAGValidator

router = APIRouter()

_DAGS: dict[str, DAG] = {}


class DAGBuildRequest(BaseModel):
    graph_id: str
    intent_id: str


class DAGPatchRequest(BaseModel):
    op: str           # add_filter | remove_node | edit_join | add_transform
    node_id: str | None = None
    params: dict[str, Any] = {}


@router.post("/build")
async def build_dag(body: DAGBuildRequest):
    """Build a logical DAG from a grounded intent."""
    from api.routes.graph import _GRAPHS
    from api.routes.commands import _GROUNDED

    graph = _GRAPHS.get(body.graph_id)
    if not graph:
        raise HTTPException(status_code=404, detail="Graph not found")
    grounded = _GROUNDED.get(body.intent_id)
    if not grounded:
        raise HTTPException(status_code=404, detail="Grounded intent not found")
    if grounded.needs_user_confirmation:
        raise HTTPException(
            status_code=422,
            detail="Intent has unconfirmed low-confidence mappings. Confirm them before building the DAG.",
        )

    try:
        builder = DAGBuilder(graph)
        dag = builder.build(grounded)

        # Auto-optimize
        optimizer = DAGOptimizer()
        dag = optimizer.optimize(dag)

        # Validate
        validator = DAGValidator()
        validation = validator.validate(dag)

        _DAGS[dag.id] = dag

        return {
            "dag_id": dag.id,
            "node_count": len(dag.nodes),
            "edge_count": len(dag.edges),
            "validation": validation.to_dict(),
            "dag": dag.to_dict(),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{dag_id}")
async def get_dag(dag_id: str):
    dag = _DAGS.get(dag_id)
    if not dag:
        raise HTTPException(status_code=404, detail="DAG not found")
    return dag.to_dict()


@router.get("/{dag_id}/validate")
async def validate_dag(dag_id: str):
    dag = _DAGS.get(dag_id)
    if not dag:
        raise HTTPException(status_code=404, detail="DAG not found")
    validator = DAGValidator()
    result = validator.validate(dag)
    return result.to_dict()


@router.get("/{dag_id}/compiled")
async def get_compiled_code(dag_id: str, target: str = "spark"):
    """Return the compiled PySpark or SQL code for this DAG."""
    dag = _DAGS.get(dag_id)
    if not dag:
        raise HTTPException(status_code=404, detail="DAG not found")

    try:
        if target == "spark":
            from compiler.spark_compiler import SparkCompiler
            compiler = SparkCompiler()
            code = compiler.compile(dag, plan_id=dag_id)
        elif target == "sql":
            from compiler.sql_compiler import SQLCompiler
            compiler = SQLCompiler()
            code = compiler.compile(dag)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown target: {target!r}")

        return {"dag_id": dag_id, "target": target, "code": code, "language": "python" if target == "spark" else "sql"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.patch("/{dag_id}")
async def patch_dag(dag_id: str, body: DAGPatchRequest):
    """Apply an incremental edit to an existing DAG without full re-parse."""
    dag = _DAGS.get(dag_id)
    if not dag:
        raise HTTPException(status_code=404, detail="DAG not found")
    try:
        from dag.patch import DAGPatcher
        patcher = DAGPatcher()
        dag = patcher.apply(dag, body.op, body.node_id, body.params)
        _DAGS[dag_id] = dag
        return {"dag_id": dag_id, "op": body.op, "dag": dag.to_dict()}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


def get_dag_or_404(dag_id: str) -> DAG:
    d = _DAGS.get(dag_id)
    if not d:
        raise HTTPException(status_code=404, detail=f"DAG {dag_id!r} not found")
    return d
