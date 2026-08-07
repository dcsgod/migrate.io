"""
api/routes/graph.py
Schema graph build, retrieve, visualize, and save to Neo4j.
"""
from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from graph.builder import GraphBuilder
from graph.models import SchemaGraph

router = APIRouter()

# In-memory graph store (keyed by graph_id)
_GRAPHS: dict[str, SchemaGraph] = {}


class GraphBuildRequest(BaseModel):
    source_connection_id: str
    dest_connection_id: str
    run_inference: bool = False
    save_to_neo4j: bool = False
    tenant_id: str = "default"


@router.post("/build")
async def build_graph(body: GraphBuildRequest):
    """Build the schema relationship graph for a source+destination pair."""
    from api.routes.connections import _CONNECTOR_CACHE

    source = _CONNECTOR_CACHE.get(body.source_connection_id)
    dest = _CONNECTOR_CACHE.get(body.dest_connection_id)
    if not source:
        raise HTTPException(status_code=404, detail=f"Source connection not found: {body.source_connection_id}")
    if not dest:
        raise HTTPException(status_code=404, detail=f"Dest connection not found: {body.dest_connection_id}")

    try:
        # Check Neo4j cache first
        graph = None
        pair_hash = SchemaGraph.make_connection_hash(source.connector_id, dest.connector_id)

        if body.save_to_neo4j or True:  # check cache regardless
            try:
                from graph.persistence import GraphPersistence
                import os
                persistence = GraphPersistence(
                    uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
                    user=os.environ.get("NEO4J_USER", "neo4j"),
                    password=os.environ.get("NEO4J_PASSWORD", ""),
                    tenant_id=body.tenant_id,
                )
                persistence.connect()
                if persistence.exists(pair_hash):
                    graph = persistence.load(pair_hash)
            except Exception:
                pass  # Neo4j not available — build fresh

        if not graph:
            builder = GraphBuilder(source, dest)
            graph = builder.build()

        # Run inference if requested
        if body.run_inference:
            from graph.inference import EdgeInferrer
            from graph.store import GraphStore
            store = GraphStore.from_schema_graph(graph)
            inferrer = EdgeInferrer(store)
            inferred_edges = inferrer.infer_all()
            for edge in inferred_edges:
                store.add_edge(edge)
            graph = store.to_schema_graph(
                graph.id, source.connector_id, dest.connector_id
            )

        # Save to Neo4j if requested
        if body.save_to_neo4j:
            try:
                persistence.save(graph)
            except Exception as exc:
                pass  # Don't fail the build if Neo4j is unavailable

        _GRAPHS[graph.id] = graph

        return {
            "graph_id": graph.id,
            "connection_pair_hash": graph.connection_pair_hash,
            "node_count": graph.node_count(),
            "edge_count": graph.edge_count(),
            "drift_count": len(graph.drift),
            "low_confidence_edges": len(graph.low_confidence_edges()),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{graph_id}")
async def get_graph(graph_id: str):
    """Return the full graph JSON for visualization."""
    graph = _GRAPHS.get(graph_id)
    if not graph:
        raise HTTPException(status_code=404, detail="Graph not found")
    return {
        "graph_id": graph.id,
        "nodes": [n.model_dump(mode="json") for n in graph.nodes.values()],
        "edges": [e.model_dump(mode="json") for e in graph.edges.values()],
        "drift": [d.model_dump(mode="json") for d in graph.drift],
        "built_at": graph.built_at.isoformat(),
    }


@router.get("/{graph_id}/nodes")
async def list_nodes(graph_id: str, connector_id: str | None = None):
    graph = _GRAPHS.get(graph_id)
    if not graph:
        raise HTTPException(status_code=404, detail="Graph not found")
    nodes = list(graph.nodes.values())
    if connector_id:
        nodes = [n for n in nodes if n.connector_id == connector_id]
    return [n.model_dump(mode="json") for n in nodes]


@router.get("/{graph_id}/edges")
async def list_edges(graph_id: str, min_confidence: float = 0.0):
    graph = _GRAPHS.get(graph_id)
    if not graph:
        raise HTTPException(status_code=404, detail="Graph not found")
    edges = [e for e in graph.edges.values() if e.confidence >= min_confidence]
    return [e.model_dump(mode="json") for e in edges]


@router.get("/{graph_id}/drift")
async def get_drift(graph_id: str):
    graph = _GRAPHS.get(graph_id)
    if not graph:
        raise HTTPException(status_code=404, detail="Graph not found")
    return [d.model_dump(mode="json") for d in graph.drift]


def get_graph_or_404(graph_id: str) -> SchemaGraph:
    g = _GRAPHS.get(graph_id)
    if not g:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id!r} not found")
    return g
