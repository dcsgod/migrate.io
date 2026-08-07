"""
api/routes/commands.py
NL command parsing → IntentJSON → GroundedIntent → XAI explainability.
"""
from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from intent.explainability import ExplainabilityTracer
from intent.grounding import IntentGrounder
from intent.parser import IntentParser
from intent.schema import GroundedIntent, IntentJSON

router = APIRouter()

# In-memory state
_INTENTS: dict[str, IntentJSON] = {}
_GROUNDED: dict[str, GroundedIntent] = {}
_EXPLANATIONS: dict[str, dict] = {}

_parser: IntentParser | None = None


def _get_parser() -> IntentParser:
    global _parser
    if _parser is None:
        _parser = IntentParser()
    return _parser


class CommandRequest(BaseModel):
    graph_id: str
    nl_command: str
    tenant_id: str = "default"


class ConfirmMappingRequest(BaseModel):
    intent_id: str
    entity_name: str
    confirmed_node_id: str


@router.post("/")
async def submit_command(body: CommandRequest):
    """Parse a NL command against the graph and return intent + grounding."""
    from api.routes.graph import _GRAPHS

    graph = _GRAPHS.get(body.graph_id)
    if not graph:
        raise HTTPException(status_code=404, detail=f"Graph {body.graph_id!r} not found")

    intent_id = str(uuid.uuid4())

    try:
        # Parse NL → raw IntentJSON
        parser = _get_parser()
        graph_dict = {
            "nodes": {k: v.model_dump(mode="json") for k, v in graph.nodes.items()},
            "edges": {k: v.model_dump(mode="json") for k, v in graph.edges.items()},
        }
        raw_intent = parser.parse(body.nl_command, graph_dict)
        _INTENTS[intent_id] = raw_intent

        # Ground intent → resolve to node IDs
        grounder = IntentGrounder(graph)
        grounded = grounder.ground(raw_intent)
        _GROUNDED[intent_id] = grounded

        # XAI explainability
        tracer = ExplainabilityTracer()
        source_nodes = {
            rt.node_id: graph.get_node(rt.node_id)
            for rt in grounded.source_tables
            if rt.node_id
        }
        target_node = graph.get_node(grounded.target_table.node_id) if grounded.target_table.node_id else None

        explanation = {}
        if target_node:
            explanation = tracer.explain(grounded, {k: v for k, v in source_nodes.items() if v}, target_node)
        _EXPLANATIONS[intent_id] = explanation

        return {
            "intent_id": intent_id,
            "intent": raw_intent.model_dump(mode="json"),
            "grounded": grounded.model_dump(mode="json"),
            "explanation": explanation,
            "needs_confirmation": grounded.needs_user_confirmation,
            "unresolved": grounded.unresolved_entities,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{intent_id}")
async def get_intent(intent_id: str):
    intent = _INTENTS.get(intent_id)
    if not intent:
        raise HTTPException(status_code=404, detail="Intent not found")
    return {
        "intent_id": intent_id,
        "intent": intent.model_dump(mode="json"),
        "grounded": _GROUNDED.get(intent_id, {}) and _GROUNDED[intent_id].model_dump(mode="json"),
    }


@router.get("/{intent_id}/explain")
async def get_explanation(intent_id: str):
    if intent_id not in _EXPLANATIONS:
        raise HTTPException(status_code=404, detail="Explanation not found")
    return {"intent_id": intent_id, "explanation": _EXPLANATIONS[intent_id]}


@router.post("/{intent_id}/confirm-mapping")
async def confirm_mapping(intent_id: str, body: ConfirmMappingRequest):
    """User confirms a low-confidence entity mapping."""
    grounded = _GROUNDED.get(intent_id)
    if not grounded:
        raise HTTPException(status_code=404, detail="Intent not found")

    # Update the resolution for the confirmed entity
    for rt in grounded.source_tables:
        if rt.entity_name == body.entity_name:
            rt.node_id = body.confirmed_node_id
            rt.confidence = 1.0
            rt.needs_user_confirmation = False
            break
    if grounded.target_table.entity_name == body.entity_name:
        grounded.target_table.node_id = body.confirmed_node_id
        grounded.target_table.confidence = 1.0
        grounded.target_table.needs_user_confirmation = False

    return {"confirmed": True, "entity": body.entity_name, "resolved_to": body.confirmed_node_id}


def get_grounded_or_404(intent_id: str) -> GroundedIntent:
    g = _GROUNDED.get(intent_id)
    if not g:
        raise HTTPException(status_code=404, detail=f"Grounded intent {intent_id!r} not found")
    return g
