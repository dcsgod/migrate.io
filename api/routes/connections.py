"""
api/routes/connections.py
Connection registration, status, schema listing.
"""
from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from connectors import build_connector, list_connector_types

router = APIRouter()

# In-memory registry (replace with DB in production)
_CONNECTIONS: dict[str, dict[str, Any]] = {}
_CONNECTOR_CACHE: dict[str, Any] = {}


class ConnectionCreate(BaseModel):
    connector_type: str
    name: str
    config: dict[str, Any]
    tenant_id: str = "default"


class ConnectionResponse(BaseModel):
    id: str
    connector_type: str
    name: str
    tenant_id: str
    is_connected: bool
    capabilities: dict[str, Any]


@router.get("/types")
async def list_types():
    """List all available connector types."""
    return {"types": list_connector_types()}


@router.post("/", response_model=ConnectionResponse)
async def create_connection(body: ConnectionCreate):
    """Register and connect a new source or destination connector."""
    conn_id = str(uuid.uuid4())
    try:
        connector = build_connector(body.connector_type, conn_id, body.config)
        connector.connect()
        caps = connector.capabilities()
        _CONNECTIONS[conn_id] = {
            "id": conn_id,
            "connector_type": body.connector_type,
            "name": body.name,
            "config": body.config,
            "tenant_id": body.tenant_id,
        }
        _CONNECTOR_CACHE[conn_id] = connector
        return ConnectionResponse(
            id=conn_id,
            connector_type=body.connector_type,
            name=body.name,
            tenant_id=body.tenant_id,
            is_connected=connector.is_connected,
            capabilities=vars(caps),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{connection_id}/status")
async def get_status(connection_id: str):
    conn = _CONNECTOR_CACHE.get(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    return conn.health_check()


@router.get("/{connection_id}/objects")
async def list_objects(connection_id: str):
    conn = _CONNECTOR_CACHE.get(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        return {"connection_id": connection_id, "objects": conn.list_objects()}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{connection_id}/schema/{object_id:path}")
async def get_schema(connection_id: str, object_id: str):
    conn = _CONNECTOR_CACHE.get(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        node = conn.read_schema(object_id)
        return node.model_dump(mode="json")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.delete("/{connection_id}")
async def delete_connection(connection_id: str):
    conn = _CONNECTOR_CACHE.pop(connection_id, None)
    if conn:
        conn.close()
    _CONNECTIONS.pop(connection_id, None)
    return {"deleted": connection_id}


def get_connector(connection_id: str):
    """Dependency helper for other routes."""
    conn = _CONNECTOR_CACHE.get(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Connection {connection_id!r} not found")
    return conn
