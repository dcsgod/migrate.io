"""
graph/models.py
Pydantic domain models for the schema relationship graph.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


# ─────────────────────────────────────────────────────────────
# Column / field descriptors
# ─────────────────────────────────────────────────────────────

class ColumnType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    BINARY = "binary"
    STRUCT = "struct"
    ARRAY = "array"
    MAP = "map"
    DECIMAL = "decimal"
    UNKNOWN = "unknown"


class ColumnDef(BaseModel):
    name: str
    dtype: ColumnType = ColumnType.UNKNOWN
    nullable: bool = True
    primary_key: bool = False
    foreign_key: str | None = None          # "<node_id>.<column_name>"
    description: str | None = None
    sample_values: list[Any] = Field(default_factory=list)
    stats: dict[str, Any] = Field(default_factory=dict)  # null_pct, distinct_count, …


# ─────────────────────────────────────────────────────────────
# Graph nodes
# ─────────────────────────────────────────────────────────────

class NodeKind(str, Enum):
    TABLE = "table"
    FILE = "file"
    TOPIC = "topic"
    OBJECT = "object"   # ERP business object


class GraphNode(BaseModel):
    id: str                                    # unique across source+dest
    name: str                                  # table / file / topic name
    qualified_name: str                        # schema.table or bucket/prefix
    connector_id: str                          # which connector owns this node
    kind: NodeKind = NodeKind.TABLE
    columns: list[ColumnDef] = Field(default_factory=list)
    row_count: int | None = None
    size_bytes: int | None = None
    partition_keys: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    discovered_at: datetime = Field(default_factory=datetime.utcnow)

    @classmethod
    def make_id(cls, connector_id: str, qualified_name: str) -> str:
        raw = f"{connector_id}:{qualified_name}"
        return hashlib.sha1(raw.encode()).hexdigest()[:16]

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def get_column(self, name: str) -> ColumnDef | None:
        return next((c for c in self.columns if c.name == name), None)

    def schema_fingerprint(self) -> str:
        """Hash of column names+types for drift detection."""
        sig = json.dumps(
            [(c.name, c.dtype.value) for c in sorted(self.columns, key=lambda c: c.name)]
        )
        return hashlib.md5(sig.encode()).hexdigest()


# ─────────────────────────────────────────────────────────────
# Graph edges
# ─────────────────────────────────────────────────────────────

class EdgeKind(str, Enum):
    EXPLICIT = "explicit"       # FK declared in schema
    INFERRED_NAME = "inferred_name"   # name-similarity match
    INFERRED_VALUE = "inferred_value" # value-overlap match
    LLM_SUGGESTED = "llm_suggested"  # LLM heuristic


class JoinKey(BaseModel):
    source_column: str
    target_column: str
    confidence: float = 1.0    # 0.0–1.0


class GraphEdge(BaseModel):
    id: str
    source_node_id: str
    target_node_id: str
    kind: EdgeKind = EdgeKind.EXPLICIT
    join_keys: list[JoinKey] = Field(default_factory=list)
    confidence: float = 1.0    # overall edge confidence
    reasoning: str | None = None   # XAI: why this edge was created
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="after")
    def _check_confidence(self) -> "GraphEdge":
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("confidence must be in [0, 1]")
        return self

    @classmethod
    def make_id(cls, source_node_id: str, target_node_id: str) -> str:
        raw = f"{source_node_id}→{target_node_id}"
        return hashlib.sha1(raw.encode()).hexdigest()[:16]

    @property
    def is_high_confidence(self) -> bool:
        return self.confidence >= 0.85

    @property
    def needs_user_confirmation(self) -> bool:
        return not self.is_high_confidence and self.kind != EdgeKind.EXPLICIT


# ─────────────────────────────────────────────────────────────
# Schema graph
# ─────────────────────────────────────────────────────────────

class SchemaDriftEntry(BaseModel):
    node_id: str
    node_name: str
    old_fingerprint: str
    new_fingerprint: str
    added_columns: list[str] = Field(default_factory=list)
    removed_columns: list[str] = Field(default_factory=list)
    type_changed_columns: list[str] = Field(default_factory=list)
    detected_at: datetime = Field(default_factory=datetime.utcnow)


class SchemaGraph(BaseModel):
    id: str
    connection_pair_hash: str   # hash(source_id + dest_id) — for Neo4j keying
    nodes: dict[str, GraphNode] = Field(default_factory=dict)  # node_id → node
    edges: dict[str, GraphEdge] = Field(default_factory=dict)  # edge_id → edge
    drift: list[SchemaDriftEntry] = Field(default_factory=list)
    built_at: datetime = Field(default_factory=datetime.utcnow)
    source_connector_id: str = ""
    dest_connector_id: str = ""

    def add_node(self, node: GraphNode) -> None:
        self.nodes[node.id] = node

    def add_edge(self, edge: GraphEdge) -> None:
        self.edges[edge.id] = edge

    def get_node(self, node_id: str) -> GraphNode | None:
        return self.nodes.get(node_id)

    def get_edges_for_node(self, node_id: str) -> list[GraphEdge]:
        return [
            e for e in self.edges.values()
            if e.source_node_id == node_id or e.target_node_id == node_id
        ]

    def low_confidence_edges(self, threshold: float = 0.85) -> list[GraphEdge]:
        return [e for e in self.edges.values() if e.confidence < threshold]

    def node_count(self) -> int:
        return len(self.nodes)

    def edge_count(self) -> int:
        return len(self.edges)

    @classmethod
    def make_connection_hash(cls, source_id: str, dest_id: str) -> str:
        raw = f"{source_id}||{dest_id}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]
