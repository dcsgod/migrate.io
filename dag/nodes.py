"""
dag/nodes.py
DAGNode types: the engine-agnostic building blocks of every pipeline.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class NodeStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    ERROR = "error"
    SKIPPED = "skipped"


class SchemaField(BaseModel):
    name: str
    dtype: str
    nullable: bool = True


class DAGNode(BaseModel, ABC):
    """Base class for all DAG nodes."""
    id: str
    label: str = ""
    status: NodeStatus = NodeStatus.PENDING
    input_schema: list[SchemaField] = Field(default_factory=list)
    output_schema: list[SchemaField] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    # Runtime metrics (filled during execution)
    rows_in: int | None = None
    rows_out: int | None = None
    duration_ms: int | None = None
    error_message: str | None = None

    model_config = {"arbitrary_types_allowed": True}

    @abstractmethod
    def node_type(self) -> str:
        """Return the node type string."""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.node_type(),
            "label": self.label,
            "status": self.status.value,
            "input_schema": [f.model_dump() for f in self.input_schema],
            "output_schema": [f.model_dump() for f in self.output_schema],
            "metadata": self.metadata,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "duration_ms": self.duration_ms,
            "error_message": self.error_message,
        }


# ─────────────────────────────────────────────────────────────
# Concrete node types
# ─────────────────────────────────────────────────────────────

class ReadNode(DAGNode):
    """Read from a source connector."""
    connector_id: str
    object_id: str
    format: str = "table"
    read_options: dict[str, Any] = Field(default_factory=dict)
    # Incremental / CDC options
    watermark_column: str | None = None
    last_watermark: Any = None

    def node_type(self) -> str:
        return "read"

    @property
    def source_path(self) -> str:
        return f"{self.connector_id}:{self.object_id}"


class FilterNode(DAGNode):
    """Apply a WHERE clause to a DataFrame."""
    predicate: str          # SQL expression e.g. "status != 'cancelled'"
    parent_id: str          # ID of the node whose output we filter

    def node_type(self) -> str:
        return "filter"


class JoinNode(DAGNode):
    """Join two DataFrames."""
    left_id: str            # ID of left parent node
    right_id: str           # ID of right parent node
    left_key: str
    right_key: str
    join_type: str = "inner"   # inner | left | right | full | cross
    additional_conditions: str | None = None   # extra SQL predicate

    def node_type(self) -> str:
        return "join"


class TransformNode(DAGNode):
    """Apply a pluggable transform op to a DataFrame."""
    op: str                 # join | dedupe | pivot | scd_merge | type_cast | mask | custom_expr
    params: dict[str, Any] = Field(default_factory=dict)
    parent_id: str

    def node_type(self) -> str:
        return "transform"


class AggregateNode(DAGNode):
    """GROUP BY + aggregate."""
    parent_id: str
    group_by: list[str]
    aggregations: list[dict[str, str]]  # [{"col": "amount", "func": "sum", "alias": "total"}]

    def node_type(self) -> str:
        return "aggregate"


class UnionNode(DAGNode):
    """UNION ALL of multiple parent nodes."""
    parent_ids: list[str]
    distinct: bool = False

    def node_type(self) -> str:
        return "union"


class SelectNode(DAGNode):
    """SELECT / column projection and renaming."""
    parent_id: str
    expressions: list[dict[str, str]]   # [{"expr": "UPPER(name)", "alias": "name"}]

    def node_type(self) -> str:
        return "select"


class QualityGateNode(DAGNode):
    """Data quality check — null%, duplicate, referential integrity."""
    parent_id: str
    checks: list[dict[str, Any]]   # [{"type": "null_pct", "column": "email", "threshold": 0.05}]
    fail_on_error: bool = True

    def node_type(self) -> str:
        return "quality_gate"


class WriteNode(DAGNode):
    """Write to a destination connector."""
    connector_id: str
    object_id: str
    mode: str = "overwrite"           # overwrite | append | merge | error
    write_options: dict[str, Any] = Field(default_factory=dict)
    parent_id: str
    staging: bool = True              # always write to staging first

    def node_type(self) -> str:
        return "write"


# ─────────────────────────────────────────────────────────────
# DAG container
# ─────────────────────────────────────────────────────────────

class DAG(BaseModel):
    """Engine-agnostic directed acyclic graph of DAGNodes."""
    id: str
    nodes: dict[str, DAGNode] = Field(default_factory=dict)
    edges: list[tuple[str, str]] = Field(default_factory=list)   # (parent_id, child_id)
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}

    def add_node(self, node: DAGNode) -> None:
        self.nodes[node.id] = node

    def add_edge(self, parent_id: str, child_id: str) -> None:
        self.edges.append((parent_id, child_id))

    def get_node(self, node_id: str) -> DAGNode | None:
        return self.nodes.get(node_id)

    def parents_of(self, node_id: str) -> list[DAGNode]:
        return [self.nodes[p] for p, c in self.edges if c == node_id and p in self.nodes]

    def children_of(self, node_id: str) -> list[DAGNode]:
        return [self.nodes[c] for p, c in self.edges if p == node_id and c in self.nodes]

    def topological_order(self) -> list[DAGNode]:
        """Return nodes in topological order for execution."""
        import networkx as nx
        g = nx.DiGraph()
        for node_id in self.nodes:
            g.add_node(node_id)
        for p, c in self.edges:
            g.add_edge(p, c)
        ordered_ids = list(nx.topological_sort(g))
        return [self.nodes[nid] for nid in ordered_ids if nid in self.nodes]

    def read_nodes(self) -> list[ReadNode]:
        return [n for n in self.nodes.values() if isinstance(n, ReadNode)]

    def write_nodes(self) -> list[WriteNode]:
        return [n for n in self.nodes.values() if isinstance(n, WriteNode)]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "nodes": [n.to_dict() for n in self.nodes.values()],
            "edges": [{"from": p, "to": c} for p, c in self.edges],
            "metadata": self.metadata,
        }
