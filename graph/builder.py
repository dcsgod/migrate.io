"""
graph/builder.py
Builds the schema relationship graph from source + destination connectors.
"""
from __future__ import annotations

import hashlib
import uuid
from typing import TYPE_CHECKING, Any

import structlog

from graph.models import EdgeKind, GraphEdge, GraphNode, JoinKey, SchemaGraph
from graph.store import GraphStore

if TYPE_CHECKING:
    from connectors.base.connector import Connector
    from connectors.base.introspector import SchemaIntrospector

logger = structlog.get_logger(__name__)


class GraphBuilder:
    """
    Crawls source and destination connectors, builds GraphNodes for each
    discovered object, detects explicit FK edges, and returns a SchemaGraph.

    Usage:
        builder = GraphBuilder(source_connector, dest_connector)
        graph   = builder.build()
    """

    def __init__(
        self,
        source: "Connector",
        destination: "Connector",
        source_introspector: "SchemaIntrospector | None" = None,
        dest_introspector: "SchemaIntrospector | None" = None,
        max_objects: int = 500,
    ) -> None:
        self._source = source
        self._dest = destination
        self._source_introspector = source_introspector
        self._dest_introspector = dest_introspector
        self._max_objects = max_objects

    def build(self, existing_graph: SchemaGraph | None = None) -> SchemaGraph:
        """
        Build (or refresh) the schema graph.

        If `existing_graph` is provided, only newly discovered nodes are added
        and schema drift is detected for existing ones.
        """
        graph_id = str(uuid.uuid4())
        store = GraphStore()
        if existing_graph:
            store = GraphStore.from_schema_graph(existing_graph)

        logger.info("graph_builder.crawling_source", connector=self._source.connector_id)
        source_nodes = self._crawl(self._source, self._source_introspector, existing_graph)

        logger.info("graph_builder.crawling_dest", connector=self._dest.connector_id)
        dest_nodes = self._crawl(self._dest, self._dest_introspector, existing_graph)

        all_nodes = source_nodes + dest_nodes
        drift = []

        for node in all_nodes:
            existing = store.get_node(node.id)
            if existing and existing_graph:
                # Drift detection
                if existing.schema_fingerprint() != node.schema_fingerprint():
                    from graph.models import SchemaDriftEntry
                    old_cols = set(existing.column_names())
                    new_cols = set(node.column_names())
                    entry = SchemaDriftEntry(
                        node_id=node.id,
                        node_name=node.name,
                        old_fingerprint=existing.schema_fingerprint(),
                        new_fingerprint=node.schema_fingerprint(),
                        added_columns=list(new_cols - old_cols),
                        removed_columns=list(old_cols - new_cols),
                        type_changed_columns=[
                            c.name
                            for c in node.columns
                            if (ec := existing.get_column(c.name)) and ec.dtype != c.dtype
                        ],
                    )
                    drift.append(entry)
                    logger.warning(
                        "graph_builder.schema_drift",
                        node=node.name,
                        added=entry.added_columns,
                        removed=entry.removed_columns,
                    )
            store.add_node(node)

        # Build explicit FK edges (RDBMS-style)
        explicit_edges = self._build_explicit_edges(all_nodes)
        for edge in explicit_edges:
            store.add_edge(edge)

        logger.info(
            "graph_builder.complete",
            nodes=store.node_count(),
            edges=store.edge_count(),
            drift_count=len(drift),
        )

        schema_graph = store.to_schema_graph(
            graph_id=graph_id,
            source_id=self._source.connector_id,
            dest_id=self._dest.connector_id,
        )
        schema_graph.drift = drift
        return schema_graph

    def _crawl(
        self,
        connector: "Connector",
        introspector: "SchemaIntrospector | None",
        existing_graph: SchemaGraph | None,
    ) -> list[GraphNode]:
        """Crawl one connector and return GraphNodes."""
        if introspector:
            return introspector.crawl(max_objects=self._max_objects)

        # Fallback: use connector's own list_objects + read_schema
        nodes: list[GraphNode] = []
        objects = connector.list_objects()
        for obj in objects[: self._max_objects]:
            try:
                node = connector.read_schema(obj["id"])
                nodes.append(node)
            except Exception as exc:
                logger.warning(
                    "graph_builder.skip_object",
                    object_id=obj["id"],
                    reason=str(exc),
                )
        return nodes

    def _build_explicit_edges(self, nodes: list[GraphNode]) -> list[GraphEdge]:
        """
        Detect explicit FK relationships from ColumnDef.foreign_key declarations.
        FK format: "<qualified_name>.<column_name>"
        """
        edges: list[GraphEdge] = []
        # Build lookup: qualified_name → node
        by_qualified: dict[str, GraphNode] = {n.qualified_name: n for n in nodes}

        for node in nodes:
            for col in node.columns:
                if not col.foreign_key:
                    continue
                # Parse "schema.table.column"
                parts = col.foreign_key.rsplit(".", 1)
                if len(parts) != 2:
                    continue
                ref_table_qname, ref_col_name = parts
                target_node = by_qualified.get(ref_table_qname)
                if target_node is None:
                    continue

                edge_id = GraphEdge.make_id(node.id, target_node.id)
                edge = GraphEdge(
                    id=edge_id,
                    source_node_id=node.id,
                    target_node_id=target_node.id,
                    kind=EdgeKind.EXPLICIT,
                    join_keys=[JoinKey(source_column=col.name, target_column=ref_col_name, confidence=1.0)],
                    confidence=1.0,
                    reasoning=f"Explicit FK: {node.qualified_name}.{col.name} → {col.foreign_key}",
                )
                edges.append(edge)
        return edges
