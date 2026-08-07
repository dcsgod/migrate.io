"""
graph/store.py
In-memory networkx graph backend.
"""
from __future__ import annotations

from typing import Any

import networkx as nx

from graph.models import GraphEdge, GraphNode, SchemaGraph


class GraphStore:
    """
    Thin wrapper around a networkx DiGraph that stores GraphNode / GraphEdge
    domain objects as node/edge attributes.

    This is the in-memory default. For persistence, see graph/persistence.py.
    """

    def __init__(self) -> None:
        self._g: nx.DiGraph = nx.DiGraph()

    # ── Node operations ───────────────────────────────────────

    def add_node(self, node: GraphNode) -> None:
        self._g.add_node(node.id, data=node)

    def get_node(self, node_id: str) -> GraphNode | None:
        if node_id in self._g:
            return self._g.nodes[node_id]["data"]
        return None

    def remove_node(self, node_id: str) -> None:
        if node_id in self._g:
            self._g.remove_node(node_id)

    def all_nodes(self) -> list[GraphNode]:
        return [self._g.nodes[n]["data"] for n in self._g.nodes]

    def nodes_by_connector(self, connector_id: str) -> list[GraphNode]:
        return [n for n in self.all_nodes() if n.connector_id == connector_id]

    # ── Edge operations ───────────────────────────────────────

    def add_edge(self, edge: GraphEdge) -> None:
        self._g.add_edge(edge.source_node_id, edge.target_node_id, data=edge, key=edge.id)

    def get_edge(self, edge_id: str) -> GraphEdge | None:
        for _, _, data in self._g.edges(data=True):
            if data.get("data") and data["data"].id == edge_id:
                return data["data"]
        return None

    def all_edges(self) -> list[GraphEdge]:
        return [data["data"] for _, _, data in self._g.edges(data=True) if "data" in data]

    def edges_from(self, node_id: str) -> list[GraphEdge]:
        return [
            data["data"]
            for _, _, data in self._g.out_edges(node_id, data=True)
            if "data" in data
        ]

    def edges_to(self, node_id: str) -> list[GraphEdge]:
        return [
            data["data"]
            for _, _, data in self._g.in_edges(node_id, data=True)
            if "data" in data
        ]

    def edges_for_node(self, node_id: str) -> list[GraphEdge]:
        return self.edges_from(node_id) + self.edges_to(node_id)

    # ── Graph queries ─────────────────────────────────────────

    def neighbors(self, node_id: str) -> list[GraphNode]:
        result = []
        for n in list(self._g.successors(node_id)) + list(self._g.predecessors(node_id)):
            node = self.get_node(n)
            if node:
                result.append(node)
        return result

    def shortest_join_path(self, source_id: str, target_id: str) -> list[str] | None:
        """Return list of node IDs forming the shortest join path, or None."""
        try:
            return nx.shortest_path(self._g.to_undirected(), source_id, target_id)
        except nx.NetworkXNoPath:
            return None

    def has_cycle(self) -> bool:
        return not nx.is_directed_acyclic_graph(self._g)

    def connected_components(self) -> list[list[str]]:
        return list(nx.connected_components(self._g.to_undirected()))

    # ── Serialisation ─────────────────────────────────────────

    def to_schema_graph(self, graph_id: str, source_id: str, dest_id: str) -> SchemaGraph:
        nodes = {n.id: n for n in self.all_nodes()}
        edges = {e.id: e for e in self.all_edges()}
        return SchemaGraph(
            id=graph_id,
            connection_pair_hash=SchemaGraph.make_connection_hash(source_id, dest_id),
            nodes=nodes,
            edges=edges,
            source_connector_id=source_id,
            dest_connector_id=dest_id,
        )

    @classmethod
    def from_schema_graph(cls, schema_graph: SchemaGraph) -> "GraphStore":
        store = cls()
        for node in schema_graph.nodes.values():
            store.add_node(node)
        for edge in schema_graph.edges.values():
            store.add_edge(edge)
        return store

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": [n.model_dump(mode="json") for n in self.all_nodes()],
            "edges": [e.model_dump(mode="json") for e in self.all_edges()],
        }

    def node_count(self) -> int:
        return self._g.number_of_nodes()

    def edge_count(self) -> int:
        return self._g.number_of_edges()

    def __repr__(self) -> str:
        return f"GraphStore(nodes={self.node_count()}, edges={self.edge_count()})"
