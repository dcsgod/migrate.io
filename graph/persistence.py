"""
graph/persistence.py
Neo4j-backed graph persistence — save/load SchemaGraph by connection-pair hash.
"""
from __future__ import annotations

import json
from typing import Any

import structlog

from graph.models import (
    ColumnDef,
    ColumnType,
    EdgeKind,
    GraphEdge,
    GraphNode,
    JoinKey,
    NodeKind,
    SchemaGraph,
)

logger = structlog.get_logger(__name__)


class GraphPersistence:
    """
    Saves and loads SchemaGraph objects to/from Neo4j.
    Graphs are namespaced per tenant + connection-pair hash so one
    tenant's graph is never visible to another.

    Neo4j schema:
        (:GraphSnapshot {hash, tenant_id, built_at, source_connector, dest_connector})
        (:SchemaNode {node_id, name, qualified_name, connector_id, kind, columns_json, ...})
        (:SchemaEdge {edge_id, kind, confidence, join_keys_json, reasoning})
        (:SchemaNode)-[:CONNECTED_TO {edge_id}]->(:SchemaNode)
        (:GraphSnapshot)-[:CONTAINS]->(:SchemaNode)
    """

    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        database: str = "neo4j",
        tenant_id: str = "default",
    ) -> None:
        self._uri = uri
        self._user = user
        self._password = password
        self._database = database
        self._tenant_id = tenant_id
        self._driver: Any = None

    def connect(self) -> None:
        try:
            from neo4j import GraphDatabase
            self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))
            self._driver.verify_connectivity()
            logger.info("neo4j.connected", uri=self._uri, database=self._database)
        except Exception as exc:
            raise RuntimeError(f"Neo4j connection failed: {exc}") from exc

    def close(self) -> None:
        if self._driver:
            self._driver.close()

    # ── Save ──────────────────────────────────────────────────

    def save(self, graph: SchemaGraph) -> None:
        """Persist a SchemaGraph. Overwrites any existing graph for the same hash+tenant."""
        if not self._driver:
            raise RuntimeError("Call connect() before save().")
        with self._driver.session(database=self._database) as session:
            session.execute_write(self._do_save, graph, self._tenant_id)
        logger.info("neo4j.saved", hash=graph.connection_pair_hash, nodes=len(graph.nodes))

    @staticmethod
    def _do_save(tx: Any, graph: SchemaGraph, tenant_id: str) -> None:
        pair_hash = graph.connection_pair_hash

        # Delete old snapshot for this tenant+hash
        tx.run(
            """
            MATCH (s:GraphSnapshot {hash: $hash, tenant_id: $tenant_id})
            DETACH DELETE s
            """,
            hash=pair_hash,
            tenant_id=tenant_id,
        )

        # Create snapshot node
        tx.run(
            """
            CREATE (:GraphSnapshot {
                id: $id, hash: $hash, tenant_id: $tenant_id,
                built_at: $built_at, source_connector: $src, dest_connector: $dst
            })
            """,
            id=graph.id,
            hash=pair_hash,
            tenant_id=tenant_id,
            built_at=graph.built_at.isoformat(),
            src=graph.source_connector_id,
            dst=graph.dest_connector_id,
        )

        # Create schema nodes
        for node in graph.nodes.values():
            tx.run(
                """
                MERGE (n:SchemaNode {node_id: $node_id, hash: $hash, tenant_id: $tenant_id})
                SET n.name = $name, n.qualified_name = $qname,
                    n.connector_id = $cid, n.kind = $kind,
                    n.columns_json = $cols, n.row_count = $rc
                WITH n
                MATCH (s:GraphSnapshot {hash: $hash, tenant_id: $tenant_id})
                MERGE (s)-[:CONTAINS]->(n)
                """,
                node_id=node.id,
                hash=pair_hash,
                tenant_id=tenant_id,
                name=node.name,
                qname=node.qualified_name,
                cid=node.connector_id,
                kind=node.kind.value,
                cols=json.dumps([c.model_dump(mode="json") for c in node.columns]),
                rc=node.row_count,
            )

        # Create edges
        for edge in graph.edges.values():
            tx.run(
                """
                MATCH (a:SchemaNode {node_id: $src, hash: $hash, tenant_id: $tid})
                MATCH (b:SchemaNode {node_id: $tgt, hash: $hash, tenant_id: $tid})
                MERGE (a)-[r:CONNECTED_TO {edge_id: $eid, hash: $hash}]->(b)
                SET r.kind = $kind, r.confidence = $conf,
                    r.join_keys = $jk, r.reasoning = $reasoning
                """,
                src=edge.source_node_id,
                tgt=edge.target_node_id,
                hash=pair_hash,
                tid=tenant_id,
                eid=edge.id,
                kind=edge.kind.value,
                conf=edge.confidence,
                jk=json.dumps([k.model_dump(mode="json") for k in edge.join_keys]),
                reasoning=edge.reasoning or "",
            )

    # ── Load ──────────────────────────────────────────────────

    def load(self, connection_pair_hash: str) -> SchemaGraph | None:
        """Load a persisted SchemaGraph by hash+tenant. Returns None if not found."""
        if not self._driver:
            raise RuntimeError("Call connect() before load().")
        with self._driver.session(database=self._database) as session:
            return session.execute_read(self._do_load, connection_pair_hash, self._tenant_id)

    @staticmethod
    def _do_load(tx: Any, pair_hash: str, tenant_id: str) -> SchemaGraph | None:
        snap = tx.run(
            "MATCH (s:GraphSnapshot {hash: $hash, tenant_id: $tid}) RETURN s",
            hash=pair_hash,
            tid=tenant_id,
        ).single()
        if not snap:
            return None
        s = snap["s"]

        # Load nodes
        node_records = tx.run(
            """
            MATCH (snap:GraphSnapshot {hash: $hash, tenant_id: $tid})-[:CONTAINS]->(n:SchemaNode)
            RETURN n
            """,
            hash=pair_hash,
            tid=tenant_id,
        ).data()

        nodes: dict[str, GraphNode] = {}
        for record in node_records:
            n = record["n"]
            raw_cols = json.loads(n["columns_json"])
            cols = [ColumnDef(**{**c, "dtype": ColumnType(c["dtype"])}) for c in raw_cols]
            node = GraphNode(
                id=n["node_id"],
                name=n["name"],
                qualified_name=n["qualified_name"],
                connector_id=n["connector_id"],
                kind=NodeKind(n["kind"]),
                columns=cols,
                row_count=n.get("row_count"),
            )
            nodes[node.id] = node

        # Load edges
        edge_records = tx.run(
            """
            MATCH (a:SchemaNode {hash: $hash, tenant_id: $tid})-[r:CONNECTED_TO]->(b:SchemaNode)
            RETURN a.node_id AS src, b.node_id AS tgt, r
            """,
            hash=pair_hash,
            tid=tenant_id,
        ).data()

        edges: dict[str, GraphEdge] = {}
        for record in edge_records:
            r = record["r"]
            raw_jk = json.loads(r["join_keys"])
            jks = [JoinKey(**k) for k in raw_jk]
            edge = GraphEdge(
                id=r["edge_id"],
                source_node_id=record["src"],
                target_node_id=record["tgt"],
                kind=EdgeKind(r["kind"]),
                join_keys=jks,
                confidence=r["confidence"],
                reasoning=r.get("reasoning"),
            )
            edges[edge.id] = edge

        from datetime import datetime
        return SchemaGraph(
            id=s["id"],
            connection_pair_hash=pair_hash,
            nodes=nodes,
            edges=edges,
            built_at=datetime.fromisoformat(s["built_at"]),
            source_connector_id=s["source_connector"],
            dest_connector_id=s["dest_connector"],
        )

    def exists(self, connection_pair_hash: str) -> bool:
        """Check if a graph exists for this hash+tenant without loading it."""
        if not self._driver:
            return False
        with self._driver.session(database=self._database) as session:
            result = session.run(
                "MATCH (s:GraphSnapshot {hash: $hash, tenant_id: $tid}) RETURN count(s) AS cnt",
                hash=connection_pair_hash,
                tid=self._tenant_id,
            ).single()
            return (result["cnt"] > 0) if result else False

    def delete(self, connection_pair_hash: str) -> None:
        if not self._driver:
            return
        with self._driver.session(database=self._database) as session:
            session.execute_write(
                lambda tx: tx.run(
                    "MATCH (s:GraphSnapshot {hash: $hash, tenant_id: $tid}) DETACH DELETE s",
                    hash=connection_pair_hash,
                    tid=self._tenant_id,
                )
            )
