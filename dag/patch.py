"""
dag/patch.py
Applies incremental user edits to an existing DAG without full re-parse.
"""
from __future__ import annotations

import uuid
from typing import Any

import structlog

from dag.nodes import DAG, FilterNode, JoinNode, TransformNode

logger = structlog.get_logger(__name__)


class DAGPatcher:
    """
    Applies a single named patch operation to an existing DAG.
    Used for iterative refinement ("exclude cancelled orders",
    "change join to left join", "add masking for email").
    """

    SUPPORTED_OPS = [
        "add_filter",
        "remove_node",
        "edit_join",
        "edit_filter",
        "add_transform",
        "remove_transform",
    ]

    def apply(
        self,
        dag: DAG,
        op: str,
        node_id: str | None,
        params: dict[str, Any],
    ) -> DAG:
        if op not in self.SUPPORTED_OPS:
            raise ValueError(f"Unknown patch op: {op!r}. Supported: {self.SUPPORTED_OPS}")

        logger.info("dag_patcher.apply", op=op, node_id=node_id)

        if op == "add_filter":
            return self._add_filter(dag, params)
        elif op == "remove_node":
            return self._remove_node(dag, node_id)
        elif op == "edit_join":
            return self._edit_join(dag, node_id, params)
        elif op == "edit_filter":
            return self._edit_filter(dag, node_id, params)
        elif op == "add_transform":
            return self._add_transform(dag, params)
        elif op == "remove_transform":
            return self._remove_node(dag, node_id)
        return dag

    def _add_filter(self, dag: DAG, params: dict[str, Any]) -> DAG:
        after_node_id = params.get("after_node_id", "")
        predicate = params.get("predicate", "")
        if not predicate:
            raise ValueError("add_filter requires 'predicate'")

        # Find the write node and insert filter before it
        fn = FilterNode(
            id=f"filter_{uuid.uuid4().hex[:6]}",
            label=f"Filter: {predicate[:40]}",
            predicate=predicate,
            parent_id=after_node_id,
        )
        dag.add_node(fn)

        # Rewire: find any node that had after_node_id as parent and re-point to fn
        for node in dag.nodes.values():
            if hasattr(node, "parent_id") and node.parent_id == after_node_id and node.id != fn.id:
                node.parent_id = fn.id  # type: ignore[attr-defined]
                # Update edges
                dag.edges = [(p, c) for p, c in dag.edges if not (p == after_node_id and c == node.id)]
                dag.add_edge(fn.id, node.id)
                break

        dag.add_edge(after_node_id, fn.id)
        return dag

    def _remove_node(self, dag: DAG, node_id: str | None) -> DAG:
        if not node_id or node_id not in dag.nodes:
            raise ValueError(f"Node {node_id!r} not found")
        # Find parent and child
        parents = [p for p, c in dag.edges if c == node_id]
        children = [c for p, c in dag.edges if p == node_id]
        # Re-wire: connect parent directly to child
        dag.edges = [(p, c) for p, c in dag.edges if p != node_id and c != node_id]
        for parent in parents:
            for child in children:
                dag.add_edge(parent, child)
        del dag.nodes[node_id]
        return dag

    def _edit_join(self, dag: DAG, node_id: str | None, params: dict[str, Any]) -> DAG:
        if not node_id or node_id not in dag.nodes:
            raise ValueError(f"JoinNode {node_id!r} not found")
        node = dag.nodes[node_id]
        if not isinstance(node, JoinNode):
            raise ValueError(f"Node {node_id!r} is not a JoinNode")
        if "left_key" in params:
            node.left_key = params["left_key"]
        if "right_key" in params:
            node.right_key = params["right_key"]
        if "join_type" in params:
            node.join_type = params["join_type"]
        return dag

    def _edit_filter(self, dag: DAG, node_id: str | None, params: dict[str, Any]) -> DAG:
        if not node_id or node_id not in dag.nodes:
            raise ValueError(f"FilterNode {node_id!r} not found")
        node = dag.nodes[node_id]
        if not isinstance(node, FilterNode):
            raise ValueError(f"Node {node_id!r} is not a FilterNode")
        if "predicate" in params:
            node.predicate = params["predicate"]
            node.label = f"Filter: {params['predicate'][:40]}"
        return dag

    def _add_transform(self, dag: DAG, params: dict[str, Any]) -> DAG:
        after_node_id = params.get("after_node_id", "")
        op = params.get("op", "")
        if not op:
            raise ValueError("add_transform requires 'op'")
        tn = TransformNode(
            id=f"transform_{uuid.uuid4().hex[:6]}",
            label=f"Transform: {op}",
            op=op,
            params=params.get("params", {}),
            parent_id=after_node_id,
        )
        dag.add_node(tn)
        dag.add_edge(after_node_id, tn.id)
        return dag
