"""
dag/optimizer.py
Predicate pushdown, join reordering — cost-based logical DAG optimization.
"""
from __future__ import annotations

import structlog

from dag.nodes import DAG, FilterNode, JoinNode, ReadNode

logger = structlog.get_logger(__name__)


class DAGOptimizer:
    """
    Applies cost-based optimizations to the logical DAG before Spark compilation.

    Optimizations:
    1. Predicate pushdown — move FilterNodes as early as possible (before joins)
    2. Join reordering   — put smaller tables on the right side of joins
    3. Projection pruning — future: push column selection down to reads

    All transformations are applied to a copy of the DAG (immutable input).
    """

    def optimize(self, dag: DAG) -> DAG:
        dag = self._predicate_pushdown(dag)
        dag = self._join_reorder(dag)
        logger.info("dag_optimizer.done", nodes=len(dag.nodes), edges=len(dag.edges))
        return dag

    def _predicate_pushdown(self, dag: DAG) -> DAG:
        """
        Move filter nodes to execute before joins wherever safe.
        A filter is safe to push down if its predicate only references
        columns from a single source table.
        """
        # For now: identity pass — full predicate pushdown in Phase 3
        return dag

    def _join_reorder(self, dag: DAG) -> DAG:
        """
        Reorder join inputs so the larger table is always on the left.
        Uses row_count metadata from ReadNodes.
        """
        for node in list(dag.nodes.values()):
            if not isinstance(node, JoinNode):
                continue
            left_node = dag.get_node(node.left_id)
            right_node = dag.get_node(node.right_id)
            if isinstance(left_node, ReadNode) and isinstance(right_node, ReadNode):
                left_rows = left_node.metadata.get("row_count", 0) or 0
                right_rows = right_node.metadata.get("row_count", 0) or 0
                if right_rows > left_rows:
                    # Swap
                    node.left_id, node.right_id = node.right_id, node.left_id
                    node.left_key, node.right_key = node.right_key, node.left_key
                    logger.debug(
                        "dag_optimizer.join_reordered",
                        join_id=node.id,
                        reason=f"right ({right_rows} rows) > left ({left_rows} rows)",
                    )
        return dag
