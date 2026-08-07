"""
intent/grounding.py
Resolves IntentJSON entity names to actual GraphNode IDs.
"""
from __future__ import annotations

import structlog
from Levenshtein import ratio as levenshtein_ratio

from graph.models import SchemaGraph
from intent.schema import (
    FilterClause,
    GroundedIntent,
    IntentJSON,
    JoinSpec,
    ResolvedJoin,
    ResolvedTable,
)

logger = structlog.get_logger(__name__)

RESOLUTION_THRESHOLD = 0.60    # minimum name-match score to auto-resolve
CONFIRMATION_THRESHOLD = 0.85  # below this → flag for user confirmation


class IntentGrounder:
    """
    Resolves entity names in an IntentJSON against the SchemaGraph.

    Resolution order for each table name:
    1. Exact match on node.name or node.qualified_name
    2. Case-insensitive exact match
    3. Best Levenshtein match above threshold
    4. Unresolved → added to unresolved_entities list
    """

    def __init__(self, schema_graph: SchemaGraph) -> None:
        self._graph = schema_graph
        self._nodes = list(schema_graph.nodes.values())

    def ground(self, intent: IntentJSON) -> GroundedIntent:
        """Ground all entity names in the intent and return a GroundedIntent."""
        resolved_sources = [self._resolve_table(name) for name in intent.source_tables]
        resolved_target = self._resolve_table(intent.target_table)

        resolved_joins = [
            self._resolve_join(js, resolved_sources)
            for js in intent.joins
        ]

        unresolved = []
        for rt in resolved_sources:
            if not rt.node_id:
                unresolved.append(rt.entity_name)
        if not resolved_target.node_id:
            unresolved.append(resolved_target.entity_name)

        logger.info(
            "grounder.done",
            sources=[r.entity_name for r in resolved_sources],
            target=resolved_target.entity_name,
            unresolved=unresolved,
            needs_confirmation=any(r.needs_user_confirmation for r in resolved_sources)
                or resolved_target.needs_user_confirmation,
        )

        return GroundedIntent(
            original_intent=intent,
            operation=intent.operation,
            source_tables=resolved_sources,
            target_table=resolved_target,
            filters=intent.filters,
            resolved_joins=resolved_joins,
            transforms=intent.transforms,
            column_mappings=intent.column_mappings,
            output_columns=intent.output_columns,
            incremental=intent.incremental,
            dry_run=intent.dry_run,
            unresolved_entities=unresolved,
        )

    def _resolve_table(self, entity_name: str) -> ResolvedTable:
        """Find the best-matching GraphNode for a table name."""
        # 1. Exact name match
        for node in self._nodes:
            if node.name == entity_name or node.qualified_name == entity_name:
                return ResolvedTable(
                    entity_name=entity_name,
                    node_id=node.id,
                    node_qualified_name=node.qualified_name,
                    confidence=1.0,
                )

        # 2. Case-insensitive exact
        name_lower = entity_name.lower()
        for node in self._nodes:
            if node.name.lower() == name_lower or node.qualified_name.lower() == name_lower:
                return ResolvedTable(
                    entity_name=entity_name,
                    node_id=node.id,
                    node_qualified_name=node.qualified_name,
                    confidence=0.95,
                )

        # 3. Best Levenshtein
        best_node = None
        best_score = 0.0
        for node in self._nodes:
            score = max(
                levenshtein_ratio(name_lower, node.name.lower()),
                levenshtein_ratio(name_lower, node.qualified_name.lower().split(".")[-1]),
            )
            if score > best_score:
                best_score = score
                best_node = node

        if best_node and best_score >= RESOLUTION_THRESHOLD:
            return ResolvedTable(
                entity_name=entity_name,
                node_id=best_node.id,
                node_qualified_name=best_node.qualified_name,
                confidence=round(best_score, 3),
                needs_user_confirmation=best_score < CONFIRMATION_THRESHOLD,
            )

        # 4. Unresolved
        logger.warning("grounder.unresolved", entity=entity_name, best_score=best_score)
        return ResolvedTable(
            entity_name=entity_name,
            node_id="",
            node_qualified_name="",
            confidence=0.0,
            needs_user_confirmation=True,
        )

    def _resolve_join(
        self,
        join_spec: JoinSpec,
        resolved_sources: list[ResolvedTable],
    ) -> ResolvedJoin:
        """Resolve a JoinSpec to actual node IDs and validate join keys."""
        left_rt = next(
            (r for r in resolved_sources if r.entity_name == join_spec.left_table), None
        ) or self._resolve_table(join_spec.left_table)

        right_rt = next(
            (r for r in resolved_sources if r.entity_name == join_spec.right_table), None
        ) or self._resolve_table(join_spec.right_table)

        # Try to find the join key in the graph edges
        confidence = 1.0
        reasoning = "User-specified join key"
        needs_confirmation = False

        if left_rt.node_id and right_rt.node_id:
            # Check if there's a graph edge supporting this join
            left_node = self._graph.get_node(left_rt.node_id)
            right_node = self._graph.get_node(right_rt.node_id)

            has_matching_edge = False
            for edge in self._graph.edges.values():
                if (edge.source_node_id == left_rt.node_id and edge.target_node_id == right_rt.node_id) or \
                   (edge.source_node_id == right_rt.node_id and edge.target_node_id == left_rt.node_id):
                    for jk in edge.join_keys:
                        if (jk.source_column == join_spec.left_key and jk.target_column == join_spec.right_key) or \
                           (jk.source_column == join_spec.right_key and jk.target_column == join_spec.left_key):
                            has_matching_edge = True
                            confidence = edge.confidence
                            reasoning = edge.reasoning or "Graph edge confirmed join"
                            needs_confirmation = edge.needs_user_confirmation
                            break

            if not has_matching_edge and left_node and right_node:
                confidence = 0.70
                reasoning = f"Join key specified by user; not found in graph edges. Verify {join_spec.left_key} ↔ {join_spec.right_key}."
                needs_confirmation = True

        return ResolvedJoin(
            join_spec=join_spec,
            left_node_id=left_rt.node_id,
            right_node_id=right_rt.node_id,
            resolved_left_key=join_spec.left_key,
            resolved_right_key=join_spec.right_key,
            confidence=confidence,
            reasoning=reasoning,
            needs_user_confirmation=needs_confirmation or left_rt.needs_user_confirmation or right_rt.needs_user_confirmation,
        )
