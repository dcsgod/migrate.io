"""
graph/inference.py
Edge inferrer — name similarity, value overlap, and LLM-assisted inference.
Produces confidence-scored GraphEdges for nodes with no explicit FK.
"""
from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Any

import structlog
from Levenshtein import ratio as levenshtein_ratio

from graph.models import EdgeKind, GraphEdge, GraphNode, JoinKey

if TYPE_CHECKING:
    from graph.store import GraphStore

logger = structlog.get_logger(__name__)

# Threshold below which we don't emit an inferred edge
NAME_SIMILARITY_THRESHOLD = 0.70
VALUE_OVERLAP_THRESHOLD = 0.50
LLM_CONFIDENCE_DEFAULT = 0.75


class EdgeInferrer:
    """
    Infers join relationships between nodes based on:
      1. Name similarity (Levenshtein) — fast, no Spark needed
      2. Value overlap  — samples columns and computes Jaccard similarity
      3. LLM fallback   — asks the LLM to suggest join keys for ambiguous pairs

    Only node pairs that don't already have an explicit edge are considered.
    Low-confidence edges are flagged: needs_user_confirmation == True.
    """

    def __init__(
        self,
        store: "GraphStore",
        spark: Any = None,
        llm_client: Any = None,
        value_sample_size: int = 200,
        max_pairs: int = 100,
    ) -> None:
        self._store = store
        self._spark = spark
        self._llm = llm_client
        self._value_sample_size = value_sample_size
        self._max_pairs = max_pairs

    def infer_all(self) -> list[GraphEdge]:
        """
        Run all inference strategies and return a deduplicated list of edges.
        Does not add edges to the store — caller decides.
        """
        nodes = self._store.all_nodes()
        existing_pairs = {
            (e.source_node_id, e.target_node_id)
            for e in self._store.all_edges()
        }

        candidate_pairs = [
            (a, b)
            for a, b in itertools.combinations(nodes, 2)
            if (a.id, b.id) not in existing_pairs
            and a.connector_id != b.connector_id  # cross-connector only
        ]
        candidate_pairs = candidate_pairs[: self._max_pairs]

        edges: dict[str, GraphEdge] = {}
        for source_node, target_node in candidate_pairs:
            edge = self._infer_pair(source_node, target_node)
            if edge and edge.id not in edges:
                edges[edge.id] = edge

        logger.info("edge_inferrer.done", inferred=len(edges))
        return list(edges.values())

    def _infer_pair(self, source: GraphNode, target: GraphNode) -> GraphEdge | None:
        """Try to infer a join between two nodes. Returns None if no match found."""
        best_join_keys: list[JoinKey] = []
        best_confidence = 0.0
        best_kind = EdgeKind.INFERRED_NAME
        best_reasoning = ""

        # ── Strategy 1: Column name similarity ────────────────
        for src_col in source.columns:
            for tgt_col in target.columns:
                sim = levenshtein_ratio(
                    src_col.name.lower().replace("_", ""),
                    tgt_col.name.lower().replace("_", ""),
                )
                if sim >= NAME_SIMILARITY_THRESHOLD:
                    confidence = round(sim * 0.9, 3)  # cap at 90% — name alone isn't proof
                    if confidence > best_confidence:
                        best_confidence = confidence
                        best_join_keys = [JoinKey(
                            source_column=src_col.name,
                            target_column=tgt_col.name,
                            confidence=confidence,
                        )]
                        best_kind = EdgeKind.INFERRED_NAME
                        best_reasoning = (
                            f"Column name similarity {sim:.0%}: "
                            f"{source.name}.{src_col.name} ↔ {target.name}.{tgt_col.name}"
                        )

        # ── Strategy 2: Value overlap (Jaccard) ───────────────
        if self._spark and best_join_keys:
            for jk in best_join_keys:
                try:
                    src_vals = set(self._sample_values(source, jk.source_column))
                    tgt_vals = set(self._sample_values(target, jk.target_column))
                    if src_vals and tgt_vals:
                        jaccard = len(src_vals & tgt_vals) / len(src_vals | tgt_vals)
                        if jaccard >= VALUE_OVERLAP_THRESHOLD:
                            combined_confidence = round(
                                0.4 * jk.confidence + 0.6 * jaccard, 3
                            )
                            if combined_confidence > best_confidence:
                                best_confidence = combined_confidence
                                best_kind = EdgeKind.INFERRED_VALUE
                                best_reasoning = (
                                    f"Value overlap (Jaccard={jaccard:.0%}): "
                                    f"{source.name}.{jk.source_column} ↔ "
                                    f"{target.name}.{jk.target_column}. "
                                    f"Name similarity was {jk.confidence:.0%}."
                                )
                except Exception as exc:
                    logger.warning("edge_inferrer.value_overlap_failed", reason=str(exc))

        # ── Strategy 3: LLM fallback ──────────────────────────
        if not best_join_keys and self._llm:
            try:
                llm_result = self._llm_suggest(source, target)
                if llm_result:
                    best_join_keys = llm_result["join_keys"]
                    best_confidence = LLM_CONFIDENCE_DEFAULT
                    best_kind = EdgeKind.LLM_SUGGESTED
                    best_reasoning = llm_result["reasoning"]
            except Exception as exc:
                logger.warning("edge_inferrer.llm_failed", reason=str(exc))

        if not best_join_keys:
            return None

        edge_id = GraphEdge.make_id(source.id, target.id)
        return GraphEdge(
            id=edge_id,
            source_node_id=source.id,
            target_node_id=target.id,
            kind=best_kind,
            join_keys=best_join_keys,
            confidence=best_confidence,
            reasoning=best_reasoning,
        )

    def _sample_values(self, node: GraphNode, column_name: str) -> list[Any]:
        if not self._spark:
            return []
        from connectors import build_connector
        connector = self._store.get_node(node.id)  # get connector via node
        # In practice this would call connector.read(spark, object_id).select(column_name).sample...
        # Simplified: return empty list here; real impl wires through spark session
        return []

    def _llm_suggest(self, source: GraphNode, target: GraphNode) -> dict | None:
        """Ask the LLM to suggest join keys between two nodes."""
        prompt = (
            f"You are an expert data engineer. Suggest the most likely join key(s) "
            f"between these two tables.\n\n"
            f"Table A: {source.name}\nColumns: {source.column_names()}\n\n"
            f"Table B: {target.name}\nColumns: {target.column_names()}\n\n"
            f"Respond ONLY with a JSON object like:\n"
            f'{{"join_keys": [{{"source_column": "...", "target_column": "..."}}], '
            f'"reasoning": "..."}}\n'
            f'If no join is likely, respond with {{"join_keys": [], "reasoning": "no match"}}.'
        )
        import json
        response = self._llm.chat(prompt)
        try:
            data = json.loads(response)
            if not data.get("join_keys"):
                return None
            return {
                "join_keys": [
                    JoinKey(source_column=k["source_column"], target_column=k["target_column"])
                    for k in data["join_keys"]
                ],
                "reasoning": data.get("reasoning", "LLM suggested"),
            }
        except Exception:
            return None
