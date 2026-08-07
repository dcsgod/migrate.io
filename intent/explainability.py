"""
intent/explainability.py
XAI — generates human-readable reasoning for every mapping decision.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import structlog

from graph.models import GraphNode
from intent.schema import ColumnMapping, GroundedIntent, ResolvedJoin, ResolvedTable

logger = structlog.get_logger(__name__)

_PROMPT_DIR = Path(__file__).parent / "prompts"


class MappingExplanation(dict):
    """A single mapping decision with its XAI reasoning."""
    source: str
    target: str
    confidence: float
    strategy: str
    reasoning: str
    sample_values: list[Any]
    warnings: list[str]


class ExplainabilityTracer:
    """
    Attaches human-readable reasoning to every mapping decision in a
    GroundedIntent. Produces an ExplainabilityReport visible in the
    IntentViewer UI panel.
    """

    def __init__(self, llm_client: Any = None) -> None:
        self._llm = llm_client
        self._prompt_template = (_PROMPT_DIR / "explain_mapping.txt").read_text(encoding="utf-8")

    def explain(
        self,
        grounded_intent: GroundedIntent,
        source_nodes: dict[str, GraphNode],
        target_node: GraphNode,
    ) -> dict[str, Any]:
        """
        Generate the full explainability report for a GroundedIntent.

        Returns:
            {
                "table_resolutions": [...],
                "join_explanations": [...],
                "column_explanations": [...],
                "warnings": [...],
                "overall_confidence": float,
            }
        """
        report: dict[str, Any] = {
            "table_resolutions": [],
            "join_explanations": [],
            "column_explanations": [],
            "warnings": [],
            "overall_confidence": 1.0,
        }

        # ── Table resolutions ─────────────────────────────────
        for rt in grounded_intent.source_tables:
            entry = self._explain_table_resolution(rt, source_nodes)
            report["table_resolutions"].append(entry)
            if rt.needs_user_confirmation:
                report["warnings"].append(
                    f"Low-confidence table resolution: {rt.entity_name!r} → "
                    f"{rt.node_qualified_name!r} ({rt.confidence:.0%})"
                )

        # Target
        tgt_entry = self._explain_table_resolution(grounded_intent.target_table, {})
        report["table_resolutions"].append(tgt_entry)

        # ── Join explanations ─────────────────────────────────
        for rj in grounded_intent.resolved_joins:
            entry = self._explain_join(rj)
            report["join_explanations"].append(entry)
            if rj.needs_user_confirmation:
                report["warnings"].append(
                    f"Low-confidence join: {rj.join_spec.left_table}.{rj.resolved_left_key} ↔ "
                    f"{rj.join_spec.right_table}.{rj.resolved_right_key} ({rj.confidence:.0%})"
                )

        # ── Column mapping explanations ───────────────────────
        for cm in grounded_intent.column_mappings:
            entry = self._explain_column_mapping(cm, source_nodes, target_node)
            report["column_explanations"].append(entry)

        # ── Unresolved entities ───────────────────────────────
        for name in grounded_intent.unresolved_entities:
            report["warnings"].append(f"Could not resolve entity: {name!r}")

        # ── Overall confidence ────────────────────────────────
        all_scores = (
            [rt.confidence for rt in grounded_intent.source_tables]
            + [grounded_intent.target_table.confidence]
            + [rj.confidence for rj in grounded_intent.resolved_joins]
        )
        if all_scores:
            report["overall_confidence"] = round(sum(all_scores) / len(all_scores), 3)

        return report

    def _explain_table_resolution(
        self, rt: ResolvedTable, nodes: dict[str, GraphNode]
    ) -> dict[str, Any]:
        node = nodes.get(rt.node_id) if rt.node_id else None
        return {
            "entity_name": rt.entity_name,
            "resolved_to": rt.node_qualified_name or "UNRESOLVED",
            "confidence": rt.confidence,
            "needs_confirmation": rt.needs_user_confirmation,
            "columns": node.column_names() if node else [],
            "reasoning": (
                f"Exact match" if rt.confidence == 1.0
                else f"Fuzzy match ({rt.confidence:.0%} name similarity)"
                if rt.confidence > 0
                else "Could not resolve — no similar table found in the graph"
            ),
        }

    def _explain_join(self, rj: ResolvedJoin) -> dict[str, Any]:
        return {
            "left_table": rj.join_spec.left_table,
            "right_table": rj.join_spec.right_table,
            "left_key": rj.resolved_left_key,
            "right_key": rj.resolved_right_key,
            "join_type": rj.join_spec.join_type,
            "confidence": rj.confidence,
            "needs_confirmation": rj.needs_user_confirmation,
            "reasoning": rj.reasoning,
        }

    def _explain_column_mapping(
        self,
        cm: ColumnMapping,
        source_nodes: dict[str, GraphNode],
        target_node: GraphNode,
    ) -> dict[str, Any]:
        # Find the source node that has this column
        src_node: GraphNode | None = None
        for node in source_nodes.values():
            if node.get_column(cm.source_column):
                src_node = node
                break

        src_col = src_node.get_column(cm.source_column) if src_node else None
        tgt_col = target_node.get_column(cm.target_column)

        warnings = []
        if src_col and tgt_col and src_col.dtype != tgt_col.dtype:
            warnings.append(
                f"Type mismatch: {cm.source_column} is {src_col.dtype.value} "
                f"but {cm.target_column} expects {tgt_col.dtype.value}"
            )

        return {
            "source_column": cm.source_column,
            "target_column": cm.target_column,
            "transform": cm.transform,
            "source_type": src_col.dtype.value if src_col else "unknown",
            "target_type": tgt_col.dtype.value if tgt_col else "unknown",
            "warnings": warnings,
        }
