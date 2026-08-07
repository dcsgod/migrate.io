"""
dag/builder.py
Converts a GroundedIntent into a logical DAG of DAGNodes.
"""
from __future__ import annotations

import uuid
from typing import Any

import structlog

from dag.nodes import (
    DAG,
    DAGNode,
    FilterNode,
    JoinNode,
    QualityGateNode,
    ReadNode,
    SelectNode,
    TransformNode,
    WriteNode,
)
from graph.models import SchemaGraph
from intent.schema import FilterClause, GroundedIntent, OperationType

logger = structlog.get_logger(__name__)


def _nid(label: str) -> str:
    return f"{label}_{uuid.uuid4().hex[:6]}"


class DAGBuilder:
    """
    Converts a GroundedIntent into a logical DAG.

    The DAG is engine-agnostic — Spark specifics live only in compiler/.
    """

    def __init__(self, schema_graph: SchemaGraph) -> None:
        self._graph = schema_graph

    def build(self, intent: GroundedIntent) -> DAG:
        dag = DAG(id=str(uuid.uuid4()))
        logger.info("dag_builder.building", operation=intent.operation.value)

        # ── 1. Read nodes for each source table ───────────────
        read_node_ids: list[str] = []
        for rt in intent.source_tables:
            if not rt.node_id:
                logger.warning("dag_builder.skip_unresolved", entity=rt.entity_name)
                continue
            graph_node = self._graph.get_node(rt.node_id)
            rn = ReadNode(
                id=_nid("read"),
                label=f"Read {rt.entity_name}",
                connector_id=graph_node.connector_id if graph_node else rt.node_id,
                object_id=rt.node_id,
                watermark_column=intent.incremental.watermark_column if intent.incremental else None,
                last_watermark=intent.incremental.last_watermark if intent.incremental else None,
            )
            dag.add_node(rn)
            read_node_ids.append(rn.id)

        current_node_id = read_node_ids[0] if read_node_ids else None

        # ── 2. Join nodes (multi-table) ───────────────────────
        for rj in intent.resolved_joins:
            left_read_id = self._find_read_for_node(dag, rj.left_node_id) or current_node_id
            right_read_id = self._find_read_for_node(dag, rj.right_node_id)

            if not right_read_id:
                # Add a read node for the right side if missing
                rn = ReadNode(
                    id=_nid("read"),
                    label=f"Read {rj.join_spec.right_table}",
                    connector_id=rj.right_node_id,
                    object_id=rj.right_node_id,
                )
                dag.add_node(rn)
                right_read_id = rn.id

            jn = JoinNode(
                id=_nid("join"),
                label=f"Join {rj.join_spec.left_table} ↔ {rj.join_spec.right_table}",
                left_id=left_read_id,
                right_id=right_read_id,
                left_key=rj.resolved_left_key,
                right_key=rj.resolved_right_key,
                join_type=rj.join_spec.join_type,
            )
            dag.add_node(jn)
            dag.add_edge(left_read_id, jn.id)
            dag.add_edge(right_read_id, jn.id)
            current_node_id = jn.id

        # If no joins and multiple reads, add implicit join
        if len(read_node_ids) > 1 and not intent.resolved_joins:
            left_id = read_node_ids[0]
            for right_id in read_node_ids[1:]:
                jn = JoinNode(
                    id=_nid("join"),
                    label="Auto-join (verify keys)",
                    left_id=left_id,
                    right_id=right_id,
                    left_key="",   # requires user confirmation
                    right_key="",
                    join_type="inner",
                )
                dag.add_node(jn)
                dag.add_edge(left_id, jn.id)
                dag.add_edge(right_id, jn.id)
                left_id = jn.id
            current_node_id = left_id

        # ── 3. Filter nodes ───────────────────────────────────
        for fc in intent.filters:
            pred = self._filter_to_sql(fc)
            fn = FilterNode(
                id=_nid("filter"),
                label=f"Filter: {pred}",
                predicate=pred,
                parent_id=current_node_id or "",
            )
            dag.add_node(fn)
            if current_node_id:
                dag.add_edge(current_node_id, fn.id)
            current_node_id = fn.id

        # ── 4. Transform nodes ────────────────────────────────
        for ts in intent.transforms:
            tn = TransformNode(
                id=_nid("transform"),
                label=f"Transform: {ts.op}",
                op=ts.op,
                params=ts.params,
                parent_id=current_node_id or "",
            )
            dag.add_node(tn)
            if current_node_id:
                dag.add_edge(current_node_id, tn.id)
            current_node_id = tn.id

        # ── 5. Column projection / rename ─────────────────────
        if intent.column_mappings or intent.output_columns:
            exprs = []
            if intent.column_mappings:
                for cm in intent.column_mappings:
                    src = cm.transform.replace("{{source}}", cm.source_column) if cm.transform else cm.source_column
                    exprs.append({"expr": src, "alias": cm.target_column})
            elif intent.output_columns:
                exprs = [{"expr": col, "alias": col} for col in intent.output_columns]

            sn = SelectNode(
                id=_nid("select"),
                label="Project columns",
                parent_id=current_node_id or "",
                expressions=exprs,
            )
            dag.add_node(sn)
            if current_node_id:
                dag.add_edge(current_node_id, sn.id)
            current_node_id = sn.id

        # ── 6. Quality gate (always added as a check node) ────
        qg = QualityGateNode(
            id=_nid("quality_gate"),
            label="Data quality checks",
            parent_id=current_node_id or "",
            checks=[
                {"type": "null_pct", "column": "*", "threshold": 0.95},
                {"type": "row_count", "min": 1},
            ],
            fail_on_error=False,   # warn, don't block by default
        )
        dag.add_node(qg)
        if current_node_id:
            dag.add_edge(current_node_id, qg.id)
        current_node_id = qg.id

        # ── 7. Write node ─────────────────────────────────────
        if intent.target_table.node_id:
            tgt_graph_node = self._graph.get_node(intent.target_table.node_id)
            wn = WriteNode(
                id=_nid("write"),
                label=f"Write → {intent.target_table.entity_name}",
                connector_id=tgt_graph_node.connector_id if tgt_graph_node else intent.target_table.node_id,
                object_id=intent.target_table.node_id,
                mode=self._write_mode(intent.operation),
                parent_id=current_node_id or "",
                staging=True,
            )
            dag.add_node(wn)
            if current_node_id:
                dag.add_edge(current_node_id, wn.id)

        logger.info("dag_builder.done", nodes=len(dag.nodes), edges=len(dag.edges))
        return dag

    def _find_read_for_node(self, dag: DAG, node_id: str) -> str | None:
        for n in dag.nodes.values():
            if isinstance(n, ReadNode) and n.object_id == node_id:
                return n.id
        return None

    @staticmethod
    def _filter_to_sql(fc: FilterClause) -> str:
        op_map = {
            "eq": "=", "neq": "!=", "gt": ">", "gte": ">=",
            "lt": "<", "lte": "<=", "like": "LIKE",
        }
        if fc.op.value in ("is_null", "is_not_null"):
            return f"`{fc.column}` {fc.op.value.replace('_', ' ').upper()}"
        if fc.op.value == "in":
            vals = ", ".join(f"'{v}'" for v in fc.value) if isinstance(fc.value, list) else f"'{fc.value}'"
            return f"`{fc.column}` IN ({vals})"
        if fc.op.value == "not_in":
            vals = ", ".join(f"'{v}'" for v in fc.value) if isinstance(fc.value, list) else f"'{fc.value}'"
            return f"`{fc.column}` NOT IN ({vals})"
        sql_op = op_map.get(fc.op.value, "=")
        val = f"'{fc.value}'" if isinstance(fc.value, str) else fc.value
        return f"`{fc.column}` {sql_op} {val}"

    @staticmethod
    def _write_mode(op: OperationType) -> str:
        if op == OperationType.MERGE:
            return "merge"
        if op == OperationType.INCREMENTAL:
            return "append"
        return "overwrite"
