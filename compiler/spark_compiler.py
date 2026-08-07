"""
compiler/spark_compiler.py
Logical DAG → PySpark/SQL code generation.
"""
from __future__ import annotations

from textwrap import dedent, indent
from typing import Any

import structlog

from dag.nodes import (
    DAG,
    AggregateNode,
    FilterNode,
    JoinNode,
    QualityGateNode,
    ReadNode,
    SelectNode,
    TransformNode,
    UnionNode,
    WriteNode,
)

logger = structlog.get_logger(__name__)

_INDENT = "    "


class SparkCompiler:
    """
    Walks a logical DAG in topological order and emits a self-contained
    PySpark script string.

    Output is a complete .py file that can be submitted directly to
    Databricks Workflows, spark-submit, or run locally.
    """

    def __init__(self, staging_path: str = "/tmp/migrate_io/staging") -> None:
        self._staging_path = staging_path

    def compile(self, dag: DAG, plan_id: str = "plan") -> str:
        """
        Walk the DAG in topological order and emit a PySpark script.
        Returns the script as a string.
        """
        ordered = dag.topological_order()
        lines: list[str] = []

        # ── Header ────────────────────────────────────────────
        lines += self._header(dag, plan_id)

        # ── SparkSession init ─────────────────────────────────
        lines += [
            "# ── SparkSession ──────────────────────────────────",
            "from pyspark.sql import SparkSession",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.window import Window",
            "from delta.tables import DeltaTable",
            "",
            "spark = (",
            '    SparkSession.builder',
            f'    .appName("migrate_io_{plan_id}")',
            '    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")',
            '    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")',
            '    .getOrCreate()',
            ")",
            "",
        ]

        # ── Node code ─────────────────────────────────────────
        lines += ["# ── Pipeline steps ───────────────────────────────────", ""]
        for node in ordered:
            node_lines = self._compile_node(node, dag)
            lines += node_lines
            lines.append("")

        # ── Footer ────────────────────────────────────────────
        lines += [
            'print("Pipeline complete.")',
            'spark.stop()',
        ]

        script = "\n".join(lines)
        logger.info("spark_compiler.compiled", dag_id=dag.id, node_count=len(ordered))
        return script

    def _header(self, dag: DAG, plan_id: str) -> list[str]:
        reads = dag.read_nodes()
        writes = dag.write_nodes()
        return [
            '"""',
            f"Auto-generated PySpark migration script",
            f"DAG ID   : {dag.id}",
            f"Plan ID  : {plan_id}",
            f"Sources  : {', '.join(n.object_id for n in reads)}",
            f"Targets  : {', '.join(n.object_id for n in writes)}",
            f"Nodes    : {len(dag.nodes)}",
            '"""',
            "",
            "import os",
            "import sys",
            "from datetime import datetime",
            "",
        ]

    def _compile_node(self, node: Any, dag: DAG) -> list[str]:
        var = self._var(node.id)
        lines: list[str] = [f"# ── {node.node_type().upper()}: {node.label} ──"]

        if isinstance(node, ReadNode):
            lines += self._compile_read(node, var)
        elif isinstance(node, FilterNode):
            parent_var = self._var(node.parent_id)
            lines += [f'{var} = {parent_var}.filter("""{node.predicate}""")']
        elif isinstance(node, JoinNode):
            lv = self._var(node.left_id)
            rv = self._var(node.right_id)
            if node.left_key and node.right_key:
                lines += [
                    f'{var} = {lv}.join(',
                    f'{_INDENT}{rv},',
                    f'{_INDENT}on={lv}["{node.left_key}"] == {rv}["{node.right_key}"],',
                    f'{_INDENT}how="{node.join_type}",',
                    ')',
                ]
            else:
                lines += [
                    f"# WARNING: Join keys not specified — this join requires user confirmation",
                    f'# {var} = {lv}.join({rv}, on=<join_key>, how="{node.join_type}")',
                    f"{var} = {lv}  # PLACEHOLDER — fix join keys",
                ]
        elif isinstance(node, TransformNode):
            lines += self._compile_transform(node, var)
        elif isinstance(node, SelectNode):
            parent_var = self._var(node.parent_id)
            exprs = ", ".join(
                f'F.expr("{e["expr"]}").alias("{e["alias"]}")'
                for e in node.expressions
            )
            lines += [f"{var} = {parent_var}.select({exprs})"]
        elif isinstance(node, AggregateNode):
            parent_var = self._var(node.parent_id)
            group = ", ".join(f'"{c}"' for c in node.group_by)
            aggs = ", ".join(
                f'F.{a["func"]}("{a["col"]}").alias("{a["alias"]}")'
                for a in node.aggregations
            )
            lines += [f"{var} = {parent_var}.groupBy({group}).agg({aggs})"]
        elif isinstance(node, UnionNode):
            parent_vars = [self._var(p) for p in node.parent_ids]
            union_call = "unionByName" if not node.distinct else "unionByName(...).distinct"
            lines += [
                f"{var} = {parent_vars[0]}",
                *[f"{var} = {var}.unionByName({pv})" for pv in parent_vars[1:]],
            ]
            if node.distinct:
                lines += [f"{var} = {var}.distinct()"]
        elif isinstance(node, QualityGateNode):
            lines += self._compile_quality_gate(node, var)
        elif isinstance(node, WriteNode):
            lines += self._compile_write(node, var)
        else:
            lines += [f"# Unknown node type: {node.node_type()}"]

        return lines

    def _compile_read(self, node: ReadNode, var: str) -> list[str]:
        lines = []
        path = f"# Source: {node.object_id} via connector {node.connector_id}"
        lines.append(path)
        lines.append(f'# TODO: Replace with connector.read(spark, "{node.object_id}")')
        lines.append(f'{var} = spark.read.format("delta").load("{node.object_id}")')
        if node.watermark_column and node.last_watermark is not None:
            lines.append(
                f'{var} = {var}.filter(F.col("{node.watermark_column}") > "{node.last_watermark}")'
            )
        return lines

    def _compile_transform(self, node: TransformNode, var: str) -> list[str]:
        parent_var = self._var(node.parent_id)
        op = node.op
        p = node.params

        if op == "dedupe":
            keys = p.get("keys", [])
            order_col = p.get("order_col", "updated_at")
            key_str = ", ".join(f'"{k}"' for k in keys)
            return [
                f"_w_{var} = Window.partitionBy({key_str}).orderBy(F.col(\"{order_col}\").desc())",
                f"{var} = {parent_var}.withColumn('_rn', F.row_number().over(_w_{var})).filter('_rn = 1').drop('_rn')",
            ]
        elif op == "mask":
            columns = p.get("columns", [])
            strategy = p.get("strategy", "hash")
            lines = [f"{var} = {parent_var}"]
            for col in columns:
                if strategy == "hash":
                    lines.append(f'{var} = {var}.withColumn("{col}", F.sha2(F.col("{col}").cast("string"), 256))')
                elif strategy == "nullify":
                    lines.append(f'{var} = {var}.withColumn("{col}", F.lit(None))')
                elif strategy == "tokenize":
                    lines.append(f'{var} = {var}.withColumn("{col}", F.concat(F.lit("TOKEN_"), F.md5(F.col("{col}").cast("string"))))')
            return lines
        elif op == "type_cast":
            casts = p.get("casts", {})
            lines = [f"{var} = {parent_var}"]
            for col, dtype in casts.items():
                lines.append(f'{var} = {var}.withColumn("{col}", F.col("{col}").cast("{dtype}"))')
            return lines
        elif op == "scd_merge":
            return [
                f"# SCD merge for {var}",
                f"# TODO: Implement SCD Type {p.get('type', 1)} merge using Delta MERGE INTO",
                f"{var} = {parent_var}",
            ]
        elif op == "pivot":
            pivot_col = p.get("pivot_col", "")
            values = p.get("values", [])
            agg_col = p.get("agg_col", "value")
            agg_fn = p.get("agg_fn", "sum")
            val_str = str(values) if values else ""
            return [
                f'{var} = {parent_var}.groupBy(*[c for c in {parent_var}.columns if c != "{pivot_col}"]).pivot("{pivot_col}", {val_str}).{agg_fn}("{agg_col}")',
            ]
        elif op == "custom_expr":
            expr_str = p.get("expression", "")
            alias = p.get("alias", "custom_col")
            return [
                f'{var} = {parent_var}.withColumn("{alias}", F.expr("{expr_str}"))',
            ]
        else:
            return [f"# Unknown transform op: {op}", f"{var} = {parent_var}"]

    def _compile_quality_gate(self, node: QualityGateNode, var: str) -> list[str]:
        parent_var = self._var(node.parent_id)
        lines = [
            f"{var} = {parent_var}",
            f"_qg_{var}_count = {var}.count()",
            f'print(f"Quality gate: {{_qg_{var}_count}} rows")',
            f"if _qg_{var}_count == 0:",
            f'    print("WARNING: Empty DataFrame after pipeline — no rows to write.")',
        ]
        return lines

    def _compile_write(self, node: WriteNode, var: str) -> list[str]:
        parent_var = self._var(node.parent_id)
        staging_path = f"{self._staging_path}/{node.id}"
        lines = [
            f"# Stage to Delta before committing to production",
            f'{parent_var}.write.format("delta").mode("overwrite").save("{staging_path}")',
            f'print("Staged to: {staging_path}")',
            f"# Preview (remove for production)",
            f'{parent_var}.limit(20).show()',
            f"# After user approval, run commit.py to merge staging → production",
        ]
        return lines

    @staticmethod
    def _var(node_id: str) -> str:
        """Convert a node ID to a valid Python variable name."""
        return f"df_{node_id.replace('-', '_').replace('.', '_')}"
