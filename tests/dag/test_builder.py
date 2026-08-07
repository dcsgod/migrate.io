"""
tests/dag/test_builder.py
Tests DAG building from grounded intents.
"""
import pytest


def test_dag_builds_from_grounded_intent(sample_dag):
    assert len(sample_dag.nodes) > 0
    assert len(sample_dag.edges) > 0


def test_dag_has_read_node(sample_dag):
    from dag.nodes import ReadNode
    assert any(isinstance(n, ReadNode) for n in sample_dag.nodes.values())


def test_dag_has_write_node(sample_dag):
    from dag.nodes import WriteNode
    assert any(isinstance(n, WriteNode) for n in sample_dag.nodes.values())


def test_dag_topological_order_valid(sample_dag):
    ordered = sample_dag.topological_order()
    assert len(ordered) == len(sample_dag.nodes)


def test_dag_validator_no_errors_on_valid_dag(sample_dag):
    from dag.validator import DAGValidator
    result = DAGValidator().validate(sample_dag)
    assert not result.has_errors


def test_dag_compiled_not_empty(sample_dag):
    from compiler.spark_compiler import SparkCompiler
    code = SparkCompiler().compile(sample_dag, plan_id="test")
    assert "SparkSession" in code
    assert "df_" in code


def test_dag_optimizer_does_not_break_dag(sample_dag):
    from dag.optimizer import DAGOptimizer
    optimized = DAGOptimizer().optimize(sample_dag)
    assert len(optimized.nodes) == len(sample_dag.nodes)


def test_dag_patcher_add_filter(sample_dag):
    from dag.nodes import ReadNode, FilterNode
    from dag.patch import DAGPatcher
    read_node = next(n for n in sample_dag.nodes.values() if isinstance(n, ReadNode))
    patched = DAGPatcher().apply(
        sample_dag, "add_filter", None,
        {"after_node_id": read_node.id, "predicate": "status != 'cancelled'"}
    )
    filter_nodes = [n for n in patched.nodes.values() if isinstance(n, FilterNode)]
    assert len(filter_nodes) >= 1
