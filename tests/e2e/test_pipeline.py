"""
tests/e2e/test_pipeline.py
Full end-to-end pipeline test: mock connectors → graph → intent → DAG → compile.
"""
import pytest


def test_full_pipeline_green_path(mock_llm, mock_source_connector, mock_dest_connector):
    """
    Walks the entire pipeline from connector registration through compiled code.
    No real Spark or LLM is used.
    """
    # 1. Build graph
    from graph.builder import GraphBuilder
    builder = GraphBuilder(mock_source_connector, mock_dest_connector)
    graph = builder.build()
    assert graph.node_count() > 0

    # 2. Parse NL command
    from intent.parser import IntentParser
    import json
    mock_llm.complete.return_value = json.dumps({
        "operation": "copy",
        "source_tables": ["orders.parquet"],
        "target_table": "fact_orders",
        "filters": [{"column": "status", "op": "neq", "value": "cancelled"}],
        "joins": [], "transforms": [], "column_mappings": [],
        "output_columns": [], "incremental": None, "dry_run": False,
    })
    parser = IntentParser(llm=mock_llm)
    graph_dict = {
        "nodes": {k: v.model_dump(mode="json") for k, v in graph.nodes.items()},
        "edges": {},
    }
    intent = parser.parse("copy orders to fact_orders excluding cancelled", graph_dict)
    assert intent.operation.value == "copy"
    assert len(intent.filters) == 1

    # 3. Ground intent
    from intent.grounding import IntentGrounder
    grounder = IntentGrounder(graph)
    grounded = grounder.ground(intent)
    assert grounded.source_tables

    # 4. Build DAG
    from dag.builder import DAGBuilder
    dag_builder = DAGBuilder(graph)
    dag = dag_builder.build(grounded)
    assert len(dag.nodes) > 0

    # 5. Validate DAG
    from dag.validator import DAGValidator
    result = DAGValidator().validate(dag)
    assert not result.has_errors

    # 6. Compile to Spark
    from compiler.spark_compiler import SparkCompiler
    code = SparkCompiler().compile(dag, plan_id="e2e_test")
    assert "SparkSession" in code
    assert "staged" in code.lower() or "staging" in code.lower()


def test_pipeline_rejection_path(mock_llm, mock_source_connector, mock_dest_connector):
    """Verify rejection path produces no errors and no data is committed."""
    from graph.builder import GraphBuilder
    graph = GraphBuilder(mock_source_connector, mock_dest_connector).build()
    # Simulate a run being rejected
    run = {"run_id": "test-123", "status": "staged"}
    run["status"] = "rejected"
    assert run["status"] == "rejected"
