"""
tests/conftest.py
Shared pytest fixtures for all test modules.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


# ── Mock connectors ───────────────────────────────────────────

@pytest.fixture
def mock_source_connector():
    """Returns a connected mock object-storage connector."""
    from connectors.mock.object_storage import MockObjectStorageConnector
    conn = MockObjectStorageConnector("source", {})
    conn.connect()
    return conn


@pytest.fixture
def mock_dest_connector():
    """Returns a connected mock warehouse connector."""
    from connectors.mock.warehouse import MockWarehouseConnector
    conn = MockWarehouseConnector("destination", {})
    conn.connect()
    return conn


@pytest.fixture
def mock_rdbms_connector():
    from connectors.mock.rdbms import MockRDBMSConnector
    conn = MockRDBMSConnector("rdbms_src", {})
    conn.connect()
    return conn


@pytest.fixture
def mock_erp_connector():
    from connectors.mock.erp import MockERPConnector
    conn = MockERPConnector("erp_src", {})
    conn.connect()
    return conn


# ── Schema graph ─────────────────────────────────────────────

@pytest.fixture
def sample_schema_graph(mock_source_connector, mock_dest_connector):
    """Build a real schema graph from mock connectors."""
    from graph.builder import GraphBuilder
    builder = GraphBuilder(mock_source_connector, mock_dest_connector)
    return builder.build()


# ── Grounded intent ──────────────────────────────────────────

@pytest.fixture
def sample_grounded_intent(sample_schema_graph):
    """Create a synthetic GroundedIntent for testing."""
    from intent.schema import (
        GroundedIntent, IntentJSON, OperationType, ResolvedTable
    )
    nodes = list(sample_schema_graph.nodes.values())
    src_node = next((n for n in nodes if n.connector_id == "source"), nodes[0])
    tgt_node = next((n for n in nodes if n.connector_id == "destination"), nodes[-1])

    raw = IntentJSON(
        operation=OperationType.COPY,
        source_tables=[src_node.name],
        target_table=tgt_node.name,
        user_nl_command="copy orders to warehouse",
    )
    grounded = GroundedIntent(
        original_intent=raw,
        operation=OperationType.COPY,
        source_tables=[ResolvedTable(
            entity_name=src_node.name,
            node_id=src_node.id,
            node_qualified_name=src_node.qualified_name,
            confidence=1.0,
        )],
        target_table=ResolvedTable(
            entity_name=tgt_node.name,
            node_id=tgt_node.id,
            node_qualified_name=tgt_node.qualified_name,
            confidence=1.0,
        ),
    )
    return grounded


# ── DAG ──────────────────────────────────────────────────────

@pytest.fixture
def sample_dag(sample_grounded_intent, sample_schema_graph):
    """Build a DAG from the sample grounded intent."""
    from dag.builder import DAGBuilder
    builder = DAGBuilder(sample_schema_graph)
    return builder.build(sample_grounded_intent)


# ── Mock LLM ─────────────────────────────────────────────────

@pytest.fixture
def mock_llm():
    """Returns a mock LLM client that returns a fixed IntentJSON."""
    import json
    mock = MagicMock()
    mock.complete.return_value = json.dumps({
        "operation": "copy",
        "source_tables": ["orders.parquet"],
        "target_table": "fact_orders",
        "filters": [],
        "joins": [],
        "transforms": [],
        "column_mappings": [],
        "output_columns": [],
        "incremental": None,
        "dry_run": False,
    })
    return mock
