"""
tests/graph/test_builder.py
Tests graph building from mock connectors.
"""
import pytest
from graph.builder import GraphBuilder


def test_graph_builds_from_mock_connectors(mock_source_connector, mock_dest_connector):
    builder = GraphBuilder(mock_source_connector, mock_dest_connector)
    graph = builder.build()
    assert graph.node_count() > 0
    assert graph.source_connector_id == "source"
    assert graph.dest_connector_id == "destination"


def test_graph_has_nodes_from_both_connectors(sample_schema_graph):
    node_connectors = {n.connector_id for n in sample_schema_graph.nodes.values()}
    assert "source" in node_connectors
    assert "destination" in node_connectors


def test_graph_drift_empty_on_first_build(sample_schema_graph):
    assert len(sample_schema_graph.drift) == 0


def test_graph_explicit_edges_from_fk(mock_rdbms_connector, mock_dest_connector):
    """FK-declared columns should produce explicit edges."""
    builder = GraphBuilder(mock_rdbms_connector, mock_dest_connector)
    graph = builder.build()
    explicit_edges = [e for e in graph.edges.values() if e.kind.value == "explicit"]
    assert len(explicit_edges) >= 1


def test_schema_fingerprint_stable(sample_schema_graph):
    """Same node should produce the same fingerprint."""
    for node in sample_schema_graph.nodes.values():
        fp1 = node.schema_fingerprint()
        fp2 = node.schema_fingerprint()
        assert fp1 == fp2
