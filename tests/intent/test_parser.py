"""
tests/intent/test_parser.py
Tests IntentParser with a mocked LLM — no real API calls.
"""
import json
import pytest
from intent.parser import IntentParser
from intent.schema import OperationType


def test_parser_returns_intent_json(mock_llm, sample_schema_graph):
    parser = IntentParser(llm=mock_llm)
    graph_dict = {
        "nodes": {k: v.model_dump(mode="json") for k, v in sample_schema_graph.nodes.items()},
        "edges": {},
    }
    intent = parser.parse("copy orders to warehouse", graph_dict)
    assert intent.operation == OperationType.COPY
    assert len(intent.source_tables) > 0
    assert intent.target_table


def test_parser_records_nl_command(mock_llm, sample_schema_graph):
    parser = IntentParser(llm=mock_llm)
    graph_dict = {
        "nodes": {k: v.model_dump(mode="json") for k, v in sample_schema_graph.nodes.items()},
        "edges": {},
    }
    intent = parser.parse("copy orders to warehouse", graph_dict)
    assert intent.user_nl_command == "copy orders to warehouse"


def test_parser_handles_invalid_json(mock_llm, sample_schema_graph):
    mock_llm.complete.return_value = "This is not JSON"
    parser = IntentParser(llm=mock_llm)
    with pytest.raises(ValueError, match="invalid JSON"):
        parser.parse("copy orders", {"nodes": {}, "edges": {}})
