"""
tests/connectors/test_mock.py
Tests every mock connector satisfies the Connector interface contract.
"""
import pytest


def test_mock_object_storage_connect(mock_source_connector):
    assert mock_source_connector.is_connected


def test_mock_object_storage_list_objects(mock_source_connector):
    objects = mock_source_connector.list_objects()
    assert len(objects) > 0
    assert all("id" in o and "name" in o for o in objects)


def test_mock_object_storage_read_schema(mock_source_connector):
    objects = mock_source_connector.list_objects()
    schema = mock_source_connector.read_schema(objects[0]["id"])
    assert schema.name
    assert len(schema.columns) > 0
    assert schema.connector_id == "source"


def test_mock_object_storage_capabilities(mock_source_connector):
    caps = mock_source_connector.capabilities()
    assert caps.can_read()


def test_mock_warehouse_connect(mock_dest_connector):
    assert mock_dest_connector.is_connected


def test_mock_warehouse_list_objects(mock_dest_connector):
    objects = mock_dest_connector.list_objects()
    assert len(objects) == 3


def test_mock_warehouse_read_schema(mock_dest_connector):
    schema = mock_dest_connector.read_schema("dim_customer")
    assert schema.name == "dim_customer"
    assert any(c.primary_key for c in schema.columns)


def test_mock_warehouse_time_travel(mock_dest_connector):
    caps = mock_dest_connector.capabilities()
    assert caps.supports_time_travel


def test_mock_rdbms_fk_edges(mock_rdbms_connector):
    """FK columns should be declared on the orders table."""
    schema = mock_rdbms_connector.read_schema("orders_rdbms")
    fk_cols = [c for c in schema.columns if c.foreign_key]
    assert len(fk_cols) >= 1


def test_mock_erp_read_only(mock_erp_connector):
    """ERP connector should not support writes."""
    caps = mock_erp_connector.capabilities()
    assert not caps.can_write()


def test_mock_erp_objects(mock_erp_connector):
    objects = mock_erp_connector.list_objects()
    assert any(o["id"] == "BKPF" for o in objects)
