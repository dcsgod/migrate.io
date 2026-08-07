"""
connectors/mock/warehouse.py
Mock warehouse connector — simulates Databricks/Snowflake table catalog.
"""
from __future__ import annotations

from typing import Any

from connectors import register
from connectors.base.capabilities import AuthMethod, ConnectorCapabilities, ReadShape, WriteShape
from connectors.base.connector import Connector
from graph.models import ColumnDef, ColumnType, GraphNode, NodeKind


@register("mock_warehouse")
class MockWarehouseConnector(Connector):
    """Simulates a Databricks/Snowflake/BigQuery table source."""

    _MOCK_TABLES = [
        {
            "id": "dim_customer",
            "name": "dim_customer",
            "qualified_name": "mock_catalog.gold.dim_customer",
            "columns": [
                ("customer_id", ColumnType.LONG, False, True),
                ("customer_key", ColumnType.STRING, False, False),
                ("full_name", ColumnType.STRING, True, False),
                ("email", ColumnType.STRING, True, False),
                ("phone", ColumnType.STRING, True, False),
                ("country_code", ColumnType.STRING, True, False),
                ("segment", ColumnType.STRING, True, False),
                ("lifetime_value", ColumnType.DOUBLE, True, False),
                ("is_active", ColumnType.BOOLEAN, True, False),
                ("created_at", ColumnType.TIMESTAMP, False, False),
                ("updated_at", ColumnType.TIMESTAMP, True, False),
            ],
            "row_count": 250_000,
            "size_bytes": 10_000_000,
            "partition_keys": ["country_code"],
        },
        {
            "id": "fact_orders",
            "name": "fact_orders",
            "qualified_name": "mock_catalog.gold.fact_orders",
            "columns": [
                ("order_id", ColumnType.LONG, False, True),
                ("customer_id", ColumnType.LONG, False, False),
                ("product_id", ColumnType.STRING, True, False),
                ("order_date", ColumnType.DATE, False, False),
                ("ship_date", ColumnType.DATE, True, False),
                ("quantity", ColumnType.INTEGER, True, False),
                ("unit_price", ColumnType.DOUBLE, True, False),
                ("discount", ColumnType.DOUBLE, True, False),
                ("total_amount", ColumnType.DOUBLE, True, False),
                ("status", ColumnType.STRING, True, False),
                ("region", ColumnType.STRING, True, False),
            ],
            "row_count": 8_000_000,
            "size_bytes": 320_000_000,
            "partition_keys": ["order_date"],
        },
        {
            "id": "dim_product",
            "name": "dim_product",
            "qualified_name": "mock_catalog.gold.dim_product",
            "columns": [
                ("product_id", ColumnType.STRING, False, True),
                ("sku", ColumnType.STRING, False, False),
                ("product_name", ColumnType.STRING, False, False),
                ("category", ColumnType.STRING, True, False),
                ("subcategory", ColumnType.STRING, True, False),
                ("brand", ColumnType.STRING, True, False),
                ("cost_price", ColumnType.DECIMAL, True, False),
                ("sell_price", ColumnType.DECIMAL, True, False),
                ("is_discontinued", ColumnType.BOOLEAN, True, False),
            ],
            "row_count": 50_000,
            "size_bytes": 3_000_000,
            "partition_keys": [],
        },
    ]

    def connect(self) -> None:
        self._connected = True

    def list_objects(self) -> list[dict[str, Any]]:
        return [
            {"id": t["id"], "name": t["name"], "qualified_name": t["qualified_name"], "kind": "table"}
            for t in self._MOCK_TABLES
        ]

    def read_schema(self, object_id: str) -> GraphNode:
        tbl = next((t for t in self._MOCK_TABLES if t["id"] == object_id), None)
        if tbl is None:
            from connectors.base.connector import SchemaDiscoveryError
            raise SchemaDiscoveryError(f"Mock table not found: {object_id}")
        columns = [
            ColumnDef(name=col[0], dtype=col[1], nullable=col[2], primary_key=col[3])
            for col in tbl["columns"]
        ]
        node_id = GraphNode.make_id(self._connector_id, tbl["qualified_name"])
        return GraphNode(
            id=node_id,
            name=tbl["name"],
            qualified_name=tbl["qualified_name"],
            connector_id=self._connector_id,
            kind=NodeKind.TABLE,
            columns=columns,
            row_count=tbl["row_count"],
            size_bytes=tbl["size_bytes"],
            partition_keys=tbl["partition_keys"],
        )

    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        from pyspark.sql import Row
        tbl = next((t for t in self._MOCK_TABLES if t["id"] == object_id), None)
        if tbl is None:
            from connectors.base.connector import ReadError
            raise ReadError(f"Mock table not found: {object_id}")
        data = [Row(**{col[0]: None for col in tbl["columns"]})]
        return spark.createDataFrame(data)

    def write(self, spark: Any, df: Any, target_id: str, mode: str = "overwrite", options: dict[str, Any] | None = None) -> None:
        pass  # no-op mock

    def capabilities(self) -> ConnectorCapabilities:
        return ConnectorCapabilities(
            read_shapes=frozenset([ReadShape.TABLES]),
            write_shapes=frozenset([WriteShape.TABLES]),
            supports_bulk=True,
            supports_cdc=True,
            supports_incremental=True,
            supports_atomic_write=True,
            supports_time_travel=True,
            supports_schema_evolution=True,
            auth_methods=frozenset([AuthMethod.API_KEY, AuthMethod.OAUTH2]),
        )

    def get_source_stats(self, object_id: str) -> dict[str, Any]:
        tbl = next((t for t in self._MOCK_TABLES if t["id"] == object_id), None)
        if not tbl:
            return {}
        return {
            "row_count": tbl["row_count"],
            "size_bytes": tbl["size_bytes"],
            "partition_count": max(1, tbl["size_bytes"] // (128 * 1024 * 1024)),
        }
