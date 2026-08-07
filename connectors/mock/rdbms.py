"""
connectors/mock/rdbms.py
Mock RDBMS connector — simulates a Postgres/MySQL/Oracle/Teradata source.
"""
from __future__ import annotations

from typing import Any

from connectors import register
from connectors.base.capabilities import AuthMethod, ConnectorCapabilities, ReadShape, WriteShape
from connectors.base.connector import Connector
from graph.models import ColumnDef, ColumnType, GraphNode, NodeKind


@register("mock_rdbms")
class MockRDBMSConnector(Connector):
    """Simulates a relational database with FK-declared relationships."""

    _MOCK_TABLES = [
        {
            "id": "users",
            "name": "users",
            "qualified_name": "mock_db.public.users",
            "columns": [
                ("user_id", ColumnType.INTEGER, False, True, None),
                ("username", ColumnType.STRING, False, False, None),
                ("email", ColumnType.STRING, False, False, None),
                ("password_hash", ColumnType.STRING, False, False, None),
                ("created_at", ColumnType.TIMESTAMP, False, False, None),
                ("last_login", ColumnType.TIMESTAMP, True, False, None),
                ("is_verified", ColumnType.BOOLEAN, True, False, None),
            ],
            "row_count": 100_000,
            "size_bytes": 5_000_000,
        },
        {
            "id": "orders_rdbms",
            "name": "orders",
            "qualified_name": "mock_db.public.orders",
            "columns": [
                ("order_id", ColumnType.INTEGER, False, True, None),
                ("user_id", ColumnType.INTEGER, False, False, "mock_db.public.users.user_id"),
                ("product_id", ColumnType.INTEGER, False, False, "mock_db.public.products.product_id"),
                ("quantity", ColumnType.INTEGER, False, False, None),
                ("order_total", ColumnType.DECIMAL, False, False, None),
                ("placed_at", ColumnType.TIMESTAMP, False, False, None),
                ("shipped_at", ColumnType.TIMESTAMP, True, False, None),
                ("status", ColumnType.STRING, False, False, None),
            ],
            "row_count": 500_000,
            "size_bytes": 25_000_000,
        },
        {
            "id": "products_rdbms",
            "name": "products",
            "qualified_name": "mock_db.public.products",
            "columns": [
                ("product_id", ColumnType.INTEGER, False, True, None),
                ("name", ColumnType.STRING, False, False, None),
                ("description", ColumnType.STRING, True, False, None),
                ("price", ColumnType.DECIMAL, False, False, None),
                ("stock", ColumnType.INTEGER, False, False, None),
                ("category_id", ColumnType.INTEGER, True, False, "mock_db.public.categories.category_id"),
            ],
            "row_count": 10_000,
            "size_bytes": 600_000,
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
            raise SchemaDiscoveryError(f"Mock RDBMS table not found: {object_id}")
        columns = [
            ColumnDef(name=col[0], dtype=col[1], nullable=col[2], primary_key=col[3], foreign_key=col[4])
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
        )

    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        from pyspark.sql import Row
        tbl = next((t for t in self._MOCK_TABLES if t["id"] == object_id), None)
        if tbl is None:
            from connectors.base.connector import ReadError
            raise ReadError(f"Mock RDBMS table not found: {object_id}")
        data = [Row(**{col[0]: None for col in tbl["columns"]})]
        return spark.createDataFrame(data)

    def write(self, spark: Any, df: Any, target_id: str, mode: str = "overwrite", options: dict[str, Any] | None = None) -> None:
        pass

    def capabilities(self) -> ConnectorCapabilities:
        return ConnectorCapabilities(
            read_shapes=frozenset([ReadShape.TABLES]),
            write_shapes=frozenset([WriteShape.TABLES]),
            supports_bulk=True,
            supports_cdc=True,
            supports_incremental=True,
            supports_schema_discovery=True,
            auth_methods=frozenset([AuthMethod.BASIC]),
        )

    def get_source_stats(self, object_id: str) -> dict[str, Any]:
        tbl = next((t for t in self._MOCK_TABLES if t["id"] == object_id), None)
        if not tbl:
            return {}
        return {"row_count": tbl["row_count"], "size_bytes": tbl["size_bytes"], "partition_count": 4}
