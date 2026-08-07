"""
connectors/mock/object_storage.py
Mock object-storage connector — returns synthetic Parquet-schema nodes.
Used for local dev / testing without real cloud credentials.
"""
from __future__ import annotations

from typing import Any

from connectors import register
from connectors.base.capabilities import AuthMethod, ConnectorCapabilities, ReadShape, WriteShape
from connectors.base.connector import Connector
from graph.models import ColumnDef, ColumnType, GraphNode, NodeKind


@register("mock_object_storage")
class MockObjectStorageConnector(Connector):
    """Simulates an S3/ADLS/GCS object storage source with synthetic data."""

    _MOCK_OBJECTS = [
        {
            "id": "orders_parquet",
            "name": "orders.parquet",
            "qualified_name": "mock-bucket/raw/orders.parquet",
            "kind": "file",
            "columns": [
                ("order_id", ColumnType.LONG, True),
                ("customer_id", ColumnType.LONG, False),
                ("order_date", ColumnType.DATE, False),
                ("status", ColumnType.STRING, True),
                ("total_amount", ColumnType.DOUBLE, True),
                ("currency", ColumnType.STRING, True),
                ("region", ColumnType.STRING, True),
            ],
            "row_count": 1_500_000,
            "size_bytes": 45_000_000,
        },
        {
            "id": "customers_parquet",
            "name": "customers.parquet",
            "qualified_name": "mock-bucket/raw/customers.parquet",
            "kind": "file",
            "columns": [
                ("cust_id", ColumnType.LONG, True),
                ("first_name", ColumnType.STRING, True),
                ("last_name", ColumnType.STRING, True),
                ("email", ColumnType.STRING, True),
                ("phone", ColumnType.STRING, True),
                ("country", ColumnType.STRING, True),
                ("created_at", ColumnType.TIMESTAMP, False),
            ],
            "row_count": 250_000,
            "size_bytes": 8_500_000,
        },
        {
            "id": "products_json",
            "name": "products.json",
            "qualified_name": "mock-bucket/raw/products.json",
            "kind": "file",
            "columns": [
                ("product_id", ColumnType.STRING, True),
                ("sku", ColumnType.STRING, False),
                ("name", ColumnType.STRING, False),
                ("category", ColumnType.STRING, True),
                ("price", ColumnType.DECIMAL, True),
                ("stock_quantity", ColumnType.INTEGER, True),
            ],
            "row_count": 12_000,
            "size_bytes": 900_000,
        },
    ]

    def connect(self) -> None:
        self._connected = True

    def list_objects(self) -> list[dict[str, Any]]:
        return [
            {"id": o["id"], "name": o["name"], "qualified_name": o["qualified_name"], "kind": o["kind"]}
            for o in self._MOCK_OBJECTS
        ]

    def read_schema(self, object_id: str) -> GraphNode:
        obj = next((o for o in self._MOCK_OBJECTS if o["id"] == object_id), None)
        if obj is None:
            from connectors.base.connector import SchemaDiscoveryError
            raise SchemaDiscoveryError(f"Mock object not found: {object_id}")
        columns = [
            ColumnDef(name=col[0], dtype=col[1], nullable=col[2])
            for col in obj["columns"]
        ]
        node_id = GraphNode.make_id(self._connector_id, obj["qualified_name"])
        return GraphNode(
            id=node_id,
            name=obj["name"],
            qualified_name=obj["qualified_name"],
            connector_id=self._connector_id,
            kind=NodeKind.FILE,
            columns=columns,
            row_count=obj["row_count"],
            size_bytes=obj["size_bytes"],
        )

    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        # Return a tiny Spark DataFrame of synthetic data
        obj = next((o for o in self._MOCK_OBJECTS if o["id"] == object_id), None)
        if obj is None:
            from connectors.base.connector import ReadError
            raise ReadError(f"Mock object not found: {object_id}")

        from pyspark.sql import Row
        from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

        # Minimal synthetic rows
        data = [Row(**{col[0]: None for col in obj["columns"]})]
        return spark.createDataFrame(data)

    def write(self, spark: Any, df: Any, target_id: str, mode: str = "overwrite", options: dict[str, Any] | None = None) -> None:
        # No-op for mock
        pass

    def capabilities(self) -> ConnectorCapabilities:
        return ConnectorCapabilities(
            read_shapes=frozenset([ReadShape.FILES]),
            write_shapes=frozenset([WriteShape.FILES]),
            supports_bulk=True,
            supports_cdc=False,
            supports_incremental=True,
            auth_methods=frozenset([AuthMethod.IAM_ROLE, AuthMethod.API_KEY]),
        )

    def get_source_stats(self, object_id: str) -> dict[str, Any]:
        obj = next((o for o in self._MOCK_OBJECTS if o["id"] == object_id), None)
        if not obj:
            return {}
        return {"row_count": obj["row_count"], "size_bytes": obj["size_bytes"], "partition_count": 1}
