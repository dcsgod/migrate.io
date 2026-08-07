"""
connectors/mock/erp.py
Mock ERP connector — simulates SAP ECC/S4HANA business objects.
"""
from __future__ import annotations

from typing import Any

from connectors import register
from connectors.base.capabilities import AuthMethod, ConnectorCapabilities, ReadShape, WriteShape
from connectors.base.connector import Connector
from graph.models import ColumnDef, ColumnType, GraphNode, NodeKind


@register("mock_erp")
class MockERPConnector(Connector):
    """Simulates SAP ECC / S4HANA business objects via OData / RFC."""

    _MOCK_OBJECTS = [
        {
            "id": "BKPF",
            "name": "BKPF",
            "qualified_name": "SAP.FI.BKPF",
            "description": "Accounting Document Header",
            "columns": [
                ("MANDT", ColumnType.STRING, False, True),    # Client
                ("BUKRS", ColumnType.STRING, False, True),    # Company Code
                ("BELNR", ColumnType.STRING, False, True),    # Document Number
                ("GJAHR", ColumnType.INTEGER, False, True),   # Fiscal Year
                ("BLART", ColumnType.STRING, True, False),    # Document Type
                ("BLDAT", ColumnType.DATE, True, False),      # Document Date
                ("BUDAT", ColumnType.DATE, True, False),      # Posting Date
                ("MONAT", ColumnType.INTEGER, True, False),   # Fiscal Period
                ("XBLNR", ColumnType.STRING, True, False),    # Reference Document
                ("BKTXT", ColumnType.STRING, True, False),    # Document Header Text
                ("WAERS", ColumnType.STRING, True, False),    # Currency
                ("USNAM", ColumnType.STRING, True, False),    # User Name
            ],
            "row_count": 5_000_000,
            "size_bytes": 200_000_000,
        },
        {
            "id": "BSEG",
            "name": "BSEG",
            "qualified_name": "SAP.FI.BSEG",
            "description": "Accounting Document Segment",
            "columns": [
                ("MANDT", ColumnType.STRING, False, True),
                ("BUKRS", ColumnType.STRING, False, True),
                ("BELNR", ColumnType.STRING, False, True),
                ("GJAHR", ColumnType.INTEGER, False, True),
                ("BUZEI", ColumnType.INTEGER, False, True),   # Line Item
                ("KOART", ColumnType.STRING, True, False),    # Account Type
                ("HKONT", ColumnType.STRING, True, False),    # G/L Account
                ("DMBTR", ColumnType.DECIMAL, True, False),   # Amount in Local Currency
                ("WRBTR", ColumnType.DECIMAL, True, False),   # Amount in Document Currency
                ("SGTXT", ColumnType.STRING, True, False),    # Item Text
                ("KOSTL", ColumnType.STRING, True, False),    # Cost Center
                ("PRCTR", ColumnType.STRING, True, False),    # Profit Center
            ],
            "row_count": 25_000_000,
            "size_bytes": 1_200_000_000,
        },
        {
            "id": "KNA1",
            "name": "KNA1",
            "qualified_name": "SAP.SD.KNA1",
            "description": "Customer Master — General Data",
            "columns": [
                ("MANDT", ColumnType.STRING, False, True),
                ("KUNNR", ColumnType.STRING, False, True),    # Customer Number
                ("LAND1", ColumnType.STRING, True, False),    # Country
                ("NAME1", ColumnType.STRING, True, False),    # Name 1
                ("ORT01", ColumnType.STRING, True, False),    # City
                ("STRAS", ColumnType.STRING, True, False),    # Street
                ("TELF1", ColumnType.STRING, True, False),    # Phone
                ("SMTP_ADDR", ColumnType.STRING, True, False), # Email
                ("KTOKD", ColumnType.STRING, True, False),    # Account Group
                ("ERDAT", ColumnType.DATE, True, False),      # Created On
            ],
            "row_count": 180_000,
            "size_bytes": 9_000_000,
        },
    ]

    def connect(self) -> None:
        self._connected = True

    def list_objects(self) -> list[dict[str, Any]]:
        return [
            {
                "id": o["id"],
                "name": o["name"],
                "qualified_name": o["qualified_name"],
                "kind": "object",
                "description": o["description"],
            }
            for o in self._MOCK_OBJECTS
        ]

    def read_schema(self, object_id: str) -> GraphNode:
        obj = next((o for o in self._MOCK_OBJECTS if o["id"] == object_id), None)
        if obj is None:
            from connectors.base.connector import SchemaDiscoveryError
            raise SchemaDiscoveryError(f"Mock ERP object not found: {object_id}")
        columns = [
            ColumnDef(name=col[0], dtype=col[1], nullable=col[2], primary_key=col[3])
            for col in obj["columns"]
        ]
        node_id = GraphNode.make_id(self._connector_id, obj["qualified_name"])
        return GraphNode(
            id=node_id,
            name=obj["name"],
            qualified_name=obj["qualified_name"],
            connector_id=self._connector_id,
            kind=NodeKind.OBJECT,
            columns=columns,
            row_count=obj["row_count"],
            size_bytes=obj["size_bytes"],
            metadata={"description": obj["description"], "system": "SAP"},
        )

    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        from pyspark.sql import Row
        obj = next((o for o in self._MOCK_OBJECTS if o["id"] == object_id), None)
        if obj is None:
            from connectors.base.connector import ReadError
            raise ReadError(f"Mock ERP object not found: {object_id}")
        data = [Row(**{col[0]: None for col in obj["columns"]})]
        return spark.createDataFrame(data)

    def write(self, spark: Any, df: Any, target_id: str, mode: str = "overwrite", options: dict[str, Any] | None = None) -> None:
        raise NotImplementedError("ERP connectors are read-only — writing back to SAP is not supported.")

    def capabilities(self) -> ConnectorCapabilities:
        return ConnectorCapabilities(
            read_shapes=frozenset([ReadShape.OBJECTS]),
            write_shapes=frozenset(),   # ERP is source-only
            supports_bulk=True,
            supports_cdc=False,
            supports_incremental=True,
            supports_schema_discovery=True,
            auth_methods=frozenset([AuthMethod.RFC, AuthMethod.BASIC]),
        )
