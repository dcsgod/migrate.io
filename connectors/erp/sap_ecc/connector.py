"""
connectors/erp/sap_ecc/connector.py
SAP ECC connector via RFC (pyrfc) — reads ABAP tables and business objects.
"""
from __future__ import annotations

from typing import Any

import structlog

from connectors import register
from connectors.base.capabilities import AuthMethod, ConnectorCapabilities, ReadShape, WriteShape
from connectors.base.connector import Connector, ConnectionError, ReadError, SchemaDiscoveryError
from graph.models import ColumnDef, ColumnType, GraphNode, NodeKind

logger = structlog.get_logger(__name__)

_ABAP_TYPE_MAP = {
    "C": ColumnType.STRING, "N": ColumnType.STRING,   # CHAR, NUMC
    "D": ColumnType.DATE, "T": ColumnType.STRING,      # DATE, TIME
    "I": ColumnType.INTEGER, "B": ColumnType.INTEGER,  # INT4, INT2
    "P": ColumnType.DECIMAL, "F": ColumnType.DOUBLE,   # PACKED, FLOAT
    "X": ColumnType.BINARY, "G": ColumnType.STRING,    # RAW, STRING
}


@register("sap_ecc")
class SAPECCConnector(Connector):
    """
    SAP ECC connector using RFC function modules.

    Config keys:
        host, sysnr, client, user, password, language
        Reads via RFC_READ_TABLE or custom extractors.
    """

    def __init__(self, connector_id: str, config: dict[str, Any]) -> None:
        super().__init__(connector_id, config)
        self._conn: Any = None

    def connect(self) -> None:
        try:
            import pyrfc
            self._conn = pyrfc.Connection(
                ashost=self._config.get("host", ""),
                sysnr=self._config.get("sysnr", "00"),
                client=self._config.get("client", "100"),
                user=self._config.get("user", ""),
                passwd=self._config.get("password", ""),
                lang=self._config.get("language", "EN"),
            )
            # Test with a ping
            self._conn.call("RFC_PING")
            self._connected = True
            logger.info("sap_ecc.connected", host=self._config.get("host"))
        except Exception as exc:
            raise ConnectionError(f"SAP ECC connection failed: {exc}") from exc

    def list_objects(self) -> list[dict[str, Any]]:
        """List SAP transparent tables using DD02L."""
        try:
            result = self._conn.call(
                "RFC_READ_TABLE",
                QUERY_TABLE="DD02L",
                FIELDS=[{"FIELDNAME": "TABNAME"}, {"FIELDNAME": "TABTYPE"}, {"FIELDNAME": "DDTEXT"}],
                OPTIONS=[{"TEXT": "TABTYPE EQ 'TRANSP'"}],
                ROWCOUNT=500,
            )
            rows = result.get("DATA", [])
            objects = []
            for row in rows:
                parts = row["WA"].split()
                if parts:
                    objects.append({
                        "id": parts[0],
                        "name": parts[0],
                        "qualified_name": f"SAP.ECC.{parts[0]}",
                        "kind": "object",
                        "description": " ".join(parts[2:]) if len(parts) > 2 else "",
                    })
            return objects
        except Exception as exc:
            raise SchemaDiscoveryError(f"SAP ECC list_objects failed: {exc}") from exc

    def read_schema(self, object_id: str) -> GraphNode:
        """Read field definitions from DD03L for a given table."""
        try:
            result = self._conn.call(
                "RFC_READ_TABLE",
                QUERY_TABLE="DD03L",
                FIELDS=[
                    {"FIELDNAME": "FIELDNAME"},
                    {"FIELDNAME": "INTTYPE"},
                    {"FIELDNAME": "KEYFLAG"},
                    {"FIELDNAME": "DDTEXT"},
                ],
                OPTIONS=[{"TEXT": f"TABNAME EQ '{object_id}'"}],
            )
            columns = []
            for row in result.get("DATA", []):
                parts = row["WA"].split()
                if not parts or parts[0].startswith("."):
                    continue
                fname = parts[0]
                inttype = parts[1] if len(parts) > 1 else "C"
                is_key = parts[2].strip() == "X" if len(parts) > 2 else False
                columns.append(ColumnDef(
                    name=fname,
                    dtype=_ABAP_TYPE_MAP.get(inttype, ColumnType.STRING),
                    nullable=not is_key,
                    primary_key=is_key,
                    description=" ".join(parts[3:]) if len(parts) > 3 else None,
                ))
            qname = f"SAP.ECC.{object_id}"
            return GraphNode(
                id=GraphNode.make_id(self._connector_id, qname),
                name=object_id,
                qualified_name=qname,
                connector_id=self._connector_id,
                kind=NodeKind.OBJECT,
                columns=columns,
                metadata={"system": "SAP ECC"},
            )
        except Exception as exc:
            raise SchemaDiscoveryError(f"SAP ECC read_schema failed for {object_id}: {exc}") from exc

    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        """Read table data via RFC_READ_TABLE, convert to Spark DataFrame."""
        opts = options or {}
        row_limit = opts.get("row_limit", 10_000)
        try:
            result = self._conn.call(
                "RFC_READ_TABLE",
                QUERY_TABLE=object_id,
                ROWCOUNT=row_limit,
            )
            fields = [f["FIELDNAME"] for f in result.get("FIELDS", [])]
            field_lengths = [int(f.get("LENGTH", 10)) for f in result.get("FIELDS", [])]
            rows = []
            for row in result.get("DATA", []):
                wa = row["WA"]
                record: dict[str, Any] = {}
                pos = 0
                for fname, flen in zip(fields, field_lengths):
                    record[fname] = wa[pos: pos + flen].strip()
                    pos += flen
                rows.append(record)
            return spark.createDataFrame(rows) if rows else spark.createDataFrame([], schema=",".join(f"{f} STRING" for f in fields))
        except Exception as exc:
            raise ReadError(f"SAP ECC read failed for {object_id}: {exc}") from exc

    def write(self, spark: Any, df: Any, target_id: str, mode: str = "overwrite", options: dict[str, Any] | None = None) -> None:
        raise NotImplementedError("SAP ECC is a read-only source — writes are not supported.")

    def close(self) -> None:
        if self._conn:
            self._conn.close()
        super().close()

    def capabilities(self) -> ConnectorCapabilities:
        return ConnectorCapabilities(
            read_shapes=frozenset([ReadShape.OBJECTS]),
            write_shapes=frozenset(),
            supports_bulk=True,
            supports_incremental=True,
            auth_methods=frozenset([AuthMethod.RFC]),
        )
