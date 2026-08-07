"""
connectors/warehouse/databricks/connector.py
Databricks connector — Unity Catalog + Databricks SDK.
"""
from __future__ import annotations

from typing import Any

import structlog

from connectors import register
from connectors.base.capabilities import AuthMethod, ConnectorCapabilities, ReadShape, WriteShape
from connectors.base.connector import Connector, ConnectionError, ReadError, SchemaDiscoveryError, WriteError
from graph.models import ColumnDef, ColumnType, GraphNode, NodeKind

logger = structlog.get_logger(__name__)

_TYPE_MAP = {
    "string": ColumnType.STRING, "int": ColumnType.INTEGER, "integer": ColumnType.INTEGER,
    "long": ColumnType.LONG, "bigint": ColumnType.LONG, "float": ColumnType.FLOAT,
    "double": ColumnType.DOUBLE, "boolean": ColumnType.BOOLEAN, "bool": ColumnType.BOOLEAN,
    "date": ColumnType.DATE, "timestamp": ColumnType.TIMESTAMP,
    "binary": ColumnType.BINARY, "decimal": ColumnType.DECIMAL,
    "struct": ColumnType.STRUCT, "array": ColumnType.ARRAY, "map": ColumnType.MAP,
}


@register("databricks")
class DatabricksConnector(Connector):
    """
    Databricks connector using databricks-sdk.

    Config keys:
        host          : Databricks workspace URL (https://...)
        token         : Personal access token or service principal token
        catalog       : Unity Catalog catalog name
        schema        : Schema / database name
        cluster_id    : Optional: existing all-purpose cluster ID
        warehouse_id  : Optional: SQL warehouse ID for schema queries
    """

    def __init__(self, connector_id: str, config: dict[str, Any]) -> None:
        super().__init__(connector_id, config)
        self._host = config["host"]
        self._token = config["token"]
        self._catalog = config.get("catalog", "main")
        self._schema = config.get("schema", "default")
        self._workspace: Any = None

    def connect(self) -> None:
        try:
            from databricks.sdk import WorkspaceClient
            self._workspace = WorkspaceClient(host=self._host, token=self._token)
            # Validate by fetching current user
            self._workspace.current_user.me()
            self._connected = True
            logger.info("databricks.connected", host=self._host, catalog=self._catalog)
        except Exception as exc:
            raise ConnectionError(f"Databricks connection failed: {exc}") from exc

    def list_objects(self) -> list[dict[str, Any]]:
        try:
            tables = self._workspace.tables.list(
                catalog_name=self._catalog,
                schema_name=self._schema,
            )
            return [
                {
                    "id": t.full_name,
                    "name": t.name,
                    "qualified_name": t.full_name,
                    "kind": "table",
                    "table_type": t.table_type.value if t.table_type else "UNKNOWN",
                }
                for t in tables
            ]
        except Exception as exc:
            raise SchemaDiscoveryError(f"Databricks list_objects failed: {exc}") from exc

    def read_schema(self, object_id: str) -> GraphNode:
        try:
            table = self._workspace.tables.get(object_id)
            columns = []
            for col in (table.columns or []):
                dtype = _TYPE_MAP.get(
                    (col.type_name.value if col.type_name else "").lower(),
                    ColumnType.UNKNOWN
                )
                columns.append(ColumnDef(
                    name=col.name,
                    dtype=dtype,
                    nullable=col.nullable if col.nullable is not None else True,
                    primary_key=any(c.constraint_type == "PRIMARY_KEY" for c in (col.mask or [])),
                    description=col.comment,
                ))
            return GraphNode(
                id=GraphNode.make_id(self._connector_id, object_id),
                name=table.name,
                qualified_name=object_id,
                connector_id=self._connector_id,
                kind=NodeKind.TABLE,
                columns=columns,
                row_count=table.properties.get("numRows") if table.properties else None,
                size_bytes=table.properties.get("sizeInBytes") if table.properties else None,
                partition_keys=[c.name for c in (table.columns or []) if c.partition_index is not None],
                metadata={"catalog": self._catalog, "schema": self._schema},
            )
        except Exception as exc:
            raise SchemaDiscoveryError(f"Databricks read_schema failed for {object_id}: {exc}") from exc

    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        opts = options or {}
        try:
            df = spark.table(object_id)
            if opts.get("watermark_column") and opts.get("last_watermark"):
                df = df.filter(f"`{opts['watermark_column']}` > '{opts['last_watermark']}'")
            return df
        except Exception as exc:
            raise ReadError(f"Databricks read failed for {object_id}: {exc}") from exc

    def write(self, spark: Any, df: Any, target_id: str, mode: str = "overwrite", options: dict[str, Any] | None = None) -> None:
        opts = options or {}
        try:
            writer = df.write.format("delta").mode(mode)
            if opts.get("partition_by"):
                writer = writer.partitionBy(*opts["partition_by"])
            writer.saveAsTable(target_id)
        except Exception as exc:
            raise WriteError(f"Databricks write failed for {target_id}: {exc}") from exc

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
        try:
            table = self._workspace.tables.get(object_id)
            props = table.properties or {}
            return {
                "row_count": int(props.get("numRows", 0)),
                "size_bytes": int(props.get("sizeInBytes", 0)),
                "partition_count": int(props.get("numFiles", 1)),
            }
        except Exception:
            return {}
