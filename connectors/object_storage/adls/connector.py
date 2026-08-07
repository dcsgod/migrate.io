"""
connectors/object_storage/adls/connector.py
Azure Data Lake Storage Gen2 connector.
"""
from __future__ import annotations

from typing import Any

import structlog

from connectors import register
from connectors.base.capabilities import AuthMethod, ConnectorCapabilities, ReadShape, WriteShape
from connectors.base.connector import Connector, ConnectionError, ReadError, SchemaDiscoveryError, WriteError
from graph.models import ColumnDef, ColumnType, GraphNode, NodeKind

logger = structlog.get_logger(__name__)

_SPARK_TYPE_MAP = {
    "StringType": ColumnType.STRING, "IntegerType": ColumnType.INTEGER,
    "LongType": ColumnType.LONG, "DoubleType": ColumnType.DOUBLE,
    "FloatType": ColumnType.FLOAT, "BooleanType": ColumnType.BOOLEAN,
    "DateType": ColumnType.DATE, "TimestampType": ColumnType.TIMESTAMP,
    "DecimalType": ColumnType.DECIMAL,
}


@register("adls")
class ADLSConnector(Connector):
    """
    Azure Data Lake Storage Gen2 connector.

    Config keys:
        account_name     : storage account name
        container        : container (filesystem) name
        prefix           : optional path prefix
        tenant_id        : Azure AD tenant
        client_id        : service principal client ID
        client_secret    : service principal secret
        format           : default: parquet
    """

    def __init__(self, connector_id: str, config: dict[str, Any]) -> None:
        super().__init__(connector_id, config)
        self._account = config["account_name"]
        self._container = config["container"]
        self._prefix = config.get("prefix", "")
        self._format = config.get("format", "parquet")
        self._fs_client: Any = None

    def connect(self) -> None:
        try:
            from azure.identity import ClientSecretCredential
            from azure.storage.filedatalake import DataLakeServiceClient

            cred = ClientSecretCredential(
                tenant_id=self._config["tenant_id"],
                client_id=self._config["client_id"],
                client_secret=self._config["client_secret"],
            )
            service = DataLakeServiceClient(
                account_url=f"https://{self._account}.dfs.core.windows.net",
                credential=cred,
            )
            self._fs_client = service.get_file_system_client(self._container)
            # Validate
            list(self._fs_client.get_paths(path=self._prefix, max_results=1))
            self._connected = True
            logger.info("adls.connected", account=self._account, container=self._container)
        except Exception as exc:
            raise ConnectionError(f"ADLS connection failed: {exc}") from exc

    def list_objects(self) -> list[dict[str, Any]]:
        try:
            results = []
            for path in self._fs_client.get_paths(path=self._prefix, recursive=True):
                if not path.is_directory:
                    name = path.name
                    if name.endswith((".parquet", ".csv", ".json", ".avro")):
                        results.append({
                            "id": name,
                            "name": name.split("/")[-1],
                            "qualified_name": f"abfss://{self._container}@{self._account}.dfs.core.windows.net/{name}",
                            "kind": "file",
                            "size_bytes": path.content_length,
                        })
            return results
        except Exception as exc:
            raise SchemaDiscoveryError(f"ADLS list_objects failed: {exc}") from exc

    def read_schema(self, object_id: str) -> GraphNode:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            path = f"abfss://{self._container}@{self._account}.dfs.core.windows.net/{object_id}"
            df = spark.read.format(self._format).load(path)
            columns = [
                ColumnDef(name=f.name, dtype=_SPARK_TYPE_MAP.get(type(f.dataType).__name__, ColumnType.UNKNOWN), nullable=f.nullable)
                for f in df.schema.fields
            ]
            return GraphNode(
                id=GraphNode.make_id(self._connector_id, path),
                name=object_id.split("/")[-1],
                qualified_name=path,
                connector_id=self._connector_id,
                kind=NodeKind.FILE,
                columns=columns,
            )
        except Exception as exc:
            raise SchemaDiscoveryError(f"ADLS read_schema failed: {exc}") from exc

    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        opts = options or {}
        path = f"abfss://{self._container}@{self._account}.dfs.core.windows.net/{object_id}"
        try:
            return spark.read.format(self._format).load(path)
        except Exception as exc:
            raise ReadError(f"ADLS read failed: {exc}") from exc

    def write(self, spark: Any, df: Any, target_id: str, mode: str = "overwrite", options: dict[str, Any] | None = None) -> None:
        path = f"abfss://{self._container}@{self._account}.dfs.core.windows.net/{target_id}"
        try:
            df.write.format((options or {}).get("format", self._format)).mode(mode).save(path)
        except Exception as exc:
            raise WriteError(f"ADLS write failed: {exc}") from exc

    def capabilities(self) -> ConnectorCapabilities:
        return ConnectorCapabilities(
            read_shapes=frozenset([ReadShape.FILES]),
            write_shapes=frozenset([WriteShape.FILES]),
            supports_bulk=True,
            supports_incremental=True,
            auth_methods=frozenset([AuthMethod.SERVICE_ACCOUNT, AuthMethod.OAUTH2]),
        )
