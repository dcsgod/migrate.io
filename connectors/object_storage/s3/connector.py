"""
connectors/object_storage/s3/connector.py
Amazon S3 connector — reads Parquet/CSV/JSON/Avro via boto3 + Spark.
"""
from __future__ import annotations

from typing import Any

import structlog

from connectors import register
from connectors.base.capabilities import AuthMethod, ConnectorCapabilities, ReadShape, WriteShape
from connectors.base.connector import (
    Connector,
    ConnectionError,
    ReadError,
    SchemaDiscoveryError,
    WriteError,
)
from graph.models import ColumnDef, ColumnType, GraphNode, NodeKind

logger = structlog.get_logger(__name__)

_SPARK_TYPE_MAP = {
    "StringType": ColumnType.STRING,
    "IntegerType": ColumnType.INTEGER,
    "LongType": ColumnType.LONG,
    "FloatType": ColumnType.FLOAT,
    "DoubleType": ColumnType.DOUBLE,
    "BooleanType": ColumnType.BOOLEAN,
    "DateType": ColumnType.DATE,
    "TimestampType": ColumnType.TIMESTAMP,
    "BinaryType": ColumnType.BINARY,
    "DecimalType": ColumnType.DECIMAL,
}


@register("s3")
class S3Connector(Connector):
    """
    Amazon S3 connector.

    Config keys:
        bucket          : S3 bucket name
        prefix          : optional key prefix to scope discovery
        region          : AWS region (default: us-east-1)
        access_key_id   : optional (uses IAM role if absent)
        secret_access_key: optional
        endpoint_url    : optional (set for MinIO / LocalStack)
        format          : default file format: parquet | csv | json | avro
        infer_schema    : bool (default True) — schema inference on read
    """

    def __init__(self, connector_id: str, config: dict[str, Any]) -> None:
        super().__init__(connector_id, config)
        self._s3: Any = None   # boto3 S3 client
        self._bucket: str = config["bucket"]
        self._prefix: str = config.get("prefix", "")
        self._region: str = config.get("region", "us-east-1")
        self._format: str = config.get("format", "parquet")

    def connect(self) -> None:
        try:
            import boto3
            session_kwargs: dict[str, Any] = {"region_name": self._region}
            if self._config.get("access_key_id"):
                session_kwargs["aws_access_key_id"] = self._config["access_key_id"]
                session_kwargs["aws_secret_access_key"] = self._config["secret_access_key"]

            client_kwargs: dict[str, Any] = {}
            if self._config.get("endpoint_url"):
                client_kwargs["endpoint_url"] = self._config["endpoint_url"]

            session = boto3.Session(**session_kwargs)
            self._s3 = session.client("s3", **client_kwargs)
            # Validate by checking bucket access
            self._s3.head_bucket(Bucket=self._bucket)
            self._connected = True
            logger.info("s3.connected", bucket=self._bucket, region=self._region)
        except Exception as exc:
            raise ConnectionError(f"S3 connection failed: {exc}") from exc

    def list_objects(self) -> list[dict[str, Any]]:
        if not self._connected:
            raise ConnectionError("Not connected. Call connect() first.")
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            results = []
            for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix, Delimiter="/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith((".parquet", ".csv", ".json", ".avro", ".orc")):
                        results.append({
                            "id": key,
                            "name": key.split("/")[-1],
                            "qualified_name": f"s3://{self._bucket}/{key}",
                            "kind": "file",
                            "size_bytes": obj["Size"],
                            "last_modified": obj["LastModified"].isoformat(),
                        })
                # List common prefixes as "folders"
                for prefix in page.get("CommonPrefixes", []):
                    p = prefix["Prefix"]
                    results.append({
                        "id": p,
                        "name": p.rstrip("/").split("/")[-1],
                        "qualified_name": f"s3://{self._bucket}/{p}",
                        "kind": "directory",
                    })
            return results
        except Exception as exc:
            raise SchemaDiscoveryError(f"S3 list_objects failed: {exc}") from exc

    def read_schema(self, object_id: str) -> GraphNode:
        """Infer schema by reading the first file with Spark schema inference."""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark is None:
                raise SchemaDiscoveryError("No active Spark session for schema inference.")
            path = f"s3://{self._bucket}/{object_id}"
            df = spark.read.format(self._format).load(path)
            columns = [
                ColumnDef(
                    name=f.name,
                    dtype=_SPARK_TYPE_MAP.get(type(f.dataType).__name__, ColumnType.UNKNOWN),
                    nullable=f.nullable,
                )
                for f in df.schema.fields
            ]
            node_id = GraphNode.make_id(self._connector_id, path)
            return GraphNode(
                id=node_id,
                name=object_id.split("/")[-1],
                qualified_name=path,
                connector_id=self._connector_id,
                kind=NodeKind.FILE,
                columns=columns,
                metadata={"format": self._format, "path": path},
            )
        except Exception as exc:
            raise SchemaDiscoveryError(f"S3 read_schema failed for {object_id}: {exc}") from exc

    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        try:
            opts = options or {}
            path = f"s3://{self._bucket}/{object_id}"
            reader = spark.read.format(self._format)
            if self._format == "csv":
                reader = reader.option("header", opts.get("header", "true"))
                reader = reader.option("inferSchema", opts.get("inferSchema", "true"))
            if opts.get("schema"):
                reader = reader.schema(opts["schema"])
            df = reader.load(path)
            if opts.get("watermark_column") and opts.get("last_watermark") is not None:
                df = df.filter(
                    f"{opts['watermark_column']} > '{opts['last_watermark']}'"
                )
            return df
        except Exception as exc:
            raise ReadError(f"S3 read failed for {object_id}: {exc}") from exc

    def write(self, spark: Any, df: Any, target_id: str, mode: str = "overwrite", options: dict[str, Any] | None = None) -> None:
        try:
            opts = options or {}
            path = f"s3://{self._bucket}/{target_id}"
            writer = df.write.format(opts.get("format", self._format)).mode(mode)
            if opts.get("partition_by"):
                writer = writer.partitionBy(*opts["partition_by"])
            writer.save(path)
        except Exception as exc:
            raise WriteError(f"S3 write failed for {target_id}: {exc}") from exc

    def capabilities(self) -> ConnectorCapabilities:
        return ConnectorCapabilities(
            read_shapes=frozenset([ReadShape.FILES]),
            write_shapes=frozenset([WriteShape.FILES]),
            supports_bulk=True,
            supports_cdc=False,
            supports_incremental=True,
            supports_schema_discovery=True,
            auth_methods=frozenset([AuthMethod.IAM_ROLE, AuthMethod.API_KEY]),
        )

    def get_source_stats(self, object_id: str) -> dict[str, Any]:
        try:
            response = self._s3.head_object(Bucket=self._bucket, Key=object_id)
            return {
                "size_bytes": response.get("ContentLength", 0),
                "row_count": None,   # Unknown without full read
                "partition_count": 1,
            }
        except Exception:
            return {}
