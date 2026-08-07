"""
connectors/rdbms/postgres/connector.py
PostgreSQL connector via psycopg2 + SQLAlchemy + JDBC.
"""
from __future__ import annotations

from typing import Any

import structlog

from connectors import register
from connectors.base.capabilities import AuthMethod, ConnectorCapabilities, ReadShape, WriteShape
from connectors.base.connector import Connector, ConnectionError, ReadError, SchemaDiscoveryError, WriteError
from graph.models import ColumnDef, ColumnType, GraphNode, NodeKind

logger = structlog.get_logger(__name__)

_PG_TYPE_MAP = {
    "integer": ColumnType.INTEGER, "int4": ColumnType.INTEGER, "int2": ColumnType.INTEGER,
    "bigint": ColumnType.LONG, "int8": ColumnType.LONG,
    "real": ColumnType.FLOAT, "float4": ColumnType.FLOAT,
    "double precision": ColumnType.DOUBLE, "float8": ColumnType.DOUBLE,
    "numeric": ColumnType.DECIMAL, "decimal": ColumnType.DECIMAL,
    "text": ColumnType.STRING, "varchar": ColumnType.STRING,
    "character varying": ColumnType.STRING, "char": ColumnType.STRING,
    "boolean": ColumnType.BOOLEAN, "bool": ColumnType.BOOLEAN,
    "date": ColumnType.DATE,
    "timestamp": ColumnType.TIMESTAMP, "timestamptz": ColumnType.TIMESTAMP,
    "bytea": ColumnType.BINARY,
}


@register("postgres")
class PostgresConnector(Connector):
    """
    PostgreSQL connector.

    Config keys:
        host, port, database, user, password, schema (default: public)
        jdbc_url : optional override for Spark JDBC reads
    """

    def __init__(self, connector_id: str, config: dict[str, Any]) -> None:
        super().__init__(connector_id, config)
        self._schema = config.get("schema", "public")
        self._conn: Any = None

    def _jdbc_url(self) -> str:
        if self._config.get("jdbc_url"):
            return self._config["jdbc_url"]
        host = self._config.get("host", "localhost")
        port = self._config.get("port", 5432)
        db = self._config.get("database", "")
        return f"jdbc:postgresql://{host}:{port}/{db}"

    def connect(self) -> None:
        try:
            import psycopg2
            self._conn = psycopg2.connect(
                host=self._config.get("host", "localhost"),
                port=self._config.get("port", 5432),
                dbname=self._config.get("database", ""),
                user=self._config.get("user", ""),
                password=self._config.get("password", ""),
            )
            self._connected = True
            logger.info("postgres.connected", host=self._config.get("host"))
        except Exception as exc:
            raise ConnectionError(f"Postgres connection failed: {exc}") from exc

    def list_objects(self) -> list[dict[str, Any]]:
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = %s
                ORDER BY table_name
                """,
                (self._schema,),
            )
            rows = cursor.fetchall()
            return [
                {
                    "id": f"{row[0]}.{row[1]}",
                    "name": row[1],
                    "qualified_name": f"{self._config.get('database', '')}.{row[0]}.{row[1]}",
                    "kind": "table" if row[2] == "BASE TABLE" else "view",
                }
                for row in rows
            ]
        except Exception as exc:
            raise SchemaDiscoveryError(f"Postgres list_objects failed: {exc}") from exc

    def read_schema(self, object_id: str) -> GraphNode:
        schema_name, table_name = object_id.split(".", 1) if "." in object_id else (self._schema, object_id)
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    CASE WHEN pk.column_name IS NOT NULL THEN TRUE ELSE FALSE END as is_pk,
                    ccu.table_name AS fk_table,
                    ccu.column_name AS fk_column
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT ku.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage ku
                        ON tc.constraint_name = ku.constraint_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND ku.table_schema = %s AND ku.table_name = %s
                ) pk ON pk.column_name = c.column_name
                LEFT JOIN information_schema.referential_constraints rc
                    ON rc.constraint_schema = c.table_schema
                LEFT JOIN information_schema.constraint_column_usage ccu
                    ON rc.unique_constraint_name = ccu.constraint_name
                WHERE c.table_schema = %s AND c.table_name = %s
                ORDER BY c.ordinal_position
                """,
                (schema_name, table_name, schema_name, table_name),
            )
            rows = cursor.fetchall()
            columns = [
                ColumnDef(
                    name=row[0],
                    dtype=_PG_TYPE_MAP.get(row[1].lower(), ColumnType.UNKNOWN),
                    nullable=row[2] == "YES",
                    primary_key=bool(row[3]),
                    foreign_key=f"{row[4]}.{row[5]}" if row[4] and row[5] else None,
                )
                for row in rows
            ]
            # Row count estimate
            cursor.execute(
                "SELECT reltuples::bigint FROM pg_class JOIN pg_namespace ON relnamespace=pg_namespace.oid WHERE nspname=%s AND relname=%s",
                (schema_name, table_name),
            )
            rc_row = cursor.fetchone()
            row_count = rc_row[0] if rc_row else None

            qname = f"{self._config.get('database', '')}.{schema_name}.{table_name}"
            return GraphNode(
                id=GraphNode.make_id(self._connector_id, qname),
                name=table_name,
                qualified_name=qname,
                connector_id=self._connector_id,
                kind=NodeKind.TABLE,
                columns=columns,
                row_count=row_count,
            )
        except Exception as exc:
            raise SchemaDiscoveryError(f"Postgres read_schema failed for {object_id}: {exc}") from exc

    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        opts = options or {}
        schema_name, table_name = object_id.split(".", 1) if "." in object_id else (self._schema, object_id)
        query = opts.get("query", f'(SELECT * FROM "{schema_name}"."{table_name}") AS t')
        try:
            return (
                spark.read.format("jdbc")
                .option("url", self._jdbc_url())
                .option("dbtable", query)
                .option("user", self._config.get("user", ""))
                .option("password", self._config.get("password", ""))
                .option("driver", "org.postgresql.Driver")
                .option("fetchsize", opts.get("fetchsize", 10000))
                .load()
            )
        except Exception as exc:
            raise ReadError(f"Postgres read failed for {object_id}: {exc}") from exc

    def write(self, spark: Any, df: Any, target_id: str, mode: str = "overwrite", options: dict[str, Any] | None = None) -> None:
        schema_name, table_name = target_id.split(".", 1) if "." in target_id else (self._schema, target_id)
        try:
            (
                df.write.format("jdbc")
                .option("url", self._jdbc_url())
                .option("dbtable", f'"{schema_name}"."{table_name}"')
                .option("user", self._config.get("user", ""))
                .option("password", self._config.get("password", ""))
                .option("driver", "org.postgresql.Driver")
                .mode(mode)
                .save()
            )
        except Exception as exc:
            raise WriteError(f"Postgres write failed for {target_id}: {exc}") from exc

    def close(self) -> None:
        if self._conn:
            self._conn.close()
        super().close()

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
