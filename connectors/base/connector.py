"""
connectors/base/connector.py
Abstract Connector interface — every connector must implement this.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from connectors.base.capabilities import ConnectorCapabilities

if TYPE_CHECKING:
    from graph.models import GraphNode


class ConnectorError(Exception):
    """Base error raised by connector operations."""


class ConnectionError(ConnectorError):  # noqa: A001
    """Raised when the connector cannot establish a connection."""


class SchemaDiscoveryError(ConnectorError):
    """Raised when schema/object discovery fails."""


class ReadError(ConnectorError):
    """Raised when a read operation fails."""


class WriteError(ConnectorError):
    """Raised when a write operation fails."""


class Connector(ABC):
    """
    Universal connector interface.

    Every source or destination must implement this class.
    Spark is the execution engine — connectors own auth, discovery,
    and capability declaration; they do NOT reinvent I/O.

    Lifecycle:
        connector = MyConnector(config)
        connector.connect()           # authenticate + health-check
        objects  = connector.list_objects()
        schema   = connector.read_schema(object_id)
        df       = connector.read(spark, object_id)
        connector.write(spark, df, target_id)
        connector.close()
    """

    def __init__(self, connector_id: str, config: dict[str, Any]) -> None:
        self._connector_id = connector_id
        self._config = config
        self._connected: bool = False

    # ── Identity ──────────────────────────────────────────────

    @property
    def connector_id(self) -> str:
        return self._connector_id

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Lifecycle ─────────────────────────────────────────────

    @abstractmethod
    def connect(self) -> None:
        """
        Authenticate and establish a connection.
        Must set self._connected = True on success.
        Raise ConnectionError on failure.
        """

    def close(self) -> None:
        """Release any held resources. Override if needed."""
        self._connected = False

    def __enter__(self) -> "Connector":
        self.connect()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ── Discovery ─────────────────────────────────────────────

    @abstractmethod
    def list_objects(self) -> list[dict[str, Any]]:
        """
        List all available objects (tables, files, topics).
        Returns a list of dicts with at minimum:
            { "id": str, "name": str, "qualified_name": str, "kind": str }
        """

    @abstractmethod
    def read_schema(self, object_id: str) -> "GraphNode":
        """
        Return a fully populated GraphNode for the given object_id,
        including column definitions and available stats.
        """

    # ── Data I/O ──────────────────────────────────────────────

    @abstractmethod
    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        """
        Read the object as a Spark DataFrame.
        `options` may include: filter predicates, watermark column,
        high-watermark value, partition hints.
        Returns: pyspark.sql.DataFrame
        """

    @abstractmethod
    def write(
        self,
        spark: Any,
        df: Any,
        target_id: str,
        mode: str = "overwrite",
        options: dict[str, Any] | None = None,
    ) -> None:
        """
        Write a Spark DataFrame to the target object.
        `mode`: overwrite | append | merge | error
        Raise WriteError on failure.
        """

    # ── Capabilities ──────────────────────────────────────────

    @abstractmethod
    def capabilities(self) -> ConnectorCapabilities:
        """
        Return the static capability declaration for this connector.
        Called once during registration; result is cached.
        """

    # ── Health check ─────────────────────────────────────────

    def health_check(self) -> dict[str, Any]:
        """
        Returns a dict with at least { "ok": bool, "message": str }.
        Override for connector-specific health checks.
        """
        return {"ok": self._connected, "message": "connected" if self._connected else "not connected"}

    # ── Optional: incremental / CDC ───────────────────────────

    def read_incremental(
        self,
        spark: Any,
        object_id: str,
        watermark_column: str,
        last_watermark: Any,
    ) -> Any:
        """
        Read only rows newer than `last_watermark`.
        Default: raises NotImplementedError; connectors with
        supports_incremental=True must override.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support incremental reads. "
            "Check capabilities().supports_incremental before calling."
        )

    def read_cdc(self, spark: Any, object_id: str, since: Any) -> Any:
        """
        Return a CDC stream (insert/update/delete rows).
        Default: raises NotImplementedError.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support CDC reads."
        )

    # ── Stats ─────────────────────────────────────────────────

    def get_source_stats(self, object_id: str) -> dict[str, Any]:
        """
        Return source statistics used by the cost estimator:
            { "row_count": int, "size_bytes": int, "partition_count": int }
        Default: returns empty dict (unknown stats).
        """
        return {}

    def __repr__(self) -> str:
        status = "connected" if self._connected else "disconnected"
        return f"{self.__class__.__name__}(id={self._connector_id!r}, status={status})"
