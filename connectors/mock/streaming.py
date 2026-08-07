"""
connectors/mock/streaming.py
Mock streaming connector — simulates a Kafka/Kinesis/Event Hubs topic.
"""
from __future__ import annotations

from typing import Any

from connectors import register
from connectors.base.capabilities import AuthMethod, ConnectorCapabilities, ReadShape, WriteShape
from connectors.base.connector import Connector
from graph.models import ColumnDef, ColumnType, GraphNode, NodeKind


@register("mock_streaming")
class MockStreamingConnector(Connector):
    """Simulates a Kafka-style streaming source with Avro-encoded messages."""

    _MOCK_TOPICS = [
        {
            "id": "order_events",
            "name": "order_events",
            "qualified_name": "mock-kafka.order_events",
            "columns": [
                ("event_id", ColumnType.STRING, False),
                ("event_type", ColumnType.STRING, False),   # ORDER_PLACED, SHIPPED, DELIVERED
                ("order_id", ColumnType.LONG, False),
                ("customer_id", ColumnType.LONG, True),
                ("product_id", ColumnType.STRING, True),
                ("quantity", ColumnType.INTEGER, True),
                ("amount", ColumnType.DOUBLE, True),
                ("currency", ColumnType.STRING, True),
                ("timestamp", ColumnType.TIMESTAMP, False),
                ("partition_key", ColumnType.STRING, True),
            ],
            "messages_per_sec": 5_000,
            "retention_hours": 168,
        },
        {
            "id": "user_activity",
            "name": "user_activity",
            "qualified_name": "mock-kafka.user_activity",
            "columns": [
                ("session_id", ColumnType.STRING, False),
                ("user_id", ColumnType.LONG, True),
                ("event_name", ColumnType.STRING, False),
                ("page", ColumnType.STRING, True),
                ("referrer", ColumnType.STRING, True),
                ("device_type", ColumnType.STRING, True),
                ("ip_address", ColumnType.STRING, True),
                ("timestamp", ColumnType.TIMESTAMP, False),
            ],
            "messages_per_sec": 50_000,
            "retention_hours": 72,
        },
    ]

    def connect(self) -> None:
        self._connected = True

    def list_objects(self) -> list[dict[str, Any]]:
        return [
            {"id": t["id"], "name": t["name"], "qualified_name": t["qualified_name"], "kind": "topic"}
            for t in self._MOCK_TOPICS
        ]

    def read_schema(self, object_id: str) -> GraphNode:
        topic = next((t for t in self._MOCK_TOPICS if t["id"] == object_id), None)
        if topic is None:
            from connectors.base.connector import SchemaDiscoveryError
            raise SchemaDiscoveryError(f"Mock topic not found: {object_id}")
        columns = [ColumnDef(name=col[0], dtype=col[1], nullable=col[2]) for col in topic["columns"]]
        node_id = GraphNode.make_id(self._connector_id, topic["qualified_name"])
        return GraphNode(
            id=node_id,
            name=topic["name"],
            qualified_name=topic["qualified_name"],
            connector_id=self._connector_id,
            kind=NodeKind.TOPIC,
            columns=columns,
            metadata={
                "messages_per_sec": topic["messages_per_sec"],
                "retention_hours": topic["retention_hours"],
            },
        )

    def read(self, spark: Any, object_id: str, options: dict[str, Any] | None = None) -> Any:
        raise NotImplementedError(
            "Use read_stream() for streaming connectors via the streaming compiler."
        )

    def read_stream(self, spark: Any, object_id: str) -> Any:
        """Return a Spark Structured Streaming DataFrame from a mock topic."""
        topic = next((t for t in self._MOCK_TOPICS if t["id"] == object_id), None)
        if topic is None:
            from connectors.base.connector import ReadError
            raise ReadError(f"Mock topic not found: {object_id}")
        # In real impl: spark.readStream.format("kafka")...
        raise NotImplementedError("Streaming mode is not yet wired in mock; use real Kafka connector.")

    def write(self, spark: Any, df: Any, target_id: str, mode: str = "append", options: dict[str, Any] | None = None) -> None:
        pass  # no-op mock

    def capabilities(self) -> ConnectorCapabilities:
        return ConnectorCapabilities(
            read_shapes=frozenset([ReadShape.TOPICS]),
            write_shapes=frozenset([WriteShape.TOPICS]),
            supports_bulk=False,
            supports_streaming=True,
            supports_cdc=False,
            auth_methods=frozenset([AuthMethod.API_KEY, AuthMethod.SAML]),
        )
