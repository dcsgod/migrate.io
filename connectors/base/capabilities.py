"""
connectors/base/capabilities.py
Declares what a connector can and cannot do.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ReadShape(str, Enum):
    """How data surfaces from this connector."""
    FILES = "files"        # parquet / csv / json / avro blobs
    TABLES = "tables"      # SQL-queryable tables
    TOPICS = "topics"      # streaming topics / queues
    OBJECTS = "objects"    # generic business objects (ERP)


class WriteShape(str, Enum):
    FILES = "files"
    TABLES = "tables"
    TOPICS = "topics"


class AuthMethod(str, Enum):
    API_KEY = "api_key"
    IAM_ROLE = "iam_role"
    SERVICE_ACCOUNT = "service_account"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    SAML = "saml"
    RFC = "rfc"             # SAP RFC


@dataclass(frozen=True)
class ConnectorCapabilities:
    """Immutable capability declaration for a connector."""

    read_shapes: frozenset[ReadShape] = field(default_factory=frozenset)
    write_shapes: frozenset[WriteShape] = field(default_factory=frozenset)

    # Data movement capabilities
    supports_bulk: bool = True          # Full-load batch reads
    supports_cdc: bool = False          # Change-data-capture / watermark reads
    supports_streaming: bool = False    # Structured Streaming source/sink
    supports_incremental: bool = False  # Watermark / high-watermark reads

    # Schema capabilities
    supports_schema_discovery: bool = True   # Can list objects + infer schema
    supports_schema_evolution: bool = False  # Can detect / handle drift

    # Transaction capabilities
    supports_atomic_write: bool = False  # Atomic swap / MERGE
    supports_time_travel: bool = False   # Delta / Iceberg time travel

    # Auth methods this connector accepts
    auth_methods: frozenset[AuthMethod] = field(default_factory=frozenset)

    # Estimated max throughput hint (rows/sec), 0 = unknown
    max_throughput_rows_per_sec: int = 0

    def can_read(self) -> bool:
        return bool(self.read_shapes)

    def can_write(self) -> bool:
        return bool(self.write_shapes)

    def __repr__(self) -> str:
        flags = []
        if self.supports_cdc:
            flags.append("CDC")
        if self.supports_streaming:
            flags.append("streaming")
        if self.supports_incremental:
            flags.append("incremental")
        if self.supports_time_travel:
            flags.append("time-travel")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        return (
            f"ConnectorCapabilities("
            f"read={[s.value for s in self.read_shapes]}, "
            f"write={[s.value for s in self.write_shapes]}"
            f"{flag_str})"
        )
