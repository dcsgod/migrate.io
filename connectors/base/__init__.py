"""
connectors/base/__init__.py
"""
from connectors.base.capabilities import ConnectorCapabilities, ReadShape, WriteShape, AuthMethod
from connectors.base.connector import (
    Connector,
    ConnectorError,
    ConnectionError,
    SchemaDiscoveryError,
    ReadError,
    WriteError,
)
from connectors.base.introspector import SchemaIntrospector

__all__ = [
    "Connector",
    "ConnectorCapabilities",
    "ConnectorError",
    "ConnectionError",
    "SchemaDiscoveryError",
    "ReadError",
    "WriteError",
    "ReadShape",
    "WriteShape",
    "AuthMethod",
    "SchemaIntrospector",
]
