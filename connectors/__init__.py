"""
connectors/__init__.py
Central registry for all connector types.
"""
from __future__ import annotations

from typing import Any

_REGISTRY: dict[str, type] = {}


def register(connector_type: str):
    """Decorator that registers a Connector subclass under a type key."""
    def decorator(cls):
        _REGISTRY[connector_type] = cls
        return cls
    return decorator


def build_connector(connector_type: str, connector_id: str, config: dict[str, Any]):
    """Instantiate a connector by type name."""
    if connector_type not in _REGISTRY:
        # Try auto-importing
        _auto_import(connector_type)
    if connector_type not in _REGISTRY:
        raise ValueError(
            f"Unknown connector type: {connector_type!r}. "
            f"Available: {sorted(_REGISTRY.keys())}"
        )
    return _REGISTRY[connector_type](connector_id, config)


def list_connector_types() -> list[str]:
    """Return all registered connector type keys."""
    return sorted(_REGISTRY.keys())


def _auto_import(connector_type: str) -> None:
    """
    Lazy import known connector modules to trigger their @register decorators.
    """
    import importlib

    _KNOWN_MODULES = {
        # Mocks
        "mock_object_storage": "connectors.mock.object_storage",
        "mock_warehouse": "connectors.mock.warehouse",
        "mock_rdbms": "connectors.mock.rdbms",
        "mock_erp": "connectors.mock.erp",
        "mock_streaming": "connectors.mock.streaming",
        # Object storage
        "s3": "connectors.object_storage.s3.connector",
        "adls": "connectors.object_storage.adls.connector",
        # Warehouse
        "databricks": "connectors.warehouse.databricks.connector",
        # RDBMS
        "postgres": "connectors.rdbms.postgres.connector",
        # ERP
        "sap_ecc": "connectors.erp.sap_ecc.connector",
    }

    module_path = _KNOWN_MODULES.get(connector_type)
    if module_path:
        try:
            importlib.import_module(module_path)
        except ImportError:
            pass


# ── Pre-load mocks (always available) ─────────────────────────
def _preload_mocks():
    import importlib
    for mod in [
        "connectors.mock.object_storage",
        "connectors.mock.warehouse",
        "connectors.mock.rdbms",
        "connectors.mock.erp",
        "connectors.mock.streaming",
    ]:
        try:
            importlib.import_module(mod)
        except ImportError:
            pass


_preload_mocks()
