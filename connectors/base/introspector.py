"""
connectors/base/introspector.py
Abstract SchemaIntrospector — crawls a connector and builds GraphNodes.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from connectors.base.connector import Connector
    from graph.models import GraphNode


class SchemaIntrospector(ABC):
    """
    Walks a connector's object catalogue and produces GraphNodes.
    Each connector (or connector class) provides its own introspector
    that knows how to translate native schema metadata into the
    canonical GraphNode / ColumnDef format.
    """

    def __init__(self, connector: "Connector") -> None:
        self._connector = connector

    @abstractmethod
    def crawl(self, max_objects: int = 500) -> list["GraphNode"]:
        """
        Discover all objects in the connected source/destination and
        return a list of GraphNodes with columns, types, and stats.

        Args:
            max_objects: Safety cap to avoid crawling enormous catalogues.
        """

    @abstractmethod
    def refresh_node(self, node: "GraphNode") -> "GraphNode":
        """
        Re-read schema for a single node and return an updated copy.
        Used for drift detection — does not rebuild the whole graph.
        """

    # ── Optional: sample values for inference ─────────────────

    def sample_column_values(
        self,
        spark: Any,
        object_id: str,
        column_name: str,
        n: int = 100,
    ) -> list[Any]:
        """
        Return up to `n` distinct values from `column_name` in `object_id`.
        Used by the edge inferrer for value-overlap scoring.
        Default: reads via connector.read() and samples in Spark.
        Override for more efficient connector-native sampling.
        """
        df = self._connector.read(spark, object_id)
        return (
            df.select(column_name)
            .dropna()
            .distinct()
            .limit(n)
            .rdd.flatMap(lambda r: r)
            .collect()
        )
