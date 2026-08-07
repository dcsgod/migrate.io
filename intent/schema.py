"""
intent/schema.py
Pydantic models for Intent JSON — the output of NL parsing and grounding.
"""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class OperationType(str, Enum):
    COPY = "copy"           # straight move, no transform
    TRANSFORM = "transform" # copy + inline transforms
    MERGE = "merge"         # SCD / upsert into existing dest
    DEDUPE = "dedupe"       # deduplicate source before writing
    INCREMENTAL = "incremental"  # watermark-based incremental load


class FilterOp(str, Enum):
    EQ = "eq"; NEQ = "neq"; GT = "gt"; GTE = "gte"
    LT = "lt"; LTE = "lte"; IN = "in"; NOT_IN = "not_in"
    IS_NULL = "is_null"; IS_NOT_NULL = "is_not_null"; LIKE = "like"


class FilterClause(BaseModel):
    column: str
    op: FilterOp
    value: Any = None


class JoinSpec(BaseModel):
    left_table: str         # entity name (unresolved)
    right_table: str
    left_key: str
    right_key: str
    join_type: str = "inner"   # inner | left | right | full


class TransformSpec(BaseModel):
    op: str                 # join | dedupe | pivot | scd_merge | type_cast | mask | custom_expr
    params: dict[str, Any] = Field(default_factory=dict)


class ColumnMapping(BaseModel):
    source_column: str
    target_column: str
    transform: str | None = None   # e.g. "UPPER({{source}})", "CAST({{source}} AS DATE)"


class IncrementalSpec(BaseModel):
    watermark_column: str
    last_watermark: Any = None


# ─────────────────────────────────────────────────────────────
# Raw intent — straight from LLM, entity names unresolved
# ─────────────────────────────────────────────────────────────

class IntentJSON(BaseModel):
    """
    Raw output of the LLM parser.
    All entity names (table names, column names) are as the user typed them —
    NOT yet resolved to GraphNode IDs.
    """
    operation: OperationType
    source_tables: list[str]                          # e.g. ["orders", "customers"]
    target_table: str
    filters: list[FilterClause] = Field(default_factory=list)
    joins: list[JoinSpec] = Field(default_factory=list)
    transforms: list[TransformSpec] = Field(default_factory=list)
    column_mappings: list[ColumnMapping] = Field(default_factory=list)
    output_columns: list[str] = Field(default_factory=list)   # empty = all
    incremental: IncrementalSpec | None = None
    dry_run: bool = False
    user_nl_command: str = ""        # original command, for tracing
    llm_raw_response: str = ""       # raw LLM output, for debug


# ─────────────────────────────────────────────────────────────
# Grounded intent — entity names resolved to graph node IDs
# ─────────────────────────────────────────────────────────────

class ResolvedTable(BaseModel):
    entity_name: str        # as the user typed
    node_id: str            # resolved GraphNode ID
    node_qualified_name: str
    confidence: float = 1.0
    needs_user_confirmation: bool = False


class ResolvedJoin(BaseModel):
    join_spec: JoinSpec
    left_node_id: str
    right_node_id: str
    resolved_left_key: str
    resolved_right_key: str
    confidence: float = 1.0
    reasoning: str = ""
    needs_user_confirmation: bool = False


class GroundedIntent(BaseModel):
    """
    Intent with all entity names resolved to graph node IDs.
    Low-confidence resolutions are flagged for user confirmation.
    """
    original_intent: IntentJSON
    operation: OperationType
    source_tables: list[ResolvedTable]
    target_table: ResolvedTable
    filters: list[FilterClause] = Field(default_factory=list)
    resolved_joins: list[ResolvedJoin] = Field(default_factory=list)
    transforms: list[TransformSpec] = Field(default_factory=list)
    column_mappings: list[ColumnMapping] = Field(default_factory=list)
    output_columns: list[str] = Field(default_factory=list)
    incremental: IncrementalSpec | None = None
    dry_run: bool = False
    unresolved_entities: list[str] = Field(default_factory=list)   # names we couldn't resolve

    @property
    def needs_user_confirmation(self) -> bool:
        return (
            any(t.needs_user_confirmation for t in self.source_tables)
            or self.target_table.needs_user_confirmation
            or any(j.needs_user_confirmation for j in self.resolved_joins)
        )

    @property
    def all_source_node_ids(self) -> list[str]:
        return [t.node_id for t in self.source_tables]
