"""
dag/validator.py
Validates a DAG before compilation — type checks, cycles, low-confidence edges.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import networkx as nx
import structlog

from dag.nodes import DAG, DAGNode, FilterNode, JoinNode, ReadNode, WriteNode

logger = structlog.get_logger(__name__)


class ValidationSeverity(str, Enum):
    ERROR = "error"     # Blocks compilation
    WARNING = "warning" # User should review
    INFO = "info"       # Informational


@dataclass
class ValidationIssue:
    node_id: str | None
    severity: ValidationSeverity
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "severity": self.severity.value,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class ValidationResult:
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(i.severity == ValidationSeverity.ERROR for i in self.issues)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == ValidationSeverity.ERROR]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == ValidationSeverity.WARNING]

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": not self.has_errors,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "issues": [i.to_dict() for i in self.issues],
        }

    def add(self, issue: ValidationIssue) -> None:
        self.issues.append(issue)


class DAGValidator:
    """
    Runs a battery of validation checks on a DAG.

    Checks:
    - No cycles (required for Spark)
    - At least one ReadNode
    - At least one WriteNode
    - JoinNode has both parents connected
    - FilterNode has valid parent
    - JoinNode with empty join keys → warning
    - Disconnected nodes → warning
    - Write mode 'merge' without merge key → warning
    """

    def validate(self, dag: DAG) -> ValidationResult:
        result = ValidationResult()

        self._check_empty(dag, result)
        self._check_cycles(dag, result)
        self._check_read_nodes(dag, result)
        self._check_write_nodes(dag, result)
        self._check_join_nodes(dag, result)
        self._check_filter_nodes(dag, result)
        self._check_disconnected(dag, result)

        logger.info(
            "dag_validator.done",
            errors=len(result.errors),
            warnings=len(result.warnings),
        )
        return result

    def _check_empty(self, dag: DAG, result: ValidationResult) -> None:
        if not dag.nodes:
            result.add(ValidationIssue(
                node_id=None,
                severity=ValidationSeverity.ERROR,
                code="EMPTY_DAG",
                message="DAG has no nodes.",
            ))

    def _check_cycles(self, dag: DAG, result: ValidationResult) -> None:
        g = nx.DiGraph()
        for nid in dag.nodes:
            g.add_node(nid)
        for p, c in dag.edges:
            g.add_edge(p, c)
        if not nx.is_directed_acyclic_graph(g):
            cycles = list(nx.find_cycle(g))
            result.add(ValidationIssue(
                node_id=None,
                severity=ValidationSeverity.ERROR,
                code="CYCLE_DETECTED",
                message="DAG contains a cycle — Spark cannot execute cyclic graphs.",
                details={"cycle": [(a, b) for a, b in cycles]},
            ))

    def _check_read_nodes(self, dag: DAG, result: ValidationResult) -> None:
        reads = dag.read_nodes()
        if not reads:
            result.add(ValidationIssue(
                node_id=None,
                severity=ValidationSeverity.ERROR,
                code="NO_READ_NODE",
                message="DAG has no ReadNode — nothing to read from.",
            ))
        for rn in reads:
            if not rn.object_id:
                result.add(ValidationIssue(
                    node_id=rn.id,
                    severity=ValidationSeverity.ERROR,
                    code="READ_MISSING_OBJECT_ID",
                    message=f"ReadNode {rn.id} has no object_id.",
                ))

    def _check_write_nodes(self, dag: DAG, result: ValidationResult) -> None:
        writes = dag.write_nodes()
        if not writes:
            result.add(ValidationIssue(
                node_id=None,
                severity=ValidationSeverity.WARNING,
                code="NO_WRITE_NODE",
                message="DAG has no WriteNode — data will be read and transformed but not committed.",
            ))
        for wn in writes:
            if wn.mode == "merge" and not wn.write_options.get("merge_keys"):
                result.add(ValidationIssue(
                    node_id=wn.id,
                    severity=ValidationSeverity.WARNING,
                    code="MERGE_MISSING_KEYS",
                    message=f"WriteNode {wn.id} uses mode=merge but no merge_keys specified in write_options.",
                ))

    def _check_join_nodes(self, dag: DAG, result: ValidationResult) -> None:
        parent_ids = {c for _, c in dag.edges}
        child_ids = {p for p, _ in dag.edges}
        for node in dag.nodes.values():
            if not isinstance(node, JoinNode):
                continue
            # Must have exactly 2 incoming edges
            incoming = [p for p, c in dag.edges if c == node.id]
            if len(incoming) != 2:
                result.add(ValidationIssue(
                    node_id=node.id,
                    severity=ValidationSeverity.ERROR,
                    code="JOIN_WRONG_PARENTS",
                    message=f"JoinNode {node.id} has {len(incoming)} parent(s); expected 2.",
                ))
            # Empty join keys
            if not node.left_key or not node.right_key:
                result.add(ValidationIssue(
                    node_id=node.id,
                    severity=ValidationSeverity.WARNING,
                    code="JOIN_MISSING_KEYS",
                    message=f"JoinNode {node.id} has empty join keys — user confirmation required.",
                ))

    def _check_filter_nodes(self, dag: DAG, result: ValidationResult) -> None:
        for node in dag.nodes.values():
            if not isinstance(node, FilterNode):
                continue
            if not node.predicate.strip():
                result.add(ValidationIssue(
                    node_id=node.id,
                    severity=ValidationSeverity.ERROR,
                    code="FILTER_EMPTY_PREDICATE",
                    message=f"FilterNode {node.id} has an empty predicate.",
                ))
            if not node.parent_id or node.parent_id not in dag.nodes:
                result.add(ValidationIssue(
                    node_id=node.id,
                    severity=ValidationSeverity.ERROR,
                    code="FILTER_MISSING_PARENT",
                    message=f"FilterNode {node.id} references a missing parent: {node.parent_id!r}.",
                ))

    def _check_disconnected(self, dag: DAG, result: ValidationResult) -> None:
        connected = set()
        for p, c in dag.edges:
            connected.add(p)
            connected.add(c)
        for nid in dag.nodes:
            if nid not in connected and len(dag.nodes) > 1:
                result.add(ValidationIssue(
                    node_id=nid,
                    severity=ValidationSeverity.WARNING,
                    code="DISCONNECTED_NODE",
                    message=f"Node {nid} ({dag.nodes[nid].node_type()}) is not connected to any other node.",
                ))
