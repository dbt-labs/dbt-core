"""Schema definitions for dbt rationale metadata (v1).

Defines the structured schema for both object-level rationale (persistent intent)
and change-level rationale (decision events). All validation constraints are
encoded here so that parser, validator, and analyzer share a single source of truth.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class DecisionType(str, Enum):
    BUSINESS_DEFINITION = "Business Definition"
    TECHNICAL_OPTIMIZATION = "Technical Optimization"
    REGULATORY = "Regulatory"
    EXPERIMENTAL = "Experimental"


class ChangeType(str, Enum):
    CREATION = "Creation"
    MODIFICATION = "Modification"
    DEPRECATION = "Deprecation"


class ReasonCategory(str, Enum):
    REGULATORY = "Regulatory"
    BUG_FIX = "Bug Fix"
    OPTIMIZATION = "Optimization"
    BUSINESS_ALIGNMENT = "Business Alignment"


class RiskLevel(str, Enum):
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"


# Resource types that can carry rationale
RATIONALE_RESOURCE_TYPES = frozenset({
    "models",
    "metrics",
    "semantic_models",
    "sources",
    "exposures",
    "tests",
})


# ---------------------------------------------------------------------------
# Dataclasses — Object-Level Rationale
# ---------------------------------------------------------------------------

@dataclass
class Reference:
    type: str
    id: Optional[str] = None
    url: Optional[str] = None


@dataclass
class ExceptionEntry:
    rule: str
    justification: str
    approved_by: str
    expires: Optional[str] = None   # ISO date string
    reference: Optional[Reference] = None


@dataclass
class ObjectRationale:
    """Persistent intent attached to a dbt resource."""
    intent: str
    owner: str
    domain: Optional[str] = None
    decision_type: Optional[str] = None
    references: List[Reference] = field(default_factory=list)
    policy_bindings: List[str] = field(default_factory=list)
    exceptions: List[ExceptionEntry] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Dataclasses — Change-Level Rationale
# ---------------------------------------------------------------------------

@dataclass
class ChangeRationale:
    """Decision event captured during a change (PR / CI / UI)."""
    change_type: str
    reason_category: List[str]
    summary: str
    approved_by: List[str] = field(default_factory=list)
    risk_level: Optional[str] = None


# ---------------------------------------------------------------------------
# Field-level metadata for validation
# ---------------------------------------------------------------------------

# Object-level: field name -> (required, expected_type_description)
OBJECT_RATIONALE_FIELDS: Dict[str, tuple] = {
    "intent":          (True,  "string"),
    "owner":           (True,  "string"),
    "domain":          (False, "string"),
    "decision_type":   (False, f"enum: {[e.value for e in DecisionType]}"),
    "references":      (False, "list of reference objects"),
    "policy_bindings": (False, "list of strings"),
    "exceptions":      (False, "list of exception objects"),
}

EXCEPTION_FIELDS: Dict[str, tuple] = {
    "rule":          (True,  "string"),
    "justification": (True,  "string"),
    "approved_by":   (True,  "string"),
    "expires":       (False, "date string (YYYY-MM-DD)"),
    "reference":     (False, "reference object"),
}

REFERENCE_FIELDS: Dict[str, tuple] = {
    "type": (True,  "string"),
    "id":   (False, "string"),
    "url":  (False, "string"),
}

# Change-level: field name -> (required, expected_type_description)
CHANGE_RATIONALE_FIELDS: Dict[str, tuple] = {
    "change_type":      (True,  f"enum: {[e.value for e in ChangeType]}"),
    "reason_category":  (True,  f"list of enum: {[e.value for e in ReasonCategory]}"),
    "summary":          (True,  "string"),
    "approved_by":      (False, "list of strings"),  # conditional: required when risk_level=High
    "risk_level":       (False, f"enum: {[e.value for e in RiskLevel]}"),
}


# ---------------------------------------------------------------------------
# Quality scoring weights (object-level)
# ---------------------------------------------------------------------------

QUALITY_WEIGHTS: Dict[str, int] = {
    "intent":          40,
    "owner":           20,
    "domain":          10,
    "decision_type":   10,
    "references":      10,
    "policy_bindings": 10,
}

MAX_QUALITY_SCORE = sum(QUALITY_WEIGHTS.values())  # 100


def enum_values(enum_cls: type) -> List[str]:
    """Return the string values of an enum class."""
    return [e.value for e in enum_cls]
