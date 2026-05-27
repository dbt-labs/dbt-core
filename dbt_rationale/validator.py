"""Validate rationale blocks against the v1 schema.

Produces typed, actionable validation errors that can be used for both
human-readable reports and CI enforcement decisions.
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from dbt_rationale.schema import (
    CHANGE_RATIONALE_FIELDS,
    EXCEPTION_FIELDS,
    OBJECT_RATIONALE_FIELDS,
    REFERENCE_FIELDS,
    ChangeType,
    DecisionType,
    ReasonCategory,
    RiskLevel,
    enum_values,
)

# ISO date pattern: YYYY-MM-DD
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"


@dataclass
class ValidationIssue:
    """A single validation problem found in a rationale block."""
    severity: Severity
    field: str
    message: str
    resource_type: str = ""
    resource_name: str = ""
    file_path: str = ""

    def __str__(self) -> str:
        loc = f"{self.resource_type}/{self.resource_name}" if self.resource_name else ""
        return f"[{self.severity.value}] {loc} -> {self.field}: {self.message}"


@dataclass
class ValidationResult:
    """Aggregated validation result for one or more rationale blocks."""
    issues: List[ValidationIssue] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == Severity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == Severity.WARNING)

    @property
    def is_valid(self) -> bool:
        return self.error_count == 0

    def merge(self, other: "ValidationResult") -> None:
        self.issues.extend(other.issues)


def _issue(
    severity: Severity,
    field_name: str,
    message: str,
    resource_type: str = "",
    resource_name: str = "",
    file_path: str = "",
) -> ValidationIssue:
    return ValidationIssue(
        severity=severity,
        field=field_name,
        message=message,
        resource_type=resource_type,
        resource_name=resource_name,
        file_path=file_path,
    )


# ---------------------------------------------------------------------------
# Object-level rationale validation
# ---------------------------------------------------------------------------

def validate_reference(ref: Any, prefix: str = "references[]") -> List[ValidationIssue]:
    """Validate a single reference object."""
    issues: List[ValidationIssue] = []
    if not isinstance(ref, dict):
        issues.append(_issue(Severity.ERROR, prefix, "must be a mapping"))
        return issues

    for fname, (required, _desc) in REFERENCE_FIELDS.items():
        val = ref.get(fname)
        if required and val is None:
            issues.append(_issue(Severity.ERROR, f"{prefix}.{fname}", "required field missing"))
        elif val is not None and not isinstance(val, str):
            issues.append(_issue(Severity.ERROR, f"{prefix}.{fname}", f"expected string, got {type(val).__name__}"))

    # A reference should have at least id or url
    if isinstance(ref, dict) and ref.get("id") is None and ref.get("url") is None:
        issues.append(_issue(Severity.WARNING, prefix, "reference should have at least 'id' or 'url'"))

    # Warn on unknown fields
    known = set(REFERENCE_FIELDS.keys())
    for k in ref:
        if k not in known:
            issues.append(_issue(Severity.WARNING, f"{prefix}.{k}", f"unknown field '{k}'"))

    return issues


def validate_exception(exc: Any, prefix: str = "exceptions[]") -> List[ValidationIssue]:
    """Validate a single exception entry."""
    issues: List[ValidationIssue] = []
    if not isinstance(exc, dict):
        issues.append(_issue(Severity.ERROR, prefix, "must be a mapping"))
        return issues

    for fname, (required, _desc) in EXCEPTION_FIELDS.items():
        val = exc.get(fname)
        if required and val is None:
            issues.append(_issue(Severity.ERROR, f"{prefix}.{fname}", "required field missing"))
        elif fname == "expires" and val is not None:
            if not isinstance(val, str) or not _DATE_RE.match(str(val)):
                issues.append(_issue(
                    Severity.ERROR, f"{prefix}.{fname}",
                    f"expected date string (YYYY-MM-DD), got '{val}'"
                ))
        elif fname == "reference" and val is not None:
            issues.extend(validate_reference(val, prefix=f"{prefix}.reference"))
        elif val is not None and fname not in ("expires", "reference") and not isinstance(val, str):
            issues.append(_issue(
                Severity.ERROR, f"{prefix}.{fname}",
                f"expected string, got {type(val).__name__}"
            ))

    # Warn on unknown fields
    known = set(EXCEPTION_FIELDS.keys())
    for k in exc:
        if k not in known:
            issues.append(_issue(Severity.WARNING, f"{prefix}.{k}", f"unknown field '{k}'"))

    return issues


def validate_object_rationale(
    data: Dict[str, Any],
    resource_type: str = "",
    resource_name: str = "",
    file_path: str = "",
) -> ValidationResult:
    """Validate an object-level rationale block."""
    result = ValidationResult()

    if not isinstance(data, dict):
        result.issues.append(_issue(
            Severity.ERROR, "rationale", "must be a mapping",
            resource_type, resource_name, file_path,
        ))
        return result

    # Check required / typed fields
    for fname, (required, desc) in OBJECT_RATIONALE_FIELDS.items():
        val = data.get(fname)

        if required and val is None:
            result.issues.append(_issue(
                Severity.ERROR, fname, f"required field missing (expected {desc})",
                resource_type, resource_name, file_path,
            ))
            continue

        if val is None:
            continue

        # Type-specific validation
        if fname in ("intent", "owner", "domain"):
            if not isinstance(val, str):
                result.issues.append(_issue(
                    Severity.ERROR, fname, f"expected string, got {type(val).__name__}",
                    resource_type, resource_name, file_path,
                ))

        elif fname == "decision_type":
            valid_values = enum_values(DecisionType)
            if val not in valid_values:
                result.issues.append(_issue(
                    Severity.ERROR, fname,
                    f"invalid value '{val}'; expected one of {valid_values}",
                    resource_type, resource_name, file_path,
                ))

        elif fname == "references":
            if not isinstance(val, list):
                result.issues.append(_issue(
                    Severity.ERROR, fname, "expected a list",
                    resource_type, resource_name, file_path,
                ))
            else:
                for i, ref in enumerate(val):
                    for iss in validate_reference(ref, prefix=f"references[{i}]"):
                        iss.resource_type = resource_type
                        iss.resource_name = resource_name
                        iss.file_path = file_path
                        result.issues.append(iss)

        elif fname == "policy_bindings":
            if not isinstance(val, list):
                result.issues.append(_issue(
                    Severity.ERROR, fname, "expected a list of strings",
                    resource_type, resource_name, file_path,
                ))
            elif not all(isinstance(v, str) for v in val):
                result.issues.append(_issue(
                    Severity.ERROR, fname, "all items must be strings",
                    resource_type, resource_name, file_path,
                ))

        elif fname == "exceptions":
            if not isinstance(val, list):
                result.issues.append(_issue(
                    Severity.ERROR, fname, "expected a list",
                    resource_type, resource_name, file_path,
                ))
            else:
                for i, exc in enumerate(val):
                    for iss in validate_exception(exc, prefix=f"exceptions[{i}]"):
                        iss.resource_type = resource_type
                        iss.resource_name = resource_name
                        iss.file_path = file_path
                        result.issues.append(iss)

    # Warn on unknown top-level fields (allow change_rationale at same level)
    known = set(OBJECT_RATIONALE_FIELDS.keys()) | {"change_rationale"}
    for k in data:
        if k not in known:
            result.issues.append(_issue(
                Severity.WARNING, k, f"unknown field '{k}' in rationale block",
                resource_type, resource_name, file_path,
            ))

    return result


# ---------------------------------------------------------------------------
# Change-level rationale validation
# ---------------------------------------------------------------------------

def validate_change_rationale(
    data: Dict[str, Any],
    resource_type: str = "",
    resource_name: str = "",
    file_path: str = "",
) -> ValidationResult:
    """Validate a change-level rationale block."""
    result = ValidationResult()

    if not isinstance(data, dict):
        result.issues.append(_issue(
            Severity.ERROR, "change_rationale", "must be a mapping",
            resource_type, resource_name, file_path,
        ))
        return result

    for fname, (required, desc) in CHANGE_RATIONALE_FIELDS.items():
        val = data.get(fname)

        if required and val is None:
            result.issues.append(_issue(
                Severity.ERROR, f"change_rationale.{fname}",
                f"required field missing (expected {desc})",
                resource_type, resource_name, file_path,
            ))
            continue

        if val is None:
            continue

        if fname == "change_type":
            valid_values = enum_values(ChangeType)
            if val not in valid_values:
                result.issues.append(_issue(
                    Severity.ERROR, f"change_rationale.{fname}",
                    f"invalid value '{val}'; expected one of {valid_values}",
                    resource_type, resource_name, file_path,
                ))

        elif fname == "reason_category":
            valid_values = enum_values(ReasonCategory)
            if not isinstance(val, list):
                result.issues.append(_issue(
                    Severity.ERROR, f"change_rationale.{fname}",
                    "expected a list",
                    resource_type, resource_name, file_path,
                ))
            else:
                for item in val:
                    if item not in valid_values:
                        result.issues.append(_issue(
                            Severity.ERROR, f"change_rationale.{fname}",
                            f"invalid value '{item}'; expected one of {valid_values}",
                            resource_type, resource_name, file_path,
                        ))

        elif fname == "summary":
            if not isinstance(val, str):
                result.issues.append(_issue(
                    Severity.ERROR, f"change_rationale.{fname}",
                    f"expected string, got {type(val).__name__}",
                    resource_type, resource_name, file_path,
                ))

        elif fname == "approved_by":
            if not isinstance(val, list):
                result.issues.append(_issue(
                    Severity.ERROR, f"change_rationale.{fname}",
                    "expected a list of strings",
                    resource_type, resource_name, file_path,
                ))
            elif not all(isinstance(v, str) for v in val):
                result.issues.append(_issue(
                    Severity.ERROR, f"change_rationale.{fname}",
                    "all items must be strings",
                    resource_type, resource_name, file_path,
                ))

        elif fname == "risk_level":
            valid_values = enum_values(RiskLevel)
            if val not in valid_values:
                result.issues.append(_issue(
                    Severity.ERROR, f"change_rationale.{fname}",
                    f"invalid value '{val}'; expected one of {valid_values}",
                    resource_type, resource_name, file_path,
                ))

    # Conditional: approved_by required when risk_level is High
    risk = data.get("risk_level")
    approved = data.get("approved_by")
    if risk == RiskLevel.HIGH.value and (not approved or len(approved) == 0):
        result.issues.append(_issue(
            Severity.ERROR, "change_rationale.approved_by",
            "required when risk_level is 'High'",
            resource_type, resource_name, file_path,
        ))

    # Warn on unknown fields
    known = set(CHANGE_RATIONALE_FIELDS.keys())
    for k in data:
        if k not in known:
            result.issues.append(_issue(
                Severity.WARNING, f"change_rationale.{k}",
                f"unknown field '{k}'",
                resource_type, resource_name, file_path,
            ))

    return result


# ---------------------------------------------------------------------------
# Combined validation
# ---------------------------------------------------------------------------

def validate_rationale(
    data: Dict[str, Any],
    resource_type: str = "",
    resource_name: str = "",
    file_path: str = "",
) -> ValidationResult:
    """Validate a rationale block that may contain both object-level and change-level fields."""
    # Object-level validation (the main rationale block)
    result = validate_object_rationale(data, resource_type, resource_name, file_path)

    # Change-level validation (nested under change_rationale key)
    change_data = data.get("change_rationale")
    if change_data is not None:
        change_result = validate_change_rationale(
            change_data, resource_type, resource_name, file_path
        )
        result.merge(change_result)

    return result
