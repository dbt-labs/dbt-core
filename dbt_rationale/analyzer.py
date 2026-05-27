"""Coverage scoring and quality analysis for rationale metadata.

Computes per-object quality scores, aggregate coverage metrics, and
expiring exception alerts. Outputs a structured RationaleReport that
can be rendered as text or JSON.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from dbt_rationale.parser import ParseResult, RationaleEntry
from dbt_rationale.schema import QUALITY_WEIGHTS, MAX_QUALITY_SCORE
from dbt_rationale.validator import (
    Severity,
    ValidationResult,
    validate_rationale,
)


@dataclass
class ObjectScore:
    """Quality score for a single resource's rationale."""
    resource_type: str
    resource_name: str
    file_path: str
    score: int
    max_score: int
    breakdown: Dict[str, int]
    validation: ValidationResult

    @property
    def percentage(self) -> float:
        return (self.score / self.max_score * 100) if self.max_score > 0 else 0.0


@dataclass
class ExpiringException:
    """An exception with an expiration date that is approaching or past."""
    resource_type: str
    resource_name: str
    file_path: str
    rule: str
    expires: str
    days_remaining: int
    is_expired: bool


@dataclass
class RationaleReport:
    """Full analysis report for a dbt project's rationale metadata."""
    # Scores
    object_scores: List[ObjectScore] = field(default_factory=list)
    aggregate_score: float = 0.0

    # Coverage
    total_resources: int = 0
    covered_resources: int = 0
    uncovered_resources: int = 0
    coverage_percentage: float = 0.0

    # Validation
    total_errors: int = 0
    total_warnings: int = 0

    # Exceptions
    expiring_exceptions: List[ExpiringException] = field(default_factory=list)

    # Uncovered resource details
    uncovered_details: List[Dict[str, str]] = field(default_factory=list)

    # Parse errors
    parse_errors: List[Dict[str, str]] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        """No validation errors."""
        return self.total_errors == 0

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-friendly dict."""
        return {
            "aggregate_score": round(self.aggregate_score, 1),
            "coverage": {
                "total": self.total_resources,
                "covered": self.covered_resources,
                "uncovered": self.uncovered_resources,
                "percentage": round(self.coverage_percentage, 1),
            },
            "validation": {
                "errors": self.total_errors,
                "warnings": self.total_warnings,
            },
            "expiring_exceptions": [
                {
                    "resource": f"{e.resource_type}/{e.resource_name}",
                    "rule": e.rule,
                    "expires": e.expires,
                    "days_remaining": e.days_remaining,
                    "is_expired": e.is_expired,
                }
                for e in self.expiring_exceptions
            ],
            "object_scores": [
                {
                    "resource": f"{s.resource_type}/{s.resource_name}",
                    "file": s.file_path,
                    "score": s.score,
                    "max_score": s.max_score,
                    "percentage": round(s.percentage, 1),
                    "errors": s.validation.error_count,
                    "warnings": s.validation.warning_count,
                }
                for s in self.object_scores
            ],
            "uncovered": self.uncovered_details,
            "parse_errors": self.parse_errors,
        }


def _score_rationale(data: Dict[str, Any]) -> tuple:
    """Compute quality score for a rationale dict.

    Returns (total_score, breakdown_dict).
    """
    breakdown: Dict[str, int] = {}

    for field_name, weight in QUALITY_WEIGHTS.items():
        val = data.get(field_name)
        if val is not None:
            # For list fields, only score if non-empty
            if isinstance(val, list):
                breakdown[field_name] = weight if len(val) > 0 else 0
            elif isinstance(val, str):
                breakdown[field_name] = weight if len(val.strip()) > 0 else 0
            else:
                breakdown[field_name] = weight
        else:
            breakdown[field_name] = 0

    return sum(breakdown.values()), breakdown


def _find_expiring_exceptions(
    entry: RationaleEntry,
    today: Optional[date] = None,
    warn_days: int = 30,
) -> List[ExpiringException]:
    """Find exceptions that are expired or expiring within warn_days."""
    if today is None:
        today = date.today()

    results = []
    exceptions = entry.rationale.get("exceptions", [])
    if not isinstance(exceptions, list):
        return results

    for exc in exceptions:
        if not isinstance(exc, dict):
            continue
        expires_str = exc.get("expires")
        if not expires_str:
            continue
        try:
            expires_date = datetime.strptime(str(expires_str), "%Y-%m-%d").date()
        except ValueError:
            continue

        delta = (expires_date - today).days
        if delta <= warn_days:
            results.append(ExpiringException(
                resource_type=entry.resource_type,
                resource_name=entry.resource_name,
                file_path=entry.file_path,
                rule=exc.get("rule", "<unknown>"),
                expires=str(expires_str),
                days_remaining=delta,
                is_expired=delta < 0,
            ))

    return results


def analyze(
    parse_result: ParseResult,
    today: Optional[date] = None,
    exception_warn_days: int = 30,
) -> RationaleReport:
    """Run full analysis on parsed rationale data.

    Args:
        parse_result: Output from parser.parse_project().
        today: Override date for expiration checks (useful for testing).
        exception_warn_days: Warn about exceptions expiring within this many days.

    Returns:
        A RationaleReport with scores, coverage, validation, and exception alerts.
    """
    report = RationaleReport()
    report.total_resources = parse_result.total_resources
    report.covered_resources = len(parse_result.entries)
    report.uncovered_resources = len(parse_result.uncovered)
    report.uncovered_details = parse_result.uncovered
    report.parse_errors = parse_result.errors

    if report.total_resources > 0:
        report.coverage_percentage = (report.covered_resources / report.total_resources) * 100
    else:
        report.coverage_percentage = 100.0  # No resources = vacuously covered

    all_scores: List[float] = []

    for entry in parse_result.entries:
        # Validate
        validation = validate_rationale(
            entry.rationale,
            resource_type=entry.resource_type,
            resource_name=entry.resource_name,
            file_path=entry.file_path,
        )

        # Score
        score, breakdown = _score_rationale(entry.rationale)

        obj_score = ObjectScore(
            resource_type=entry.resource_type,
            resource_name=entry.resource_name,
            file_path=entry.file_path,
            score=score,
            max_score=MAX_QUALITY_SCORE,
            breakdown=breakdown,
            validation=validation,
        )
        report.object_scores.append(obj_score)
        all_scores.append(obj_score.percentage)

        report.total_errors += validation.error_count
        report.total_warnings += validation.warning_count

        # Expiring exceptions
        report.expiring_exceptions.extend(
            _find_expiring_exceptions(entry, today=today, warn_days=exception_warn_days)
        )

    # Uncovered resources get score 0
    for _ in parse_result.uncovered:
        all_scores.append(0.0)

    # Aggregate score
    if all_scores:
        report.aggregate_score = sum(all_scores) / len(all_scores)
    else:
        report.aggregate_score = 100.0

    return report
