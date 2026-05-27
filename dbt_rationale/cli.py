"""CLI entry point for the rationale analyzer.

Usage:
    python -m dbt_rationale <project_path>
    python -m dbt_rationale <project_path> --enforce hard --min-score 60
    python -m dbt_rationale <project_path> --format json

Exit codes (for CI):
    0  All checks passed (or enforcement is off/soft)
    1  Enforcement criteria not met (hard mode only)
    2  Fatal error (invalid project path, etc.)
"""

import argparse
import json
import sys
from typing import List, Optional

from dbt_rationale.analyzer import ExpiringException, RationaleReport, analyze
from dbt_rationale.config import Config, EnforcementLevel, load_config
from dbt_rationale.parser import parse_project
from dbt_rationale.validator import Severity


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dbt-rationale",
        description="Analyze dbt projects for structured rationale metadata.",
    )
    parser.add_argument(
        "project_path",
        help="Path to the dbt project root directory.",
    )
    parser.add_argument(
        "--enforce",
        choices=["off", "soft", "hard"],
        default=None,
        help="Enforcement level (overrides .rationale.yml). Default: soft.",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=None,
        help="Minimum aggregate quality score to pass (0-100, hard mode).",
    )
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=None,
        help="Minimum coverage percentage to pass (0-100, hard mode).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        dest="output_format",
        help="Output format. Default: text.",
    )
    parser.add_argument(
        "--exception-warn-days",
        type=int,
        default=None,
        help="Warn about exceptions expiring within N days. Default: 30.",
    )
    return parser


def _merge_cli_into_config(args: argparse.Namespace, config: Config) -> Config:
    """CLI flags override config file values."""
    if args.enforce is not None:
        config.enforcement = EnforcementLevel(args.enforce)
    if args.min_score is not None:
        config.min_score = args.min_score
    if args.min_coverage is not None:
        config.min_coverage = args.min_coverage
    if args.exception_warn_days is not None:
        config.exception_warn_days = args.exception_warn_days
    return config


# ---------------------------------------------------------------------------
# Text output
# ---------------------------------------------------------------------------

def _render_text(report: RationaleReport, config: Config) -> str:
    lines: List[str] = []

    lines.append("=" * 60)
    lines.append("  dbt Rationale Analysis Report")
    lines.append("=" * 60)
    lines.append("")

    # Coverage
    lines.append(f"Coverage:  {report.covered_resources}/{report.total_resources} "
                 f"resources ({report.coverage_percentage:.1f}%)")
    lines.append(f"Score:     {report.aggregate_score:.1f}/100")
    lines.append(f"Errors:    {report.total_errors}")
    lines.append(f"Warnings:  {report.total_warnings}")
    lines.append("")

    # Validation issues
    if report.total_errors > 0 or report.total_warnings > 0:
        lines.append("-" * 60)
        lines.append("  Validation Issues")
        lines.append("-" * 60)
        for obj_score in report.object_scores:
            for issue in obj_score.validation.issues:
                prefix = "ERROR" if issue.severity == Severity.ERROR else "WARN "
                loc = f"{issue.resource_type}/{issue.resource_name}"
                lines.append(f"  {prefix}  {loc} -> {issue.field}: {issue.message}")
        lines.append("")

    # Expiring exceptions
    if report.expiring_exceptions:
        lines.append("-" * 60)
        lines.append("  Expiring / Expired Exceptions")
        lines.append("-" * 60)
        for exc in sorted(report.expiring_exceptions, key=lambda e: e.days_remaining):
            status = "EXPIRED" if exc.is_expired else f"{exc.days_remaining}d remaining"
            loc = f"{exc.resource_type}/{exc.resource_name}"
            lines.append(f"  {loc} -> {exc.rule}: {status} (expires {exc.expires})")
        lines.append("")

    # Uncovered resources
    if report.uncovered_details:
        lines.append("-" * 60)
        lines.append("  Resources Missing Rationale")
        lines.append("-" * 60)
        for unc in report.uncovered_details:
            lines.append(f"  {unc['resource_type']}/{unc['resource_name']}  ({unc['file_path']})")
        lines.append("")

    # Per-object scores
    if report.object_scores:
        lines.append("-" * 60)
        lines.append("  Per-Object Scores")
        lines.append("-" * 60)
        for obj_score in sorted(report.object_scores, key=lambda s: s.percentage):
            loc = f"{obj_score.resource_type}/{obj_score.resource_name}"
            lines.append(f"  {obj_score.score:3d}/{obj_score.max_score}  {loc}")
        lines.append("")

    # Parse errors
    if report.parse_errors:
        lines.append("-" * 60)
        lines.append("  Parse Errors")
        lines.append("-" * 60)
        for err in report.parse_errors:
            lines.append(f"  {err['file']}: {err['error']}")
        lines.append("")

    # Enforcement result
    lines.append("=" * 60)
    passed, reasons = _check_enforcement(report, config)
    if config.enforcement == EnforcementLevel.OFF:
        lines.append("  Enforcement: OFF (report only)")
    elif passed:
        lines.append(f"  Enforcement: {config.enforcement.value.upper()} - PASSED")
    else:
        lines.append(f"  Enforcement: {config.enforcement.value.upper()} - FAILED")
        for reason in reasons:
            lines.append(f"    - {reason}")
    lines.append("=" * 60)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Enforcement check
# ---------------------------------------------------------------------------

def _check_enforcement(report: RationaleReport, config: Config) -> tuple:
    """Check whether the report passes enforcement criteria.

    Returns (passed: bool, failure_reasons: list[str]).
    """
    if config.enforcement == EnforcementLevel.OFF:
        return True, []

    reasons: List[str] = []

    if report.total_errors > 0:
        reasons.append(f"{report.total_errors} validation error(s)")

    if config.min_score > 0 and report.aggregate_score < config.min_score:
        reasons.append(
            f"Aggregate score {report.aggregate_score:.1f} < minimum {config.min_score}"
        )

    if config.min_coverage > 0 and report.coverage_percentage < config.min_coverage:
        reasons.append(
            f"Coverage {report.coverage_percentage:.1f}% < minimum {config.min_coverage}%"
        )

    return len(reasons) == 0, reasons


# ---------------------------------------------------------------------------
# GitHub Actions annotations
# ---------------------------------------------------------------------------

def _emit_github_annotations(report: RationaleReport) -> None:
    """Emit GitHub Actions workflow commands for inline annotations.

    Writes to stderr so annotations don't interfere with stdout
    (especially important for --format json).  GitHub Actions reads
    workflow commands from both stdout and stderr.
    """
    for obj_score in report.object_scores:
        for issue in obj_score.validation.issues:
            level = "error" if issue.severity == Severity.ERROR else "warning"
            file_arg = f"file={issue.file_path}" if issue.file_path else ""
            msg = f"{issue.resource_type}/{issue.resource_name} -> {issue.field}: {issue.message}"
            print(f"::{level} {file_arg}::{msg}", file=sys.stderr)

    for exc in report.expiring_exceptions:
        level = "error" if exc.is_expired else "warning"
        file_arg = f"file={exc.file_path}" if exc.file_path else ""
        status = "EXPIRED" if exc.is_expired else f"expires in {exc.days_remaining} days"
        msg = f"Exception on {exc.rule} {status}"
        print(f"::{level} {file_arg}::{msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    import os
    if not os.path.isdir(args.project_path):
        print(f"Error: '{args.project_path}' is not a directory.", file=sys.stderr)
        return 2

    # Load config
    config = load_config(args.project_path)
    config = _merge_cli_into_config(args, config)

    # Parse
    parse_result = parse_project(args.project_path)

    # Analyze
    report = analyze(
        parse_result,
        exception_warn_days=config.exception_warn_days,
    )

    # Output
    if args.output_format == "json":
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(_render_text(report, config))

    # GitHub Actions annotations (detect CI environment)
    if os.environ.get("GITHUB_ACTIONS") == "true":
        _emit_github_annotations(report)

    # Exit code
    passed, _ = _check_enforcement(report, config)
    if not passed:
        return config.exit_code_on_failure

    return 0


if __name__ == "__main__":
    sys.exit(main())
