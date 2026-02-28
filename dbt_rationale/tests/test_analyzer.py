"""Tests for rationale analyzer."""

import os
from datetime import date

import pytest

from dbt_rationale.analyzer import (
    ExpiringException,
    ObjectScore,
    RationaleReport,
    _find_expiring_exceptions,
    _score_rationale,
    analyze,
)
from dbt_rationale.parser import RationaleEntry, parse_project

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class TestAnalyze:
    def test_full_project_analysis(self):
        result = parse_project(FIXTURES_DIR)
        report = analyze(result)
        assert isinstance(report, RationaleReport)
        assert report.total_resources > 0
        assert report.covered_resources > 0

    def test_empty_project(self, tmp_path):
        result = parse_project(str(tmp_path))
        report = analyze(result)
        assert report.total_resources == 0
        assert report.aggregate_score == 100.0  # vacuously true
        assert report.coverage_percentage == 100.0

    def test_coverage_calculation(self, tmp_path):
        # Create a project with 1 covered and 1 uncovered
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: covered
    meta:
      rationale:
        intent: "Has rationale"
        owner: "team@co.com"
  - name: uncovered
    description: "No rationale"
""")
        result = parse_project(str(tmp_path))
        report = analyze(result)
        assert report.total_resources == 2
        assert report.covered_resources == 1
        assert report.uncovered_resources == 1
        assert report.coverage_percentage == 50.0

    def test_quality_score_full_rationale(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: perfect
    meta:
      rationale:
        intent: "Full rationale"
        owner: "team@co.com"
        domain: "Analytics"
        decision_type: "Business Definition"
        references:
          - type: jira
            id: PROJ-1
        policy_bindings:
          - POLICY_1
""")
        result = parse_project(str(tmp_path))
        report = analyze(result)
        assert len(report.object_scores) == 1
        assert report.object_scores[0].score == 100
        assert report.aggregate_score == 100.0

    def test_quality_score_minimal_rationale(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: minimal
    meta:
      rationale:
        intent: "Just intent"
        owner: "team@co.com"
""")
        result = parse_project(str(tmp_path))
        report = analyze(result)
        assert len(report.object_scores) == 1
        # intent (40) + owner (20) = 60
        assert report.object_scores[0].score == 60

    def test_validation_errors_tracked(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: broken
    meta:
      rationale:
        domain: "Missing required fields"
""")
        result = parse_project(str(tmp_path))
        report = analyze(result)
        assert report.total_errors > 0
        assert not report.is_clean

    def test_report_to_dict(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: model1
    meta:
      rationale:
        intent: "Intent"
        owner: "owner"
""")
        result = parse_project(str(tmp_path))
        report = analyze(result)
        d = report.to_dict()
        assert "aggregate_score" in d
        assert "coverage" in d
        assert "validation" in d
        assert "object_scores" in d


class TestExpiringExceptions:
    def test_detects_expired(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: model_expired
    meta:
      rationale:
        intent: "Intent"
        owner: "owner"
        exceptions:
          - rule: "POLICY_1"
            justification: "reason"
            approved_by: "someone"
            expires: "2025-01-01"
""")
        result = parse_project(str(tmp_path))
        report = analyze(result, today=date(2026, 2, 17))
        assert len(report.expiring_exceptions) == 1
        assert report.expiring_exceptions[0].is_expired

    def test_detects_expiring_soon(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: model_expiring
    meta:
      rationale:
        intent: "Intent"
        owner: "owner"
        exceptions:
          - rule: "POLICY_1"
            justification: "reason"
            approved_by: "someone"
            expires: "2026-03-01"
""")
        result = parse_project(str(tmp_path))
        report = analyze(result, today=date(2026, 2, 17))
        assert len(report.expiring_exceptions) == 1
        assert not report.expiring_exceptions[0].is_expired
        assert report.expiring_exceptions[0].days_remaining == 12

    def test_ignores_far_future(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: model_ok
    meta:
      rationale:
        intent: "Intent"
        owner: "owner"
        exceptions:
          - rule: "POLICY_1"
            justification: "reason"
            approved_by: "someone"
            expires: "2027-12-31"
""")
        result = parse_project(str(tmp_path))
        report = analyze(result, today=date(2026, 2, 17))
        assert len(report.expiring_exceptions) == 0


class TestScoreRationale:
    def test_empty_list_fields_score_zero(self):
        score, breakdown = _score_rationale({
            "intent": "Intent",
            "owner": "owner",
            "references": [],
            "policy_bindings": [],
        })
        assert breakdown["references"] == 0
        assert breakdown["policy_bindings"] == 0
        assert score == 60  # intent + owner only

    def test_whitespace_only_string_scores_zero(self):
        score, breakdown = _score_rationale({
            "intent": "   ",
            "owner": "owner",
        })
        assert breakdown["intent"] == 0
        assert score == 20  # only owner

    def test_non_string_non_list_field_scores(self):
        # If someone puts an unexpected type, it still gets the weight
        score, breakdown = _score_rationale({
            "intent": "Intent",
            "owner": "owner",
            "domain": 123,  # not a string, but not None
        })
        assert breakdown["domain"] == 10


class TestFindExpiringExceptions:
    def test_non_list_exceptions(self):
        entry = RationaleEntry(
            resource_type="models",
            resource_name="m1",
            file_path="test.yml",
            rationale={"exceptions": "not a list"},
        )
        result = _find_expiring_exceptions(entry, today=date(2026, 2, 17))
        assert len(result) == 0

    def test_non_dict_exception_entry(self):
        entry = RationaleEntry(
            resource_type="models",
            resource_name="m1",
            file_path="test.yml",
            rationale={"exceptions": ["not a dict", 123]},
        )
        result = _find_expiring_exceptions(entry, today=date(2026, 2, 17))
        assert len(result) == 0

    def test_exception_without_expires(self):
        entry = RationaleEntry(
            resource_type="models",
            resource_name="m1",
            file_path="test.yml",
            rationale={"exceptions": [
                {"rule": "R1", "justification": "j", "approved_by": "a"},
            ]},
        )
        result = _find_expiring_exceptions(entry, today=date(2026, 2, 17))
        assert len(result) == 0

    def test_exception_with_invalid_date_format(self):
        entry = RationaleEntry(
            resource_type="models",
            resource_name="m1",
            file_path="test.yml",
            rationale={"exceptions": [
                {"rule": "R1", "justification": "j", "approved_by": "a", "expires": "not-a-date"},
            ]},
        )
        result = _find_expiring_exceptions(entry, today=date(2026, 2, 17))
        assert len(result) == 0

    def test_default_today(self):
        entry = RationaleEntry(
            resource_type="models",
            resource_name="m1",
            file_path="test.yml",
            rationale={"exceptions": [
                {"rule": "R1", "justification": "j", "approved_by": "a", "expires": "2020-01-01"},
            ]},
        )
        # Uses default today (should be in the past -> expired)
        result = _find_expiring_exceptions(entry)
        assert len(result) == 1
        assert result[0].is_expired


class TestAggregateScoring:
    def test_uncovered_resources_lower_aggregate(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        (models_dir / "schema.yml").write_text("""
models:
  - name: covered
    meta:
      rationale:
        intent: "Has rationale"
        owner: "team@co.com"
  - name: uncovered1
    description: "No rationale"
  - name: uncovered2
    description: "No rationale"
  - name: uncovered3
    description: "No rationale"
""")
        result = parse_project(str(tmp_path))
        report = analyze(result)
        # 1 covered (score 60) + 3 uncovered (score 0) = avg 15
        assert report.aggregate_score == 15.0
