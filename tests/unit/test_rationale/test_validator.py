"""Tests for rationale validator."""

import pytest

from dbt_rationale.validator import (
    Severity,
    ValidationIssue,
    ValidationResult,
    validate_change_rationale,
    validate_exception,
    validate_object_rationale,
    validate_rationale,
    validate_reference,
)


class TestValidationIssue:
    def test_str_with_resource(self):
        issue = ValidationIssue(
            severity=Severity.ERROR,
            field="intent",
            message="required field missing",
            resource_type="models",
            resource_name="fct_revenue",
        )
        s = str(issue)
        assert "[error]" in s
        assert "models/fct_revenue" in s
        assert "intent" in s

    def test_str_without_resource(self):
        issue = ValidationIssue(
            severity=Severity.WARNING,
            field="domain",
            message="some warning",
        )
        s = str(issue)
        assert "[warning]" in s
        assert "domain" in s


class TestValidationResult:
    def test_merge(self):
        r1 = ValidationResult(issues=[
            ValidationIssue(Severity.ERROR, "f1", "msg1"),
        ])
        r2 = ValidationResult(issues=[
            ValidationIssue(Severity.WARNING, "f2", "msg2"),
        ])
        r1.merge(r2)
        assert len(r1.issues) == 2
        assert r1.error_count == 1
        assert r1.warning_count == 1


class TestValidateReference:
    def test_valid_reference_with_id(self):
        issues = validate_reference({"type": "jira", "id": "FIN-123"})
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) == 0

    def test_valid_reference_with_url(self):
        issues = validate_reference({"type": "notion", "url": "https://notion.so/page"})
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) == 0

    def test_missing_type(self):
        issues = validate_reference({"id": "FIN-123"})
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert any("type" in i.field for i in errors)

    def test_no_id_or_url_warns(self):
        issues = validate_reference({"type": "jira"})
        warnings = [i for i in issues if i.severity == Severity.WARNING]
        assert len(warnings) > 0

    def test_not_a_dict(self):
        issues = validate_reference("not a dict")
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR

    def test_non_string_type_value(self):
        issues = validate_reference({"type": 123, "id": "FIN-1"})
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert any("expected string" in i.message for i in errors)

    def test_unknown_field_warns(self):
        issues = validate_reference({"type": "jira", "id": "FIN-1", "extra": "field"})
        warnings = [i for i in issues if i.severity == Severity.WARNING]
        assert any("unknown field" in i.message for i in warnings)


class TestValidateException:
    def test_valid_exception(self):
        issues = validate_exception({
            "rule": "GAAP_v1.refund_window",
            "justification": "Excluded per CFO directive",
            "approved_by": "cfo@company.com",
            "expires": "2026-06-30",
        })
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) == 0

    def test_missing_required_fields(self):
        issues = validate_exception({})
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) == 3  # rule, justification, approved_by

    def test_invalid_date(self):
        issues = validate_exception({
            "rule": "R1",
            "justification": "reason",
            "approved_by": "someone",
            "expires": "not-a-date",
        })
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert any("expires" in i.field for i in errors)

    def test_valid_date(self):
        issues = validate_exception({
            "rule": "R1",
            "justification": "reason",
            "approved_by": "someone",
            "expires": "2026-12-31",
        })
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) == 0

    def test_not_a_dict(self):
        issues = validate_exception("not a dict")
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR

    def test_non_string_required_field(self):
        issues = validate_exception({
            "rule": 123,
            "justification": "reason",
            "approved_by": "someone",
        })
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert any("expected string" in i.message for i in errors)

    def test_nested_reference(self):
        issues = validate_exception({
            "rule": "R1",
            "justification": "reason",
            "approved_by": "someone",
            "reference": {"type": "jira", "id": "FIN-1"},
        })
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) == 0

    def test_nested_invalid_reference(self):
        issues = validate_exception({
            "rule": "R1",
            "justification": "reason",
            "approved_by": "someone",
            "reference": "not a dict",
        })
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) > 0

    def test_unknown_field_warns(self):
        issues = validate_exception({
            "rule": "R1",
            "justification": "reason",
            "approved_by": "someone",
            "extra_field": "value",
        })
        warnings = [i for i in issues if i.severity == Severity.WARNING]
        assert any("unknown field" in i.message for i in warnings)


class TestValidateObjectRationale:
    def test_valid_full(self):
        result = validate_object_rationale({
            "intent": "Canonical revenue definition",
            "owner": "finance@company.com",
            "domain": "Revenue",
            "decision_type": "Business Definition",
            "references": [{"type": "jira", "id": "FIN-123"}],
            "policy_bindings": ["GAAP_v1"],
            "exceptions": [{
                "rule": "GAAP_v1.refund_window",
                "justification": "Excluded per CFO directive",
                "approved_by": "cfo@company.com",
                "expires": "2026-06-30",
            }],
        })
        assert result.is_valid
        assert result.error_count == 0

    def test_valid_minimal(self):
        result = validate_object_rationale({
            "intent": "Staging layer",
            "owner": "team@company.com",
        })
        assert result.is_valid

    def test_missing_intent(self):
        result = validate_object_rationale({
            "owner": "team@company.com",
        })
        assert not result.is_valid
        assert any("intent" in i.field for i in result.issues)

    def test_missing_owner(self):
        result = validate_object_rationale({
            "intent": "Some intent",
        })
        assert not result.is_valid
        assert any("owner" in i.field for i in result.issues)

    def test_invalid_decision_type(self):
        result = validate_object_rationale({
            "intent": "Intent",
            "owner": "owner",
            "decision_type": "InvalidType",
        })
        assert not result.is_valid
        assert any("decision_type" in i.field for i in result.issues)

    def test_valid_decision_types(self):
        for dt in ["Business Definition", "Technical Optimization", "Regulatory", "Experimental"]:
            result = validate_object_rationale({
                "intent": "Intent",
                "owner": "owner",
                "decision_type": dt,
            })
            assert result.is_valid, f"Failed for decision_type={dt}"

    def test_not_a_dict(self):
        result = validate_object_rationale("not a dict")
        assert not result.is_valid

    def test_unknown_field_warns(self):
        result = validate_object_rationale({
            "intent": "Intent",
            "owner": "owner",
            "custom_field": "value",
        })
        assert result.is_valid  # warnings don't fail
        assert result.warning_count > 0

    def test_policy_bindings_must_be_strings(self):
        result = validate_object_rationale({
            "intent": "Intent",
            "owner": "owner",
            "policy_bindings": [123, 456],
        })
        assert not result.is_valid

    def test_intent_non_string(self):
        result = validate_object_rationale({
            "intent": 123,
            "owner": "owner",
        })
        assert not result.is_valid
        assert any("intent" in i.field and "expected string" in i.message for i in result.issues)

    def test_references_not_a_list(self):
        result = validate_object_rationale({
            "intent": "Intent",
            "owner": "owner",
            "references": "not a list",
        })
        assert not result.is_valid
        assert any("references" in i.field for i in result.issues)

    def test_references_with_invalid_entry(self):
        result = validate_object_rationale({
            "intent": "Intent",
            "owner": "owner",
            "references": [{"type": "jira"}, "not a dict"],
        })
        # Second entry is invalid
        assert any("references[1]" in i.field for i in result.issues)

    def test_policy_bindings_not_a_list(self):
        result = validate_object_rationale({
            "intent": "Intent",
            "owner": "owner",
            "policy_bindings": "not a list",
        })
        assert not result.is_valid

    def test_exceptions_not_a_list(self):
        result = validate_object_rationale({
            "intent": "Intent",
            "owner": "owner",
            "exceptions": "not a list",
        })
        assert not result.is_valid

    def test_exceptions_with_invalid_entry(self):
        result = validate_object_rationale({
            "intent": "Intent",
            "owner": "owner",
            "exceptions": [{"rule": "R1", "justification": "j", "approved_by": "a"}, "not a dict"],
        })
        assert any("exceptions[1]" in i.field for i in result.issues)


class TestValidateChangeRationale:
    def test_valid_full(self):
        result = validate_change_rationale({
            "change_type": "Modification",
            "reason_category": ["Regulatory"],
            "summary": "Exclude refunds for GAAP",
            "approved_by": ["cfo@company.com"],
            "risk_level": "High",
        })
        assert result.is_valid

    def test_missing_required(self):
        result = validate_change_rationale({})
        assert not result.is_valid
        assert result.error_count == 3  # change_type, reason_category, summary

    def test_invalid_change_type(self):
        result = validate_change_rationale({
            "change_type": "InvalidType",
            "reason_category": ["Bug Fix"],
            "summary": "Fixed something",
        })
        assert not result.is_valid

    def test_invalid_reason_category(self):
        result = validate_change_rationale({
            "change_type": "Creation",
            "reason_category": ["Not A Category"],
            "summary": "Created something",
        })
        assert not result.is_valid

    def test_high_risk_requires_approval(self):
        result = validate_change_rationale({
            "change_type": "Modification",
            "reason_category": ["Bug Fix"],
            "summary": "Fixed something risky",
            "risk_level": "High",
            # Missing approved_by
        })
        assert not result.is_valid
        assert any("approved_by" in i.field for i in result.issues)

    def test_high_risk_with_approval_passes(self):
        result = validate_change_rationale({
            "change_type": "Modification",
            "reason_category": ["Bug Fix"],
            "summary": "Fixed something risky",
            "risk_level": "High",
            "approved_by": ["manager@company.com"],
        })
        assert result.is_valid

    def test_low_risk_no_approval_ok(self):
        result = validate_change_rationale({
            "change_type": "Modification",
            "reason_category": ["Optimization"],
            "summary": "Minor optimization",
            "risk_level": "Low",
        })
        assert result.is_valid

    def test_not_a_dict(self):
        result = validate_change_rationale("not a dict")
        assert not result.is_valid
        assert any("change_rationale" in i.field for i in result.issues)

    def test_reason_category_not_a_list(self):
        result = validate_change_rationale({
            "change_type": "Creation",
            "reason_category": "Regulatory",
            "summary": "Created something",
        })
        assert not result.is_valid
        assert any("reason_category" in i.field for i in result.issues)

    def test_summary_non_string(self):
        result = validate_change_rationale({
            "change_type": "Creation",
            "reason_category": ["Regulatory"],
            "summary": 123,
        })
        assert not result.is_valid
        assert any("summary" in i.field for i in result.issues)

    def test_approved_by_not_a_list(self):
        result = validate_change_rationale({
            "change_type": "Creation",
            "reason_category": ["Bug Fix"],
            "summary": "Fix",
            "approved_by": "single@email.com",
        })
        assert not result.is_valid
        assert any("approved_by" in i.field for i in result.issues)

    def test_approved_by_non_string_items(self):
        result = validate_change_rationale({
            "change_type": "Creation",
            "reason_category": ["Bug Fix"],
            "summary": "Fix",
            "approved_by": [123],
        })
        assert not result.is_valid
        assert any("approved_by" in i.field and "strings" in i.message for i in result.issues)

    def test_invalid_risk_level(self):
        result = validate_change_rationale({
            "change_type": "Creation",
            "reason_category": ["Bug Fix"],
            "summary": "Fix",
            "risk_level": "Critical",
        })
        assert not result.is_valid
        assert any("risk_level" in i.field for i in result.issues)

    def test_unknown_field_warns(self):
        result = validate_change_rationale({
            "change_type": "Creation",
            "reason_category": ["Bug Fix"],
            "summary": "Fix",
            "extra_field": "value",
        })
        # Unknown fields are warnings, not errors
        assert result.is_valid
        assert result.warning_count > 0


class TestValidateRationale:
    def test_combined_validation(self):
        result = validate_rationale({
            "intent": "Revenue definition",
            "owner": "finance@company.com",
            "change_rationale": {
                "change_type": "Modification",
                "reason_category": ["Regulatory"],
                "summary": "GAAP alignment",
                "approved_by": ["cfo@company.com"],
                "risk_level": "High",
            },
        })
        assert result.is_valid

    def test_object_valid_change_invalid(self):
        result = validate_rationale({
            "intent": "Revenue definition",
            "owner": "finance@company.com",
            "change_rationale": {
                # Missing required fields
            },
        })
        assert not result.is_valid
        # Object is fine, but change_rationale has 3 errors
        assert result.error_count == 3
