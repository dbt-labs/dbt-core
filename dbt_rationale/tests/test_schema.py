"""Tests for rationale schema definitions."""

from dbt_rationale.schema import (
    CHANGE_RATIONALE_FIELDS,
    EXCEPTION_FIELDS,
    MAX_QUALITY_SCORE,
    OBJECT_RATIONALE_FIELDS,
    QUALITY_WEIGHTS,
    RATIONALE_RESOURCE_TYPES,
    ChangeType,
    DecisionType,
    ReasonCategory,
    RiskLevel,
    enum_values,
)


class TestEnums:
    def test_decision_type_values(self):
        assert set(enum_values(DecisionType)) == {
            "Business Definition",
            "Technical Optimization",
            "Regulatory",
            "Experimental",
        }

    def test_change_type_values(self):
        assert set(enum_values(ChangeType)) == {
            "Creation",
            "Modification",
            "Deprecation",
        }

    def test_reason_category_values(self):
        assert set(enum_values(ReasonCategory)) == {
            "Regulatory",
            "Bug Fix",
            "Optimization",
            "Business Alignment",
        }

    def test_risk_level_values(self):
        assert set(enum_values(RiskLevel)) == {"Low", "Medium", "High"}


class TestFieldMetadata:
    def test_object_rationale_required_fields(self):
        required = {k for k, (req, _) in OBJECT_RATIONALE_FIELDS.items() if req}
        assert required == {"intent", "owner"}

    def test_exception_required_fields(self):
        required = {k for k, (req, _) in EXCEPTION_FIELDS.items() if req}
        assert required == {"rule", "justification", "approved_by"}

    def test_change_rationale_required_fields(self):
        required = {k for k, (req, _) in CHANGE_RATIONALE_FIELDS.items() if req}
        assert required == {"change_type", "reason_category", "summary"}

    def test_quality_weights_sum_to_100(self):
        assert MAX_QUALITY_SCORE == 100

    def test_quality_weights_cover_expected_fields(self):
        assert set(QUALITY_WEIGHTS.keys()) == {
            "intent", "owner", "domain", "decision_type", "references", "policy_bindings"
        }


class TestResourceTypes:
    def test_supported_resource_types(self):
        assert "models" in RATIONALE_RESOURCE_TYPES
        assert "metrics" in RATIONALE_RESOURCE_TYPES
        assert "semantic_models" in RATIONALE_RESOURCE_TYPES
        assert "sources" in RATIONALE_RESOURCE_TYPES
        assert "exposures" in RATIONALE_RESOURCE_TYPES
        assert "tests" in RATIONALE_RESOURCE_TYPES
