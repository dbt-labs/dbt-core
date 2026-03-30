import os
from unittest.mock import patch

import pytest

from dbt.exceptions import DbtRuntimeError
from dbt.graph.selector_methods import MethodName
from dbt.graph.selector_spec import (
    IndirectSelection,
    SelectionCriteria,
    SelectionDifference,
    SelectionIntersection,
    SelectionUnion,
)


@pytest.mark.parametrize(
    "indirect_selection_value,expected_value",
    [(v, v) for v in IndirectSelection],
)
def test_selection_criteria_default_indirect_value(indirect_selection_value, expected_value):
    # Check selection criteria with indirect selection value would follow the resolved value in flags
    # if indirect selection is not specified in the selection criteria.
    with patch("dbt.graph.selector_spec.get_flags") as patched_get_flags:
        patched_get_flags.return_value.INDIRECT_SELECTION = indirect_selection_value
        patched_get_flags.INDIRECT_SELECTION = indirect_selection_value
        selection_dict_without_indirect_selection_specified = {
            "method": "path",
            "value": "models/marts/orders.sql",
            "children": False,
            "parents": False,
        }
        selection_criteria_without_indirect_selection_specified = (
            SelectionCriteria.selection_criteria_from_dict(
                selection_dict_without_indirect_selection_specified,
                selection_dict_without_indirect_selection_specified,
            )
        )
        assert (
            selection_criteria_without_indirect_selection_specified.indirect_selection
            == expected_value
        )
        selection_dict_without_indirect_selection_specified = {
            "method": "path",
            "value": "models/marts/orders.sql",
            "children": False,
            "parents": False,
            "indirect_selection": "buildable",
        }
        selection_criteria_with_indirect_selection_specified = (
            SelectionCriteria.selection_criteria_from_dict(
                selection_dict_without_indirect_selection_specified,
                selection_dict_without_indirect_selection_specified,
            )
        )
        assert (
            selection_criteria_with_indirect_selection_specified.indirect_selection == "buildable"
        )


def test_raw_parse_simple():
    raw = "asdf"
    result = SelectionCriteria.from_single_spec(raw)
    assert result.raw == raw
    assert result.method == MethodName.FQN
    assert result.method_arguments == []
    assert result.value == raw
    assert not result.childrens_parents
    assert not result.children
    assert not result.parents
    assert result.parents_depth is None
    assert result.children_depth is None


def test_raw_parse_simple_infer_path():
    raw = os.path.join("asdf", "*")
    result = SelectionCriteria.from_single_spec(raw)
    assert result.raw == raw
    assert result.method == MethodName.Path
    assert result.method_arguments == []
    assert result.value == raw
    assert not result.childrens_parents
    assert not result.children
    assert not result.parents
    assert result.parents_depth is None
    assert result.children_depth is None


def test_raw_parse_simple_infer_path_modified():
    raw = "@" + os.path.join("asdf", "*")
    result = SelectionCriteria.from_single_spec(raw)
    assert result.raw == raw
    assert result.method == MethodName.Path
    assert result.method_arguments == []
    assert result.value == raw[1:]
    assert result.childrens_parents
    assert not result.children
    assert not result.parents
    assert result.parents_depth is None
    assert result.children_depth is None


def test_raw_parse_simple_infer_fqn_parents():
    raw = "+asdf"
    result = SelectionCriteria.from_single_spec(raw)
    assert result.raw == raw
    assert result.method == MethodName.FQN
    assert result.method_arguments == []
    assert result.value == "asdf"
    assert not result.childrens_parents
    assert not result.children
    assert result.parents
    assert result.parents_depth is None
    assert result.children_depth is None


def test_raw_parse_simple_infer_fqn_children():
    raw = "asdf+"
    result = SelectionCriteria.from_single_spec(raw)
    assert result.raw == raw
    assert result.method == MethodName.FQN
    assert result.method_arguments == []
    assert result.value == "asdf"
    assert not result.childrens_parents
    assert result.children
    assert not result.parents
    assert result.parents_depth is None
    assert result.children_depth is None


def test_raw_parse_complex():
    raw = "2+config.arg.secondarg:argument_value+4"
    result = SelectionCriteria.from_single_spec(raw)
    assert result.raw == raw
    assert result.method == MethodName.Config
    assert result.method_arguments == ["arg", "secondarg"]
    assert result.value == "argument_value"
    assert not result.childrens_parents
    assert result.children
    assert result.parents
    assert result.parents_depth == 2
    assert result.children_depth == 4


def test_raw_parse_weird():
    # you can have an empty method name (defaults to FQN/path) and you can have
    # an empty value, so you can also have this...
    result = SelectionCriteria.from_single_spec("")
    assert result.raw == ""
    assert result.method == MethodName.FQN
    assert result.method_arguments == []
    assert result.value == ""
    assert not result.childrens_parents
    assert not result.children
    assert not result.parents
    assert result.parents_depth is None
    assert result.children_depth is None


def test_raw_parse_selector_method():
    """selector:foo parses as method=Selector, value=foo (for combining YAML selectors with --select/--exclude)."""
    raw = "1+selector:staging+2"
    result = SelectionCriteria.from_single_spec(raw)
    assert result.raw == raw
    assert result.method == MethodName.Selector
    assert not result.childrens_parents
    assert result.children
    assert result.parents
    assert result.method_arguments == []
    assert result.value == "staging"
    assert result.children_depth == 2
    assert result.parents_depth == 1


def test_raw_parse_invalid():
    with pytest.raises(DbtRuntimeError):
        SelectionCriteria.from_single_spec("invalid_method:something")

    with pytest.raises(DbtRuntimeError):
        SelectionCriteria.from_single_spec("@foo+")


class TestUnaccompaniedGraphOperators:
    """Graph operators with no target node should raise a clear DbtRuntimeError.

    Valid: '+my_model', 'my_model+', '2+my_model+3', '@my_model'
    Invalid: '+' (bare parents operator), '@' (bare childrens_parents), '1+' (depth with no target),
             '+ my_model' (space-separated — ambiguous and almost certainly a typo).
    """

    def test_bare_parents_operator_raises(self):
        with pytest.raises(DbtRuntimeError, match="graph operator requires a target node"):
            SelectionCriteria.from_single_spec("+")

    def test_space_separated_children_raises(self):
        with pytest.raises(DbtRuntimeError, match="unexpected whitespace"):
            SelectionCriteria.from_single_spec("my_model +")  # trailing space before children op
        with pytest.raises(DbtRuntimeError, match="unexpected whitespace"):
            SelectionCriteria.from_single_spec("+ my_model +")  # spaces on both sides

    def test_bare_childrens_parents_operator_raises(self):
        with pytest.raises(DbtRuntimeError, match="graph operator requires a target node"):
            SelectionCriteria.from_single_spec("@")

    def test_depth_with_no_target_raises(self):
        with pytest.raises(DbtRuntimeError, match="graph operator requires a target node"):
            SelectionCriteria.from_single_spec("2+")

    def test_space_separated_parents_raises(self):
        with pytest.raises(DbtRuntimeError, match="unexpected whitespace"):
            SelectionCriteria.from_single_spec("+ my_model")

    def test_valid_parents_still_works(self):
        result = SelectionCriteria.from_single_spec("+my_model")
        assert result.parents
        assert result.value == "my_model"

    def test_valid_children_still_works(self):
        result = SelectionCriteria.from_single_spec("my_model+")
        assert result.children
        assert result.value == "my_model"

    def test_valid_childrens_parents_still_works(self):
        result = SelectionCriteria.from_single_spec("@my_model")
        assert result.childrens_parents
        assert result.value == "my_model"

    def test_valid_depth_selector_still_works(self):
        result = SelectionCriteria.from_single_spec("2+my_model+3")
        assert result.parents
        assert result.parents_depth == 2
        assert result.children
        assert result.children_depth == 3
        assert result.value == "my_model"

    def test_direction_aware_error_message_parents_side(self):
        """Whitespace on the parents side suggests +model, not model+."""
        with pytest.raises(DbtRuntimeError, match=r'\+my_model'):
            SelectionCriteria.from_single_spec("+ my_model")

    def test_direction_aware_error_message_children_side(self):
        """Whitespace on the children side suggests model+, not +model."""
        with pytest.raises(DbtRuntimeError, match=r'my_model\+'):
            SelectionCriteria.from_single_spec("my_model +")


class TestDictFromSingleSpec:
    """Validation applied via dict_from_single_spec should return an error key
    (not raise) for invalid YAML-defined selectors, and produce correct dicts for valid ones."""

    def test_bare_parents_operator_returns_error(self):
        result = SelectionCriteria.dict_from_single_spec("+")
        assert "error" in result
        assert "graph operator requires a target node" in result["error"]

    def test_bare_childrens_parents_operator_returns_error(self):
        result = SelectionCriteria.dict_from_single_spec("@")
        assert "error" in result
        assert "graph operator requires a target node" in result["error"]

    def test_space_separated_returns_error(self):
        result = SelectionCriteria.dict_from_single_spec("+ my_model")
        assert "error" in result
        assert "unexpected whitespace" in result["error"]

    def test_valid_selector_returns_dict(self):
        result = SelectionCriteria.dict_from_single_spec("+my_model")
        assert result.get("parents") is True
        assert result.get("value") == "my_model"
        assert "error" not in result

    def test_valid_fqn_selector_returns_dict(self):
        result = SelectionCriteria.dict_from_single_spec("fqn:my_model")
        assert result.get("value") == "my_model"
        assert "error" not in result


def test_intersection():
    fqn_a = SelectionCriteria.from_single_spec("fqn:model_a")
    fqn_b = SelectionCriteria.from_single_spec("fqn:model_b")
    intersection = SelectionIntersection(components=[fqn_a, fqn_b])
    assert list(intersection) == [fqn_a, fqn_b]
    combined = intersection.combine_selections(
        [{"model_a", "model_b", "model_c"}, {"model_c", "model_d"}]
    )
    assert combined == {"model_c"}


def test_difference():
    fqn_a = SelectionCriteria.from_single_spec("fqn:model_a")
    fqn_b = SelectionCriteria.from_single_spec("fqn:model_b")
    difference = SelectionDifference(components=[fqn_a, fqn_b])
    assert list(difference) == [fqn_a, fqn_b]
    combined = difference.combine_selections(
        [{"model_a", "model_b", "model_c"}, {"model_c", "model_d"}]
    )
    assert combined == {"model_a", "model_b"}

    fqn_c = SelectionCriteria.from_single_spec("fqn:model_c")
    difference = SelectionDifference(components=[fqn_a, fqn_b, fqn_c])
    assert list(difference) == [fqn_a, fqn_b, fqn_c]
    combined = difference.combine_selections(
        [{"model_a", "model_b", "model_c"}, {"model_c", "model_d"}, {"model_a"}]
    )
    assert combined == {"model_b"}


def test_union():
    fqn_a = SelectionCriteria.from_single_spec("fqn:model_a")
    fqn_b = SelectionCriteria.from_single_spec("fqn:model_b")
    fqn_c = SelectionCriteria.from_single_spec("fqn:model_c")
    difference = SelectionUnion(components=[fqn_a, fqn_b, fqn_c])
    combined = difference.combine_selections(
        [{"model_a", "model_b"}, {"model_b", "model_c"}, {"model_d"}]
    )
    assert combined == {"model_a", "model_b", "model_c", "model_d"}
