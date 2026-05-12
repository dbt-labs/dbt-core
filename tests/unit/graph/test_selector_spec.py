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
    _has_graph_operator,
    _is_unaccompanied_graph_operator,
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


# ── Unaccompanied graph operator detection ────────────────────────────────────


@pytest.mark.parametrize(
    "groupdict,expected",
    [
        # No graph operator at all — not unaccompanied
        ({"childrens_parents": None, "parents": None, "children": None, "value": ""}, False),
        ({"childrens_parents": None, "parents": None, "children": None, "value": "mymodel"}, False),
        # Has operator, empty value — unaccompanied
        ({"childrens_parents": None, "parents": "+", "children": None, "value": ""}, True),
        ({"childrens_parents": "@", "parents": None, "children": None, "value": ""}, True),
        ({"childrens_parents": None, "parents": "1+", "children": None, "value": ""}, True),
        # Has operator, numeric-only value — unaccompanied (looks like a depth modifier)
        ({"childrens_parents": None, "parents": "1+", "children": None, "value": "1"}, True),
        ({"childrens_parents": None, "parents": "+", "children": None, "value": "2"}, True),
        # Has operator, real string value — NOT unaccompanied
        ({"childrens_parents": None, "parents": "+", "children": None, "value": "mymodel"}, False),
        ({"childrens_parents": "@", "parents": None, "children": None, "value": "mymodel"}, False),
        ({"childrens_parents": None, "parents": "2+", "children": "+3", "value": "mymodel"}, False),
        # Children-only with real value — not unaccompanied
        ({"childrens_parents": None, "parents": None, "children": "+", "value": "mymodel"}, False),
        # Children-only with numeric value — unaccompanied
        ({"childrens_parents": None, "parents": None, "children": "+1", "value": "1"}, True),
        # Alphanumeric value with operator — NOT unaccompanied (could be a model named "1abc")
        ({"childrens_parents": None, "parents": "+", "children": None, "value": "1abc"}, False),
    ],
)
def test_is_unaccompanied_graph_operator(groupdict, expected):
    assert _is_unaccompanied_graph_operator(groupdict) == expected


@pytest.mark.parametrize(
    "raw_spec",
    [
        "+",
        "@",
        "1+",
        "1+1",
        "+2",
        "2+3",
    ],
)
def test_from_single_spec_warns_on_unaccompanied_operator(raw_spec):
    """from_single_spec emits a NoNodesForSelectionCriteria warning for bare operators."""
    with patch("dbt.graph.selector_spec.warn_or_error") as mock_warn:
        SelectionCriteria.from_single_spec(raw_spec)
        mock_warn.assert_called_once()
        event = mock_warn.call_args[0][0]
        assert type(event).__name__ == "NoNodesForSelectionCriteria"


@pytest.mark.parametrize(
    "raw_spec",
    [
        "mymodel",
        "+mymodel",
        "mymodel+",
        "@mymodel",
        "2+mymodel+3",
        "tag:my_tag",
        "+tag:my_tag",
        "path/to/models",
        "+path/to/models",
        # model names that start with digits are valid if they contain letters too
        "1abc",
        "+1abc",
    ],
)
def test_from_single_spec_no_warning_for_valid_selectors(raw_spec):
    """from_single_spec does not warn when a graph operator accompanies a real model name."""
    with patch("dbt.graph.selector_spec.warn_or_error") as mock_warn:
        with patch("dbt.graph.selector_spec.get_flags") as patched_flags:
            patched_flags.return_value.INDIRECT_SELECTION = IndirectSelection.Eager
            SelectionCriteria.from_single_spec(raw_spec)
        mock_warn.assert_not_called()
