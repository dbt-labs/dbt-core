from unittest.mock import MagicMock, patch

import pytest

from dbt.graph.selector_spec import SelectionCriteria
from dbt.task.compile import CompileTask


def _make_task(select):
    """Build a CompileTask with just enough attached to drive
    _get_directly_selected_unique_ids without going through __init__."""
    task = CompileTask.__new__(CompileTask)
    task.args = MagicMock(select=select, exclude=None)
    return task


def _selector_returning(per_value):
    """A fake NodeSelector whose select_included returns the unique_ids
    pre-configured per criterion value. graph.nodes() returns a sentinel
    we assert is forwarded unchanged."""
    selector = MagicMock()
    selector.graph.nodes.return_value = {"sentinel"}

    def fake_select_included(nodes, criteria):
        assert nodes == {"sentinel"}, "graph.nodes() result must be forwarded"
        return set(per_value.get(criteria.value, ()))

    selector.select_included.side_effect = fake_select_included
    return selector


class TestGetDirectlySelectedUniqueIds:
    """The helper resolves `--select` to the set of `unique_id`s the user
    anchored on, without applying graph operator (+/@) expansion. It must
    use `select_included` (the no-expansion path), not
    `get_nodes_from_criteria` (which re-expands and would defeat the point)."""

    @pytest.mark.parametrize("select", [None, (), []])
    def test_empty_selection_returns_empty_set(self, select):
        task = _make_task(select)
        with patch.object(CompileTask, "get_node_selector") as get_selector:
            assert task._get_directly_selected_unique_ids() == set()
            # No selector work should happen when there's nothing to resolve.
            get_selector.assert_not_called()

    def test_bare_name_selector(self):
        task = _make_task(("my_model",))
        selector = _selector_returning({"my_model": ["model.test.my_model"]})
        with patch.object(CompileTask, "get_node_selector", return_value=selector):
            assert task._get_directly_selected_unique_ids() == {"model.test.my_model"}
        # select_included is the no-operator-expansion path the helper uses.
        selector.select_included.assert_called_once()
        selector.get_nodes_from_criteria.assert_not_called()

    @pytest.mark.parametrize(
        "raw,flag",
        [
            ("+my_model", "parents"),
            ("my_model+", "children"),
            ("@my_model", "childrens_parents"),
        ],
    )
    def test_graph_operator_is_parsed_but_not_applied(self, raw, flag):
        """Each graph operator parses into its own flag on SelectionCriteria,
        but select_included ignores those flags — so the helper returns only
        the anchor, never the operator-expanded neighbors."""
        task = _make_task((raw,))
        selector = _selector_returning({"my_model": ["model.test.my_model"]})
        with patch.object(CompileTask, "get_node_selector", return_value=selector):
            result = task._get_directly_selected_unique_ids()

        assert result == {"model.test.my_model"}
        passed_criteria = selector.select_included.call_args.args[1]
        assert isinstance(passed_criteria, SelectionCriteria)
        assert passed_criteria.value == "my_model"
        assert getattr(passed_criteria, flag) is True  # operator parsed
        # …but never expanded — get_nodes_from_criteria is the expansion path.
        selector.get_nodes_from_criteria.assert_not_called()

    def test_multiple_selectors_unioned(self):
        task = _make_task(("a", "b"))
        selector = _selector_returning(
            {"a": ["model.test.a"], "b": ["model.test.b1", "model.test.b2"]}
        )
        with patch.object(CompileTask, "get_node_selector", return_value=selector):
            assert task._get_directly_selected_unique_ids() == {
                "model.test.a",
                "model.test.b1",
                "model.test.b2",
            }
        assert selector.select_included.call_count == 2

    def test_method_selector_can_match_multiple(self):
        """`tag:foo` resolves to every tagged node — all of them are anchors.
        Verifies the method prefix survives parsing (the dropped-method case
        is what made `dbt show --select tag:foo` filter out every result)."""
        task = _make_task(("tag:foo",))
        selector = _selector_returning({"foo": ["model.test.a", "model.test.b"]})
        with patch.object(CompileTask, "get_node_selector", return_value=selector):
            assert task._get_directly_selected_unique_ids() == {
                "model.test.a",
                "model.test.b",
            }
        passed_criteria = selector.select_included.call_args.args[1]
        assert passed_criteria.method == "tag"
        assert passed_criteria.value == "foo"

    def test_versioned_model_passed_through(self):
        task = _make_task(("my_model.v1",))
        selector = _selector_returning({"my_model.v1": ["model.test.my_model.v1"]})
        with patch.object(CompileTask, "get_node_selector", return_value=selector):
            assert task._get_directly_selected_unique_ids() == {"model.test.my_model.v1"}

    def test_no_match_returns_empty(self):
        task = _make_task(("nonexistent",))
        selector = _selector_returning({})  # select_included returns set() for anything
        with patch.object(CompileTask, "get_node_selector", return_value=selector):
            assert task._get_directly_selected_unique_ids() == set()

    def test_intersection_selector_comma_split(self):
        """Comma-separated selectors like ``state:modified+,+state:modified``
        must be split on the comma before being passed to from_single_spec.
        Regression test for #12937 — without splitting, the raw string is
        parsed as a single criterion with value='modified+,+state:modified'
        which raises in StateSelectorMethod.search."""
        task = _make_task(("state:modified+,+state:modified",))
        selector = MagicMock()
        selector.graph.nodes.return_value = {"model.test.a", "model.test.b", "model.test.c"}

        def fake_select_included(nodes, criteria):
            if criteria.value == "modified" and criteria.children:
                return {"model.test.a", "model.test.b"}
            elif criteria.value == "modified" and criteria.parents:
                return {"model.test.b", "model.test.c"}
            return set()

        selector.select_included.side_effect = fake_select_included
        with patch.object(CompileTask, "get_node_selector", return_value=selector):
            result = task._get_directly_selected_unique_ids()

        # Intersection of {a, b} and {b, c} = {b}
        assert result == {"model.test.b"}
        assert selector.select_included.call_count == 2

    def test_space_separated_selectors_unioned(self):
        """Space-separated tokens within a single --select entry are unioned,
        mirroring parse_union semantics."""
        task = _make_task(("model_a model_b",))
        selector = _selector_returning(
            {"model_a": ["model.test.a"], "model_b": ["model.test.b"]}
        )
        with patch.object(CompileTask, "get_node_selector", return_value=selector):
            result = task._get_directly_selected_unique_ids()
        assert result == {"model.test.a", "model.test.b"}
