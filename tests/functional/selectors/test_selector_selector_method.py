from typing import Any

import pytest

from dbt.events.types import SelectExcludeIgnoredWithSelectorWarning
from dbt.exceptions import DbtSelectorsError
from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.exceptions import RecursionError as DbtRecursionError

models__model_a_sql = """
SELECT 1 AS id
"""

models_model_b_sql = """
SELECT 1 AS id
"""

models_model_a_plus_1_sql = """
SELECT * FROM {{ ref('model_a') }}
"""

models_model_a_plus_2_sql = """
SELECT * FROM {{ ref('model_a_plus_1') }}
"""

models_model_c_sql = """
{{ config(tags=['tag_c']) }}

SELECT * FROM {{ ref('model_a') }}
UNION ALL
SELECT * FROM {{ ref('model_b') }}
"""


selectors__yml = """
selectors:
  - name: model_a_selector
    description: Selects model_a
    definition:
      method: fqn
      value: model_a

  - name: model_b_selector
    description: Selects model_b
    definition:
      method: fqn
      value: model_b

  - name: model_c_selector
    description: Selects model_c
    definition:
      method: fqn
      value: model_c

  - name: model_a_b_selector
    description: Selects model_a and model_b
    definition:
      union:
        - method: fqn
          value: model_a
        - method: fqn
          value: model_b

  - name: model_a_plus_1_selector
    description: Selects model_a_plus_1
    definition:
      method: fqn
      value: model_a_plus_1

  - name: model_a_plus_2_selector
    description: Selects model_a_plus_2
    definition:
      method: fqn
      value: model_a_plus_2
      parents: true
      parents_depth: 2

  - name: recursive_selector
    description: Selects recursive models
    definition:
      union:
        - model_b
        - selector:model_a_plus_2_selector

  - name: recursive_with_wildcards
    description: Selects recursive models with wildcards
    definition:
      union:
        - model_c
        - selector:model_[ab]_selector

  - name: circular_selection_hop
    description: Selects circular selection hop models
    definition:
      union:
        - selector:circular_dependency_selector

  - name: circular_dependency_selector
    description: Selects circular dependency models
    definition:
      union:
        - selector:circular_selection_hop

  - name: model_a_explicit_selector
    description: Selects model_a with an explicit selector method
    definition:
      method: selector
      value: model_a_selector
"""


@pytest.fixture(scope="class")
def models():
    return {
        "model_a.sql": models__model_a_sql,
        "model_a_plus_1.sql": models_model_a_plus_1_sql,
        "model_a_plus_2.sql": models_model_a_plus_2_sql,
        "model_b.sql": models_model_b_sql,
        "model_c.sql": models_model_c_sql,
    }


@pytest.fixture(scope="class")
def selectors():
    return selectors__yml


def assert_result_set(actual: Any, expected: set[str]):
    assert isinstance(actual, list)
    assert set(actual) == expected


class TestSelectorSelectorMethod:
    def test_ls_with_selector_returns_model_a(self, project):
        result = run_dbt(["ls", "--select", "selector:model_a_selector"])
        assert_result_set(result, {"test.model_a"})

    def test_ls_with_selector_union(self, project):
        result = run_dbt(["ls", "--select", "selector:model_a_selector selector:model_b_selector"])
        assert_result_set(result, {"test.model_a", "test.model_b"})

    def test_ls_with_selector_intersection(self, project):
        result = run_dbt(
            ["ls", "--select", "selector:model_a_b_selector,selector:model_b_selector"]
        )
        assert_result_set(result, {"test.model_b"})

    def test_ls_with_graph_operator(self, project):
        # one child one parent
        result = run_dbt(["ls", "--select", "1+selector:model_a_plus_1_selector+1"])
        assert_result_set(
            result,
            {
                "test.model_a",
                "test.model_a_plus_1",
                "test.model_a_plus_2",
            },
        )

        # two parents
        result = run_dbt(["ls", "--select", "1+selector:model_c_selector"])
        assert_result_set(result, {"test.model_a", "test.model_b", "test.model_c"})

    def test_selector_depth_overrides_operator_depth(self, project):
        result = run_dbt(["ls", "--select", "selector:model_a_plus_2_selector"])
        assert_result_set(
            result,
            {
                "test.model_a",
                "test.model_a_plus_1",
                "test.model_a_plus_2",
            },
        )

        result = run_dbt(["ls", "--select", "1+selector:model_a_plus_2_selector"])
        assert_result_set(result, {"test.model_a", "test.model_a_plus_1", "test.model_a_plus_2"})

    def test_combine_with_other_methods(self, project):
        result = run_dbt(["ls", "--select", "selector:model_a_selector tag:tag_c"])
        assert_result_set(result, {"test.model_a", "test.model_c"})

        result = run_dbt(["ls", "--select", "selector:model_a_plus_2_selector model_a model_b"])
        assert_result_set(
            result,
            {
                "test.model_a",
                "test.model_b",
                "test.model_a_plus_1",
                "test.model_a_plus_2",
            },
        )

    def test_recursive_selector(self, project):
        result = run_dbt(["ls", "--select", "selector:recursive_selector"])
        assert_result_set(
            result,
            {
                "test.model_a",
                "test.model_b",
                "test.model_a_plus_1",
                "test.model_a_plus_2",
            },
        )

    def test_select_and_exclude(self, project):
        result = run_dbt(
            [
                "ls",
                "--select",
                "1+selector:model_a_plus_1_selector",
                "--exclude",
                "selector:model_a_selector",
            ]
        )
        assert_result_set(result, {"test.model_a_plus_1"})

    def test_wildcards(self, project):
        result = run_dbt(["ls", "--select", "selector:model_?_selector"])
        assert_result_set(result, {"test.model_a", "test.model_b", "test.model_c"})

        result = run_dbt(["ls", "--select", "selector:*_c_*"])
        assert_result_set(result, {"test.model_c"})

        result = run_dbt(["ls", "--select", "selector:model_[ab]_selector"])
        assert_result_set(result, {"test.model_a", "test.model_b"})

        result = run_dbt(["ls", "--select", "selector:model_[a-c]_selector"])
        assert_result_set(result, {"test.model_a", "test.model_b", "test.model_c"})

    def test_recursive_with_wildcards(self, project):
        result = run_dbt(["ls", "--select", "selector:recursive_with_wildcards"])
        assert_result_set(result, {"test.model_a", "test.model_b", "test.model_c"})

    def test_circular_dependency(self, project):
        with pytest.raises(DbtRecursionError):
            run_dbt(["ls", "--select", "selector:circular_dependency_selector"], expect_pass=False)

    def test_selector_with_explicit_selector_method(self, project):
        result = run_dbt(["ls", "--select", "selector:model_a_explicit_selector"])
        assert_result_set(result, {"test.model_a"})

    def test_raises_if_no_selector_matches(self, project):
        with pytest.raises(DbtSelectorsError):
            run_dbt(["ls", "--select", "selector:nonexistent_selector"])


class TestSelectExcludeIgnoredWithSelectorWarning:
    """Test that SelectExcludeIgnoredWithSelectorWarning is raised when CLI is invoked with
    --selector together with --select or --exclude.
    """

    def test_warning_raised_when_selector_and_select(self, project):
        event_catcher = EventCatcher(SelectExcludeIgnoredWithSelectorWarning)
        run_dbt(
            ["ls", "--selector", "model_a_selector", "--select", "model_a"],
            callbacks=[event_catcher.catch],
        )
        assert len(event_catcher.caught_events) == 1

    def test_warning_raised_when_selector_and_exclude(self, project):
        event_catcher = EventCatcher(SelectExcludeIgnoredWithSelectorWarning)
        run_dbt(
            ["ls", "--selector", "model_a_selector", "--exclude", "model_b"],
            callbacks=[event_catcher.catch],
        )
        assert len(event_catcher.caught_events) == 1

    def test_no_warning_when_selector_only(self, project):
        event_catcher = EventCatcher(SelectExcludeIgnoredWithSelectorWarning)
        run_dbt(
            ["ls", "--selector", "model_a_selector"],
            callbacks=[event_catcher.catch],
        )
        assert len(event_catcher.caught_events) == 0

    def test_warning_raised_when_selector_and_select_and_exclude(self, project):
        event_catcher = EventCatcher(SelectExcludeIgnoredWithSelectorWarning)
        run_dbt(
            [
                "ls",
                "--selector",
                "model_a_selector",
                "--select",
                "model_a",
                "--exclude",
                "model_b",
            ],
            callbacks=[event_catcher.catch],
        )
        assert len(event_catcher.caught_events) == 1

    def test_no_warning_when_no_selector(self, project):
        event_catcher = EventCatcher(SelectExcludeIgnoredWithSelectorWarning)
        run_dbt(
            ["ls", "--select", "model_a", "--exclude", "model_b"],
            callbacks=[event_catcher.catch],
        )
        assert len(event_catcher.caught_events) == 0

    def test_all_commands_requiring_selectors_support_selectors(self, project):
        # tests all commands except for clone as clone functional tests are in adapters
        commands = [
            "build",
            ("docs", "generate"),
            "compile",
            ("show", "--select", "selector:model_a_selector"),
            "list",
            "ls",
            "run",
            "seed",
            "snapshot",
            ("source", "freshness"),
            "test",
        ]

        for command in commands:
            if isinstance(command, tuple):
                run_dbt([*command, "--selector", "model_a_explicit_selector"], expect_pass=True)
            else:
                run_dbt([command, "--selector", "model_a_explicit_selector"], expect_pass=True)
