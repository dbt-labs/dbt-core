from typing import Any, Dict

import pytest

from dbt import deprecations
from dbt.cli.main import dbtRunner
from dbt.events.types import (
    ResourceNamesWithSpacesDeprecation,
    SpacesInResourceNameDeprecation,
)
from dbt.tests.util import update_config_file
from dbt_common.events.base_types import EventLevel
from dbt_common.events.event_catcher import EventCatcher

source_with_space_in_name_schema_yml = """
version: 2

sources:
  - name: raw source
    schema: "{{ target.schema }}"
    tables:
      - name: my_table
"""

source_without_space_schema_yml = """
version: 2

sources:
  - name: raw_source
    schema: "{{ target.schema }}"
    tables:
      - name: my_table
"""

multiple_sources_with_spaces_schema_yml = """
version: 2

sources:
  - name: raw source
    schema: "{{ target.schema }}"
    tables:
      - name: my_table
  - name: another source
    schema: "{{ target.schema }}"
    tables:
      - name: other_table
"""

semantic_model_with_space_in_name_yml = """
version: 2

semantic_models:
  - name: semantic people
    label: "Semantic People"
    model: ref('people')
    dimensions:
      - name: favorite_color
        type: categorical
      - name: created_at
        type: TIME
        type_params:
          time_granularity: day
    measures:
      - name: people
        agg: count
        expr: id
    entities:
      - name: id
        type: primary
    defaults:
      agg_time_dimension: created_at
"""

people_model_sql = """
select 1 as id, 'Drew' as first_name, 'yellow' as favorite_color, 5 as tenure, current_timestamp as created_at
"""

metricflow_time_spine_sql = """
SELECT to_date('02/20/2023', 'mm/dd/yyyy') as date_day
"""


class TestSpacesInSourceNameHappyPath:
    """No warnings when source names have no spaces."""

    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "schema.yml": source_without_space_schema_yml,
        }

    def test_no_warnings_when_no_spaces_in_source_name(self, project) -> None:
        event_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        runner = dbtRunner(callbacks=[event_catcher.catch])
        runner.invoke(["parse"])
        assert len(event_catcher.caught_events) == 0


class TestSpacesInSourceNameWarning:
    """Deprecation warning when source name has spaces and flag is False."""

    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "schema.yml": source_with_space_in_name_schema_yml,
        }

    def test_warning_when_spaces_in_source_name(self, project) -> None:
        deprecations.reset_deprecations()
        event_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        total_catcher = EventCatcher(ResourceNamesWithSpacesDeprecation)
        runner = dbtRunner(callbacks=[event_catcher.catch, total_catcher.catch])
        runner.invoke(["parse"])

        assert len(event_catcher.caught_events) == 1
        msg = event_catcher.caught_events[0].info.msg.replace("\n", " ")
        assert "raw source" in msg
        assert event_catcher.caught_events[0].info.level == EventLevel.WARN
        assert len(total_catcher.caught_events) == 1


class TestSpacesInSourceNameError:
    """Error when source name has spaces and flag is True."""

    @pytest.fixture(scope="class")
    def project_config_update(self) -> Dict[str, Any]:
        return {"flags": {"require_source_and_semantic_model_names_without_spaces": True}}

    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "schema.yml": source_with_space_in_name_schema_yml,
        }

    def test_error_when_spaces_in_source_name_and_flag_true(self, project) -> None:
        event_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        runner = dbtRunner(callbacks=[event_catcher.catch])
        result = runner.invoke(["parse"])
        assert not result.success
        assert "Resource names cannot contain spaces" in str(result.exception)
        assert "raw source" in str(result.exception)
        assert len(event_catcher.caught_events) == 0


class TestSpacesInSemanticModelNameWarning:
    """Deprecation warning when semantic model name has spaces and flag is False."""

    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "people.sql": people_model_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "schema.yml": semantic_model_with_space_in_name_yml,
        }

    def test_warning_when_spaces_in_semantic_model_name(self, project) -> None:
        deprecations.reset_deprecations()
        event_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        total_catcher = EventCatcher(ResourceNamesWithSpacesDeprecation)
        runner = dbtRunner(callbacks=[event_catcher.catch, total_catcher.catch])
        runner.invoke(["parse"])

        assert len(event_catcher.caught_events) == 1
        msg = event_catcher.caught_events[0].info.msg.replace("\n", " ")
        assert "semantic people" in msg
        assert event_catcher.caught_events[0].info.level == EventLevel.WARN
        assert len(total_catcher.caught_events) == 1


class TestSpacesInSemanticModelNameError:
    """Error when semantic model name has spaces and flag is True."""

    @pytest.fixture(scope="class")
    def project_config_update(self) -> Dict[str, Any]:
        return {"flags": {"require_source_and_semantic_model_names_without_spaces": True}}

    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "people.sql": people_model_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "schema.yml": semantic_model_with_space_in_name_yml,
        }

    def test_error_when_spaces_in_semantic_model_name_and_flag_true(self, project) -> None:
        event_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        runner = dbtRunner(callbacks=[event_catcher.catch])
        result = runner.invoke(["parse"])
        assert not result.success
        assert "Resource names cannot contain spaces" in str(result.exception)
        assert "semantic people" in str(result.exception)
        assert len(event_catcher.caught_events) == 0


class TestMultipleSourcesWithSpacesDebug:
    """When multiple sources have spaces, debug mode shows all; non-debug shows first + count."""

    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "schema.yml": multiple_sources_with_spaces_schema_yml,
        }

    def test_debug_shows_all_spaces_in_source_names(self, project) -> None:
        config_patch = {"flags": {"require_source_and_semantic_model_names_without_spaces": False}}
        update_config_file(config_patch, project.project_root, "dbt_project.yml")

        # Without debug: only first source name shown, summary has count
        deprecations.reset_deprecations()
        spaces_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        total_catcher = EventCatcher(ResourceNamesWithSpacesDeprecation)
        runner = dbtRunner(callbacks=[spaces_catcher.catch, total_catcher.catch])
        runner.invoke(["parse"])
        assert len(spaces_catcher.caught_events) == 1
        assert len(total_catcher.caught_events) == 1
        assert "Spaces found in 2 resource name(s)" in total_catcher.caught_events[0].info.msg
        assert (
            "Run again with `--debug` to see them all." in total_catcher.caught_events[0].info.msg
        )

        # With debug: all source names shown
        deprecations.reset_deprecations()
        spaces_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        total_catcher = EventCatcher(ResourceNamesWithSpacesDeprecation)
        runner = dbtRunner(callbacks=[spaces_catcher.catch, total_catcher.catch])
        runner.invoke(["parse", "--debug"])
        assert len(spaces_catcher.caught_events) == 2
        assert len(total_catcher.caught_events) == 1
        assert (
            "Run again with `--debug` to see them all."
            not in total_catcher.caught_events[0].info.msg
        )
