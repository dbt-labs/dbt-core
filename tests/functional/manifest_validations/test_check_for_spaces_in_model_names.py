from typing import Dict

import pytest

from dbt import deprecations
from dbt.cli.main import dbtRunner
from dbt.events.types import (
    ResourceNamesWithSpacesDeprecation,
    SpacesInResourceNameDeprecation,
)
from dbt.tests.util import update_config_file
from dbt_common.events.base_types import EventLevel
from tests.utils import EventCatcher


class TestSpacesInModelNamesHappyPath:
    def test_no_warnings_when_no_spaces_in_name(self, project) -> None:
        event_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        runner = dbtRunner(callbacks=[event_catcher.catch])
        runner.invoke(["parse"])
        assert len(event_catcher.caught_events) == 0


class TestSpacesInModelNamesSadPath:
    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "my model.sql": "select 1 as id",
        }

    def tests_warning_when_spaces_in_name(self, project) -> None:
        event_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        total_catcher = EventCatcher(ResourceNamesWithSpacesDeprecation)
        runner = dbtRunner(callbacks=[event_catcher.catch, total_catcher.catch])
        runner.invoke(["parse"])

        assert len(total_catcher.caught_events) == 1
        assert len(event_catcher.caught_events) == 1
        event = event_catcher.caught_events[0]
        assert "Found spaces in the name of `model.test.my model`" in event.info.msg
        assert event.info.level == EventLevel.WARN


class TestSpaceInModelNamesWithDebug:
    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "my model.sql": "select 1 as id",
            "my model2.sql": "select 1 as id",
        }

    def tests_debug_when_spaces_in_name(self, project) -> None:
        deprecations.reset_deprecations()
        spaces_check_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        total_catcher = EventCatcher(ResourceNamesWithSpacesDeprecation)
        runner = dbtRunner(callbacks=[spaces_check_catcher.catch, total_catcher.catch])
        runner.invoke(["parse"])
        assert len(spaces_check_catcher.caught_events) == 1
        assert len(total_catcher.caught_events) == 1
        assert "Spaces found in 2 resource name(s)" in total_catcher.caught_events[0].info.msg
        assert (
            "Run again with `--debug` to see them all." in total_catcher.caught_events[0].info.msg
        )

        deprecations.reset_deprecations()
        spaces_check_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        total_catcher = EventCatcher(ResourceNamesWithSpacesDeprecation)
        runner = dbtRunner(callbacks=[spaces_check_catcher.catch, total_catcher.catch])
        runner.invoke(["parse", "--debug"])
        assert len(spaces_check_catcher.caught_events) == 2
        assert len(total_catcher.caught_events) == 1
        assert (
            "Run again with `--debug` to see them all."
            not in total_catcher.caught_events[0].info.msg
        )


class TestAllowSpacesInModelNamesFalse:
    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "my model.sql": "select 1 as id",
        }

    def test_require_resource_names_without_spaces(self, project):
        spaces_check_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        runner = dbtRunner(callbacks=[spaces_check_catcher.catch])
        runner.invoke(["parse"])
        assert len(spaces_check_catcher.caught_events) == 1
        assert spaces_check_catcher.caught_events[0].info.level == EventLevel.WARN

        config_patch = {"flags": {"require_resource_names_without_spaces": True}}
        update_config_file(config_patch, project.project_root, "dbt_project.yml")

        spaces_check_catcher = EventCatcher(SpacesInResourceNameDeprecation)
        runner = dbtRunner(callbacks=[spaces_check_catcher.catch])
        result = runner.invoke(["parse"])
        assert not result.success
        assert "Resource names cannot contain spaces" in result.exception.__str__()
        assert len(spaces_check_catcher.caught_events) == 1
        assert spaces_check_catcher.caught_events[0].info.level == EventLevel.ERROR
