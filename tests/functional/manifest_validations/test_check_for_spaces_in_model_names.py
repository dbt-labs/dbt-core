import pytest

from dataclasses import dataclass, field
from dbt.cli.main import dbtRunner
from dbt_common.events.base_types import BaseEvent, EventLevel, EventMsg
from dbt.events.types import SpacesInModelNameDeprecation, TotalModelNamesWithSpacesDeprecation
from dbt.tests.util import update_config_file
from typing import Dict, List


@dataclass
class EventCatcher:
    event_to_catch: BaseEvent
    caught_events: List[EventMsg] = field(default_factory=list)

    def catch(self, event: EventMsg):
        if event.info.name == self.event_to_catch.__name__:
            self.caught_events.append(event)


class TestSpacesInModelNamesHappyPath:
    def test_no_warnings_when_no_spaces_in_name(self, project) -> None:
        event_catcher = EventCatcher(SpacesInModelNameDeprecation)
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
        event_catcher = EventCatcher(SpacesInModelNameDeprecation)
        total_catcher = EventCatcher(TotalModelNamesWithSpacesDeprecation)
        runner = dbtRunner(callbacks=[event_catcher.catch, total_catcher.catch])
        runner.invoke(["parse"])

        assert len(total_catcher.caught_events) == 1
        assert len(event_catcher.caught_events) == 1
        event = event_catcher.caught_events[0]
        assert "Model `my model` has spaces in its name. This is deprecated" in event.info.msg
        assert event.info.level == EventLevel.WARN


class TestSpaceInModelNamesWithDebug:
    @pytest.fixture(scope="class")
    def models(self) -> Dict[str, str]:
        return {
            "my model.sql": "select 1 as id",
            "my model2.sql": "select 1 as id",
        }

    def tests_debug_when_spaces_in_name(self, project) -> None:
        spaces_check_catcher = EventCatcher(SpacesInModelNameDeprecation)
        total_catcher = EventCatcher(TotalModelNamesWithSpacesDeprecation)
        runner = dbtRunner(callbacks=[spaces_check_catcher.catch, total_catcher.catch])
        runner.invoke(["parse"])
        assert len(spaces_check_catcher.caught_events) == 1
        assert len(total_catcher.caught_events) == 1
        assert (
            "Spaces in model names found in 2 model(s)" in total_catcher.caught_events[0].info.msg
        )
        assert (
            "Run again with `--debug` to see them all." in total_catcher.caught_events[0].info.msg
        )

        spaces_check_catcher = EventCatcher(SpacesInModelNameDeprecation)
        total_catcher = EventCatcher(TotalModelNamesWithSpacesDeprecation)
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

    def test_dont_allow_spaces_in_model_names(self, project):
        spaces_check_catcher = EventCatcher(SpacesInModelNameDeprecation)
        runner = dbtRunner(callbacks=[spaces_check_catcher.catch])
        runner.invoke(["parse"])
        assert len(spaces_check_catcher.caught_events) == 1
        assert spaces_check_catcher.caught_events[0].info.level == EventLevel.WARN

        config_patch = {"flags": {"allow_spaces_in_model_names": False}}
        update_config_file(config_patch, project.project_root, "dbt_project.yml")

        spaces_check_catcher = EventCatcher(SpacesInModelNameDeprecation)
        runner = dbtRunner(callbacks=[spaces_check_catcher.catch])
        result = runner.invoke(["parse"])
        assert not result.success
        assert "Model names cannot contain spaces" in result.exception.__str__()
        assert len(spaces_check_catcher.caught_events) == 1
        assert spaces_check_catcher.caught_events[0].info.level == EventLevel.ERROR
