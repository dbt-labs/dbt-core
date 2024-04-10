import pytest

from dataclasses import dataclass, field
from dbt.cli.main import dbtRunner
from dbt_common.events.base_types import EventLevel, EventMsg
from dbt.events.types import SpacesInModelNameDeprecation
from typing import Dict, List


@dataclass
class EventCatcher:
    caught_events: List[EventMsg] = field(default_factory=list)
    SpacesInModelNameDeprecation.__name__

    def catch(self, event: EventMsg):
        if event.info.name == SpacesInModelNameDeprecation.__name__:
            self.caught_events.append(event)


class TestSpacesInModelNamesHappyPath:
    def test_no_warnings_when_no_spaces_in_name(self, project) -> None:
        event_catcher = EventCatcher()
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
        event_catcher = EventCatcher()
        runner = dbtRunner(callbacks=[event_catcher.catch])
        runner.invoke(["parse"])

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
        event_catcher = EventCatcher()
        runner = dbtRunner(callbacks=[event_catcher.catch])
        runner.invoke(["parse"])
        assert len(event_catcher.caught_events) == 1

        event_catcher = EventCatcher()
        runner = dbtRunner(callbacks=[event_catcher.catch])
        runner.invoke(["parse", "--debug"])
        assert len(event_catcher.caught_events) == 2
