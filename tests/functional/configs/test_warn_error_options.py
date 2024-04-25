import pytest

from dbt.cli.main import dbtRunner
from dbt.events.types import DeprecatedModel
from dbt_common.events.base_types import EventLevel
from tests.functional.utils import EventCatcher
from typing import Dict, Union

ModelsDictSpec = Dict[str, Union[str, "ModelsDictSpec"]]

my_model_sql = """SELECT 1 AS id, 'cats are cute' AS description"""
schema_yml = """
version: 2
models:
  - name: my_model
    deprecation_date: 2020-01-01
"""


class TestWarnErrorOptionsFromCLI:
    @pytest.fixture(scope="class")
    def models(self) -> ModelsDictSpec:
        return {"my_model.sql": my_model_sql, "schema.yml": schema_yml}

    @pytest.fixture(scope="function")
    def catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=DeprecatedModel)

    @pytest.fixture(scope="function")
    def runner(self, catcher: EventCatcher) -> dbtRunner:
        return dbtRunner(callbacks=[catcher.catch])

    def test_can_silence(self, project, catcher: EventCatcher, runner: dbtRunner) -> None:
        runner.invoke(["run"])
        assert len(catcher.caught_events) == 1

        catcher.flush()
        runner.invoke(
            ["run", "--warn-error-options", "{'include': 'all', 'silence': ['DeprecatedModel']}"]
        )
        assert len(catcher.caught_events) == 0

    def test_can_raise_warning_to_error(
        self, project, catcher: EventCatcher, runner: dbtRunner
    ) -> None:
        result = runner.invoke(["run"])
        assert result.success
        assert result.exception is None
        assert len(catcher.caught_events) == 1
        assert catcher.caught_events[0].info.level == EventLevel.WARN.value

        catcher.flush()
        result = runner.invoke(["run", "--warn-error-options", "{'include': ['DeprecatedModel']}"])
        assert not result.success
        assert result.exception is not None
        assert "Model my_model has passed its deprecation date of" in str(result.exception)

        catcher.flush()
        result = runner.invoke(["run", "--warn-error-options", "{'include': 'all'}"])
        assert not result.success
        assert result.exception is not None
        assert "Model my_model has passed its deprecation date of" in str(result.exception)

    def test_can_exclude_specific_event(
        self, project, catcher: EventCatcher, runner: dbtRunner
    ) -> None:
        result = runner.invoke(["run", "--warn-error-options", "{'include': 'all'}"])
        assert not result.success
        assert result.exception is not None
        assert "Model my_model has passed its deprecation date of" in str(result.exception)

        catcher.flush()
        result = runner.invoke(
            ["run", "--warn-error-options", "{'include': 'all', exclude: ['DeprecatedModel']}"]
        )
        assert result.success
        assert result.exception is None
        assert len(catcher.caught_events) == 1
        assert catcher.caught_events[0].info.level == EventLevel.WARN.value
