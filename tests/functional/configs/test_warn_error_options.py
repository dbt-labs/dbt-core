from typing import Dict, Union

import pytest

from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.events.types import DeprecatedModel
from dbt.flags import get_flags
from dbt.tests.util import run_dbt, update_config_file
from dbt_common.events.base_types import EventLevel
from tests.utils import EventCatcher

ModelsDictSpec = Dict[str, Union[str, "ModelsDictSpec"]]

my_model_sql = """SELECT 1 AS id, 'cats are cute' AS description"""
schema_yml = """
version: 2
models:
  - name: my_model
    deprecation_date: 2020-01-01
"""


@pytest.fixture(scope="class")
def models() -> ModelsDictSpec:
    return {"my_model.sql": my_model_sql, "schema.yml": schema_yml}


@pytest.fixture(scope="function")
def catcher() -> EventCatcher:
    return EventCatcher(event_to_catch=DeprecatedModel)


@pytest.fixture(scope="function")
def runner(catcher: EventCatcher) -> dbtRunner:
    return dbtRunner(callbacks=[catcher.catch])


def assert_deprecation_warning(result: dbtRunnerResult, catcher: EventCatcher) -> None:
    assert result.success
    assert result.exception is None
    assert len(catcher.caught_events) == 1
    assert catcher.caught_events[0].info.level == EventLevel.WARN.value


def assert_deprecation_error(result: dbtRunnerResult) -> None:
    assert not result.success
    assert result.exception is not None
    assert "Model my_model has passed its deprecation date of" in str(result.exception)


class TestWarnErrorOptionsFromCLI:
    def test_can_silence(self, project, catcher: EventCatcher, runner: dbtRunner) -> None:
        result = runner.invoke(["run"])
        assert_deprecation_warning(result, catcher)

        catcher.flush()
        result = runner.invoke(["run", "--warn-error-options", "{'silence': ['DeprecatedModel']}"])
        assert result.success
        assert len(catcher.caught_events) == 0

    def test_can_raise_warning_to_error(
        self, project, catcher: EventCatcher, runner: dbtRunner
    ) -> None:
        result = runner.invoke(["run"])
        assert_deprecation_warning(result, catcher)

        catcher.flush()
        result = runner.invoke(["run", "--warn-error-options", "{'include': ['DeprecatedModel']}"])
        assert_deprecation_error(result)

        catcher.flush()
        result = runner.invoke(["run", "--warn-error-options", "{'include': 'all'}"])
        assert_deprecation_error(result)

        catcher.flush()
        result = runner.invoke(["run", "--warn-error-options", "{'error': ['DeprecatedModel']}"])
        assert_deprecation_error(result)

        catcher.flush()
        result = runner.invoke(["run", "--warn-error-options", "{'error': 'all'}"])
        assert_deprecation_error(result)

    def test_can_exclude_specific_event(
        self, project, catcher: EventCatcher, runner: dbtRunner
    ) -> None:
        result = runner.invoke(["run", "--warn-error-options", "{'include': 'all'}"])
        assert_deprecation_error(result)

        catcher.flush()
        result = runner.invoke(
            ["run", "--warn-error-options", "{'include': 'all', 'exclude': ['DeprecatedModel']}"]
        )
        assert_deprecation_warning(result, catcher)

        catcher.flush()
        result = runner.invoke(
            ["run", "--warn-error-options", "{'include': 'all', 'warn': ['DeprecatedModel']}"]
        )
        assert_deprecation_warning(result, catcher)

    def test_cant_set_both_include_and_error(self, project, runner: dbtRunner) -> None:
        result = runner.invoke(
            ["run", "--warn-error-options", "{'include': 'all', 'error': 'all'}"]
        )
        assert not result.success
        assert result.exception is not None
        assert "Only `error` or `include` can be specified" in str(result.exception)

    def test_cant_set_both_exclude_and_warn(self, project, runner: dbtRunner) -> None:
        result = runner.invoke(
            [
                "run",
                "--warn-error-options",
                "{'include': 'all', 'exclude': ['DeprecatedModel'], 'warn': ['DeprecatedModel']}",
            ]
        )
        assert not result.success
        assert result.exception is not None
        assert "Only `warn` or `exclude` can be specified" in str(result.exception)


class TestWarnErrorOptionsFromProject:
    @pytest.fixture(scope="function")
    def clear_project_flags(self, project_root) -> None:
        flags = {"flags": {}}
        update_config_file(flags, project_root, "dbt_project.yml")

    def test_can_silence(
        self, project, clear_project_flags, project_root, catcher: EventCatcher, runner: dbtRunner
    ) -> None:
        result = runner.invoke(["run"])
        assert_deprecation_warning(result, catcher)

        silence_options = {"flags": {"warn_error_options": {"silence": ["DeprecatedModel"]}}}
        update_config_file(silence_options, project_root, "dbt_project.yml")

        catcher.flush()
        result = runner.invoke(["run"])
        assert result.success
        assert len(catcher.caught_events) == 0

    def test_can_raise_warning_to_error(
        self, project, clear_project_flags, project_root, catcher: EventCatcher, runner: dbtRunner
    ) -> None:
        result = runner.invoke(["run"])
        assert_deprecation_warning(result, catcher)

        include_options = {"flags": {"warn_error_options": {"include": ["DeprecatedModel"]}}}
        update_config_file(include_options, project_root, "dbt_project.yml")

        catcher.flush()
        result = runner.invoke(["run"])
        assert_deprecation_error(result)

        include_options = {"flags": {"warn_error_options": {"include": "all"}}}
        update_config_file(include_options, project_root, "dbt_project.yml")

        catcher.flush()
        result = runner.invoke(["run"])
        assert_deprecation_error(result)

    def test_can_exclude_specific_event(
        self, project, clear_project_flags, project_root, catcher: EventCatcher, runner: dbtRunner
    ) -> None:
        include_options = {"flags": {"warn_error_options": {"include": "all"}}}
        update_config_file(include_options, project_root, "dbt_project.yml")
        result = runner.invoke(["run"])
        assert_deprecation_error(result)

        exclude_options = {
            "flags": {"warn_error_options": {"include": "all", "exclude": ["DeprecatedModel"]}}
        }
        update_config_file(exclude_options, project_root, "dbt_project.yml")

        catcher.flush()
        result = runner.invoke(["run"])
        assert_deprecation_warning(result, catcher)

    def test_cant_set_both_include_and_error(
        self, project, clear_project_flags, project_root, runner: dbtRunner
    ) -> None:
        exclude_options = {"flags": {"warn_error_options": {"include": "all", "error": "all"}}}
        update_config_file(exclude_options, project_root, "dbt_project.yml")
        result = runner.invoke(["run"])
        assert not result.success
        assert result.exception is not None
        assert "Only `error` or `include` can be specified" in str(result.exception)

    def test_cant_set_both_exclude_and_warn(
        self, project, clear_project_flags, project_root, runner: dbtRunner
    ) -> None:
        exclude_options = {
            "flags": {
                "warn_error_options": {
                    "include": "all",
                    "exclude": ["DeprecatedModel"],
                    "warn": ["DeprecatedModel"],
                }
            }
        }
        update_config_file(exclude_options, project_root, "dbt_project.yml")
        result = runner.invoke(["run"])
        assert not result.success
        assert result.exception is not None
        assert "Only `warn` or `exclude` can be specified" in str(result.exception)


class TestEmptyWarnError:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_sql, "schema.yml": schema_yml}

    # This tests for a bug in creating WarnErrorOptions when warn or
    # error are set to None (in yaml =  warn:)
    def test_project_flags(self, project):
        project_flags = {
            "flags": {
                "send_anonymous_usage_stats": False,
                "warn_error_options": {
                    "warn": None,
                    "error": None,
                    "silence": ["TestsConfigDeprecation"],
                },
            }
        }
        update_config_file(project_flags, project.project_root, "dbt_project.yml")
        run_dbt(["run"])
        flags = get_flags()
        # Note: WarnErrorOptions is not a dataclass, so you won't get "silence"
        # from to_dict or stringifying.
        assert flags.warn_error_options.silence == ["TestsConfigDeprecation"]
