import pytest

from dbt.contracts.graph.model_config import OnConfigurationChangeOption
from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt_and_capture, relation_from_name

from dbt.tests.adapter.materialized_views.base import Base


class OnConfigurationChangeBase(Base):

    on_configuration_change: OnConfigurationChangeOption

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": str(self.on_configuration_change)}}

    def assert_proper_scenario(
        self,
        results,
        logs,
        status: RunStatus,
        log_message: str,
        rows_affected: int,
    ):
        assert len(results.results) == 1
        result = results.results[0]

        assert result.node.config.on_configuration_change == self.on_configuration_change
        assert result.status == status
        assert result.adapter_response["rows_affected"] == rows_affected
        assert log_message in self.stringify_logs(logs)

    def apply_configuration_change_triggering_apply(self, project):
        raise NotImplementedError(
            (
                "Overwrite this to apply a configuration change that should trigger an `apply` scenario "
                "that's specific to your adapter. Do this by altering the project files.",
            )
        )

    def apply_configuration_change_triggering_full_refresh(self, project):
        raise NotImplementedError(
            (
                "Overwrite this to apply a configuration change that should trigger a `full refresh` scenario "
                "specific to your adapter. Do this by altering the project files.",
            )
        )

    def test_full_refresh_takes_precedence_over_any_configuration_changes(self, project):
        self.apply_configuration_change_triggering_apply(project)
        self.apply_configuration_change_triggering_full_refresh(project)
        results, logs = run_dbt_and_capture(
            ["--debug", "run", "--models", self.materialized_view, "--full-refresh"]
        )
        log_message = (
            f"Applying REPLACE to: {relation_from_name(project.adapter, self.materialized_view)}"
        )
        self.assert_proper_scenario(
            results,
            logs,
            RunStatus.Success,
            log_message,
            -1,
        )
        assert (
            f"Determining configuration changes on: {project.adapter, self.materialized_view}"
            not in self.stringify_logs(logs)
        )

    def test_model_is_refreshed_with_no_configuration_changes(self, project):
        results, logs = run_dbt_and_capture(["--debug", "run", "--models", self.materialized_view])
        log_message = (
            f"Applying REFRESH to: {relation_from_name(project.adapter, self.materialized_view)}"
        )
        self.assert_proper_scenario(
            results,
            logs,
            RunStatus.Success,
            log_message,
            -1,
        )
        assert (
            f"Determining configuration changes on: {relation_from_name(project.adapter, self.materialized_view)}"
            in self.stringify_logs(logs)
        )


class OnConfigurationChangeApplyTestsBase(OnConfigurationChangeBase):

    on_configuration_change = "apply"

    def test_full_refresh_configuration_changes_will_not_attempt_apply_configuration_changes(
        self, project
    ):
        self.apply_configuration_change_triggering_apply(project)
        self.apply_configuration_change_triggering_full_refresh(project)
        results, logs = run_dbt_and_capture(["--debug", "run", "--models", self.materialized_view])
        log_message = (
            f"Applying REPLACE to: {relation_from_name(project.adapter, self.materialized_view)}"
        )
        self.assert_proper_scenario(
            results,
            logs,
            RunStatus.Success,
            log_message,
            -1,
        )
        assert (
            f"Applying ALTER to: {relation_from_name(project.adapter, self.materialized_view)}"
            not in self.stringify_logs(logs)
        )

    def test_model_applies_changes_with_configuration_changes(self, project):
        self.apply_configuration_change_triggering_apply(project)
        results, logs = run_dbt_and_capture(["--debug", "run", "--models", self.materialized_view])
        log_message = (
            f"Applying ALTER to: {relation_from_name(project.adapter, self.materialized_view)}"
        )
        self.assert_proper_scenario(
            results,
            logs,
            RunStatus.Success,
            log_message,
            -1,
        )


class OnConfigurationChangeSkipTestsBase(OnConfigurationChangeBase):

    on_configuration_change = "skip"

    def test_model_is_skipped_with_configuration_changes(self, project):
        self.apply_configuration_change_triggering_apply(project)
        results, logs = run_dbt_and_capture(["--debug", "run", "--models", self.materialized_view])
        log_message = (
            f"Configuration changes were identified and `on_configuration_change` "
            f"was set to `skip` for `{self.materialized_view}`"
        )
        self.assert_proper_scenario(results, logs, RunStatus.Success, log_message, -1)


class OnConfigurationChangeFailTestsBase(OnConfigurationChangeBase):

    on_configuration_change = "fail"

    def test_run_fails_with_configuration_changes(self, project):
        self.apply_configuration_change_triggering_apply(project)
        results, logs = run_dbt_and_capture(["--debug", "run", "--models", self.materialized_view])
        log_message = (
            f"Configuration changes were identified and `on_configuration_change` "
            f"was set to `fail` for `{self.materialized_view}`"
        )
        self.assert_proper_scenario(results, logs, RunStatus.Error, log_message, -1)
