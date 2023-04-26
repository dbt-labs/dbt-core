from typing import Tuple, Any

import pytest

from dbt.contracts.graph.model_config import OnConfigurationChangeOption
from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt_and_capture

from base import Base


class OnConfigurationChangeBase(Base):
    @staticmethod
    def assert_proper_scenario(
        results,
        logs,
        on_configuration_change: OnConfigurationChangeOption,
        status: RunStatus,
        log_message: str,
        rows_affected: int,
    ):
        assert len(results.results) == 1
        result = results.results[0]

        assert result.node.config.on_configuration_change == on_configuration_change
        assert result.status == status
        assert result.adapter_response["rows_affected"] == rows_affected
        assert log_message in logs

    def apply_configuration_change(self, project) -> Tuple[Any, Any]:
        raise NotImplementedError(
            (
                "Overwrite this to apply a configuration change specific to your adapter.",
                "Return the results and logs, e.g. by running `run_dbt_and_capture()`",
            )
        )

    def test_full_refresh_takes_precedence(self, project):
        results, logs = run_dbt_and_capture(
            ["run", "--models", self.materialized_view, "--full-refresh"]
        )
        log_message = f"Applying REPLACE to: {self.materialized_view}"
        self.assert_proper_scenario(
            results,
            logs,
            OnConfigurationChangeOption.skip,
            RunStatus.Success,
            log_message,
            len(self.starting_records),
        )

    def test_model_is_refreshed_with_no_configuration_changes(self, project):
        results, logs = run_dbt_and_capture(
            ["run", "--models", self.materialized_view, "--vars", "quoting: {identifier: True}"],
        )
        log_message = f"Applying REFRESH to: {self.materialized_view}"
        self.assert_proper_scenario(
            results,
            logs,
            OnConfigurationChangeOption.fail,
            RunStatus.Success,
            log_message,
            len(self.starting_records),
        )


class OnConfigurationChangeSkipTestsBase(OnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "skip"}}

    def test_model_is_skipped_with_configuration_changes(self, project):
        results, logs = run_dbt_and_capture(
            ["run", "--models", self.materialized_view, "--vars", "quoting: {identifier: True}"]
        )
        log_message = (
            f"Configuration changes were identified and `on_configuration_change` "
            f"was set to `skip` for `{self.materialized_view}`"
        )
        self.assert_proper_scenario(
            results, logs, OnConfigurationChangeOption.skip, RunStatus.Success, log_message, -1
        )


class OnConfigurationChangeFailTestsBase(OnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"on_configuration_change": "fail"}}

    def test_run_fails_with_configuration_changes(self, project):
        results, logs = run_dbt_and_capture(
            ["run", "--models", self.materialized_view, "--vars", "quoting: {identifier: True}"],
            expect_pass=False,
        )
        log_message = (
            "Configuration changes were identified and `on_configuration_change` was set to `fail`"
        )
        self.assert_proper_scenario(
            results, logs, OnConfigurationChangeOption.fail, RunStatus.Error, log_message, -1
        )
