from typing import Tuple, Any

import pytest

from dbt.tests.util import run_dbt_and_capture

from tests.adapter.dbt.tests.adapter.materialized_views import (
    test_basic,
    test_on_configuration_change,
)


class TestBasic(test_basic.BasicTestsBase):
    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        super().test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(project)


class TestOnConfigurationChangeSkip(
    test_on_configuration_change.OnConfigurationChangeSkipTestsBase
):
    def apply_configuration_change(self, project) -> Tuple[Any, Any]:
        return run_dbt_and_capture(
            ["run", "--models", self.materialized_view, "--vars", "quoting: {identifier: True}"]
        )

    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_model_is_skipped_with_configuration_changes(self, project):
        super().test_model_is_skipped_with_configuration_changes(project)


class TestOnConfigurationChangeFail(
    test_on_configuration_change.OnConfigurationChangeFailTestsBase
):
    def apply_configuration_change(self, project) -> Tuple[Any, Any]:
        return run_dbt_and_capture(
            ["run", "--models", self.materialized_view, "--vars", "quoting: {identifier: True}"]
        )

    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_run_fails_with_configuration_changes(self, project):
        super().test_run_fails_with_configuration_changes(project)
