import pytest

from dbt.tests.adapter.materialized_views import (
    test_basic,
    test_on_configuration_change,
)


class TestBasic(test_basic.BasicTestsBase):
    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        super().test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(project)


class TestOnConfigurationChangeApply(
    test_on_configuration_change.OnConfigurationChangeApplyTestsBase
):
    def apply_configuration_change_triggering_apply(self, project):
        pass

    def apply_configuration_change_triggering_full_refresh(self, project):
        pass

    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_full_refresh_configuration_changes_will_not_attempt_apply_configuration_changes(
        self, project
    ):
        super().test_full_refresh_configuration_changes_will_not_attempt_apply_configuration_changes(
            project
        )

    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_model_applies_changes_with_configuration_changes(self, project):
        super().test_model_applies_changes_with_configuration_changes(project)


class TestOnConfigurationChangeSkip(
    test_on_configuration_change.OnConfigurationChangeSkipTestsBase
):
    def apply_configuration_change_triggering_apply(self, project):
        pass

    def apply_configuration_change_triggering_full_refresh(self, project):
        pass

    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_model_is_skipped_with_configuration_changes(self, project):
        super().test_model_is_skipped_with_configuration_changes(project)


class TestOnConfigurationChangeFail(
    test_on_configuration_change.OnConfigurationChangeFailTestsBase
):
    def apply_configuration_change_triggering_apply(self, project):
        pass

    def apply_configuration_change_triggering_full_refresh(self, project):
        pass

    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_run_fails_with_configuration_changes(self, project):
        super().test_run_fails_with_configuration_changes(project)
