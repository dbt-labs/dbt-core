import pytest

from dbt.tests.adapter.materialized_views.test_basic import BasicTestsBase
from dbt.tests.adapter.materialized_views.test_on_configuration_change import (
    OnConfigurationChangeApplyTestsBase,
    OnConfigurationChangeSkipTestsBase,
    OnConfigurationChangeFailTestsBase,
)


def update_indexes(project, relation):
    """TODO: get the model config file and update it to change the index"""
    pass


class TestBasic(BasicTestsBase):
    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        pass


class TestOnConfigurationChangeApply(OnConfigurationChangeApplyTestsBase):
    def apply_configuration_change_triggering_apply(self, project):
        update_indexes(project, self.materialized_view)

    def apply_configuration_change_triggering_full_refresh(self, project):
        """There are no monitored changes that trigger a full refresh"""
        pass

    @pytest.mark.skip(
        "This fails because there are no monitored changes that trigger a full refresh"
    )
    def test_full_refresh_configuration_changes_will_not_attempt_apply_configuration_changes(
        self, project
    ):
        pass


class TestOnConfigurationChangeSkip(OnConfigurationChangeSkipTestsBase):
    def apply_configuration_change_triggering_apply(self, project):
        update_indexes(project, self.materialized_view)

    def apply_configuration_change_triggering_full_refresh(self, project):
        """There are no monitored changes that trigger a full refresh"""
        pass


class TestOnConfigurationChangeFail(OnConfigurationChangeFailTestsBase):
    def apply_configuration_change_triggering_apply(self, project):
        update_indexes(project, self.materialized_view)

    def apply_configuration_change_triggering_full_refresh(self, project):
        """There are no monitored changes that trigger a full refresh"""
        pass
