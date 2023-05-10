import pytest
import yaml

from dbt.tests.util import read_file, write_file

from dbt.tests.adapter.materialized_views.base import Base
from dbt.tests.adapter.materialized_views.test_basic import BasicTestsBase
from dbt.tests.adapter.materialized_views.test_on_configuration_change import (
    OnConfigurationChangeApplyTestsBase,
    OnConfigurationChangeSkipTestsBase,
    OnConfigurationChangeFailTestsBase,
)


@pytest.fixture(scope="function")
def update_indexes(project):
    current_yaml = read_file(project.project_root, "dbt_project.yml")
    config = yaml.safe_load(current_yaml)

    config["models"].update({"indexes": [{"columns": Base.base_table_columns, "type": "hash"}]})

    new_yaml = yaml.safe_dump(config)
    write_file(new_yaml, project.project_root, "dbt_project.yml")

    yield

    write_file(current_yaml, project.project_root, "dbt_project.yml")


class TestBasic(BasicTestsBase):
    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        pass


class TestOnConfigurationChangeApply(OnConfigurationChangeApplyTestsBase):
    @pytest.fixture(scope="function")
    def configuration_changes_apply(self, project, update_indexes):
        pass

    @pytest.fixture(scope="function")
    def configuration_changes_refresh(self, project):
        """There are no monitored changes that trigger a full refresh"""
        pass

    @pytest.mark.skip(
        "This fails because there are no monitored changes that trigger a full refresh"
    )
    def test_full_refresh_configuration_changes_will_not_attempt_apply_configuration_changes(
        self, project, configuration_changes_apply, configuration_changes_full_refresh
    ):
        pass


class TestOnConfigurationChangeSkip(OnConfigurationChangeSkipTestsBase):
    @pytest.fixture(scope="function")
    def configuration_changes_apply(self, project, update_indexes):
        pass

    @pytest.fixture(scope="function")
    def configuration_changes_full_refresh(self, project):
        """There are no monitored changes that trigger a full refresh"""
        pass


class TestOnConfigurationChangeFail(OnConfigurationChangeFailTestsBase):
    @pytest.fixture(scope="function")
    def configuration_changes_apply(self, project, update_indexes):
        pass

    @pytest.fixture(scope="function")
    def configuration_changes_full_refresh(self, project):
        """There are no monitored changes that trigger a full refresh"""
        pass
