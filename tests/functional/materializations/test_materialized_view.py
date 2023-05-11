import pytest
import yaml

from dbt.tests.util import read_file, write_file

from dbt.tests.adapter.materialized_views.base import Model
from dbt.tests.adapter.materialized_views.test_basic import BasicTestsBase
from dbt.tests.adapter.materialized_views.test_on_configuration_change import (
    OnConfigurationChangeApplyTestsBase,
    OnConfigurationChangeSkipTestsBase,
    OnConfigurationChangeFailTestsBase,
)


_BASE_TABLE = """
{{ config(
    materialized='table',
    indexes=[{'columns': ['id'], 'type': 'hash'}]
) }}
select
    1 as id,
    100 as value
where 0 = 1
"""


class PostgresMixin:

    base_table = Model(name="base_table", definition=_BASE_TABLE, columns=["id", "value"])

    starting_records = [(1, 100)]
    inserted_records = [(2, 200)]

    @pytest.fixture(scope="function")
    def configuration_changes_apply(self, project):
        current_yaml = read_file(project.project_root, "dbt_project.yml")
        config = yaml.safe_load(current_yaml)

        # change the index from `id` to `value`
        config["models"].update({"indexes": [{"columns": ["value"], "type": "hash"}]})

        new_yaml = yaml.safe_dump(config)
        write_file(new_yaml, project.project_root, "dbt_project.yml")

        yield

        write_file(current_yaml, project.project_root, "dbt_project.yml")

    @pytest.fixture(scope="function")
    def configuration_changes_full_refresh(self, project):
        """There are no monitored changes that trigger a full refresh"""
        pass


class TestBasic(BasicTestsBase):
    @pytest.mark.skip("This fails because we are mocking with a traditional view")
    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        pass


class TestOnConfigurationChangeApply(PostgresMixin, OnConfigurationChangeApplyTestsBase):
    @pytest.mark.skip(
        "This fails because there are no monitored changes that trigger a full refresh"
    )
    def test_full_refresh_configuration_changes_will_not_attempt_apply_configuration_changes(
        self, project, configuration_changes_apply, configuration_changes_full_refresh
    ):
        pass


class TestOnConfigurationChangeSkip(PostgresMixin, OnConfigurationChangeSkipTestsBase):
    pass


class TestOnConfigurationChangeFail(PostgresMixin, OnConfigurationChangeFailTestsBase):
    pass
