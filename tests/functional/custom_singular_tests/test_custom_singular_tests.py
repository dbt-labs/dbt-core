import pytest

from pathlib import Path

from dbt.config import RuntimeConfig
from dbt.task.test import TestTask

from dbt.tests.util import (
    run_dbt,
)

# from `test/integration/009_data_test`

models__table_copy = """
{{
    config(
        materialized='table'
    )
}}

select * from {{ this.schema }}.seed
"""

tests__fail_email_is_always_null = """
select *
from {{ ref('table_copy') }}
where email is not null
"""

tests__fail_no_ref = """
select 1
"""

tests__dotted_path_pass_id_not_null = """
{# Same as `pass_id_not_null` but with dots in its name #}

select *
from {{ ref('table_copy') }}
where id is null
"""

tests__pass_id_not_null = """
select *
from {{ ref('table_copy') }}
where id is null
"""

tests__pass_no_ref = """
select 1 limit 0
"""


class FakeArgs:
    def __init__(self):
        self.threads = 1
        self.defer = False
        self.full_refresh = False
        self.models = None
        self.select = None
        self.exclude = None
        self.single_threaded = False
        self.selector_name = None
        self.state = None
        self.defer = None


class ArgsForTest:
    def __init__(self, kwargs):
        self.which = "run"
        self.single_threaded = False
        self.profiles_dir = None
        self.project_dir = None
        self.__dict__.update(kwargs)


class TestCustomGenericTests(object):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql_file(project.test_data_dir / Path("seed_expected.sql"))

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_copy.sql": models__table_copy,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "my_db.my_schema.table_copy.pass_id_not_null.sql": tests__dotted_path_pass_id_not_null,
            "tests__pass_id_not_null.sql": tests__pass_id_not_null,
            "tests__pass_no_ref.sql": tests__pass_no_ref,
            "tests__fail_email_is_always_null.sql": tests__fail_email_is_always_null,
            "tests__fail_no_ref.sql": tests__fail_no_ref,
        }

    def run_data_validations(self, profiles_dir):
        args = FakeArgs()

        kwargs = {
            "profile": None,
            "profiles_dir": profiles_dir,
            "target": None,
        }

        config = RuntimeConfig.from_args(ArgsForTest(kwargs))
        test_task = TestTask(args, config)
        return test_task.run()

    def test_data_tests(self, project, tests, test_config):
        results = run_dbt()
        assert len(results) == 1
        test_results = self.run_data_validations(project.profiles_dir)

        for result in test_results:
            # assert that all deliberately failing tests actually fail
            if "fail" in result.node.name:
                assert result.status == "fail"
                assert not result.skipped
                assert result.failures > 0
            # assert that actual tests pass
            else:
                assert result.status == "pass"
                assert not result.skipped
                assert result.failures == 0

        # check that all tests were run
        assert len(test_results) != 0
        assert len(test_results) == len(tests)
