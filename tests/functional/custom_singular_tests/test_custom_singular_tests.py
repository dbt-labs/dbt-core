import pytest

from pathlib import Path

from dbt.tests.util import run_dbt

# from `test/integration/009_data_test`

#
# Models
#

models__table_copy = """
{{
    config(
        materialized='table'
    )
}}

select * from {{ this.schema }}.seed
"""

#
# Tests
#

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


class CustomSingularTestsBase(object):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create seed and downstream model tests are to be run on"""
        project.run_sql_file(project.test_data_dir / Path("seed_expected.sql"))

        results = run_dbt()
        assert len(results) == 1

    @pytest.fixture(scope="class")
    def models(self):
        return {"table_copy.sql": models__table_copy}


class TestPassingTests(CustomSingularTestsBase):
    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "my_db.my_schema.table_copy.pass_id_not_null.sql": tests__dotted_path_pass_id_not_null,
            "tests__pass_id_not_null.sql": tests__pass_id_not_null,
            "tests__pass_no_ref.sql": tests__pass_no_ref,
        }

    def test_data_tests(self, project, tests):
        test_results = run_dbt(["test"])
        assert len(test_results) == len(tests)

        for result in test_results:
            assert result.status == "pass"
            assert not result.skipped
            assert result.failures == 0


class TestFailingTests(CustomSingularTestsBase):
    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "tests__fail_email_is_always_null.sql": tests__fail_email_is_always_null,
            "tests__fail_no_ref.sql": tests__fail_no_ref,
        }

    def test_data_tests(self, project, tests):
        """assert that all deliberately failing tests actually fail"""
        test_results = run_dbt(["test"], expect_pass=False)
        assert len(test_results) == len(tests)

        for result in test_results:
            assert result.status == "fail"
            assert not result.skipped
            assert result.failures > 0
