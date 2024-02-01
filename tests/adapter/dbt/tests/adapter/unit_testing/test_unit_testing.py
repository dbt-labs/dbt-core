import pytest

from dbt.adapters.postgres.relation_configs import MAX_CHARACTERS_IN_IDENTIFIER
from dbt.tests.util import run_dbt, write_file

my_model_a_sql = """
SELECT
1 as a,
1 as id,
2 as not_testing,
'a' as string_a,
DATE '2020-01-02' as date_a
"""

test_model_a_long_test_name_yml = """
unit_tests:
  - name: {test_name}
    model: my_model_a
    given: []
    expect:
      rows:
        - {{a: 1, id: 1, not_testing: 2, string_a: "a", date_a: "2020-01-02"}}
"""


class BaseUnitTestLongTestName:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_a.sql": my_model_a_sql,
            "test_model_a.yml": test_model_a_long_test_name_yml,
        }

    @pytest.fixture
    def max_unit_test_name_length(self) -> int:
        return -1

    def test_long_unit_test_name(self, project, max_unit_test_name_length):
        # max test name == passing unit test
        write_file(
            test_model_a_long_test_name_yml.format(test_name="a" * max_unit_test_name_length),
            "models",
            "test_model_a.yml",
        )
        results = run_dbt(["run"])
        assert len(results) == 1

        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 1

        # max test name == failing command
        write_file(
            test_model_a_long_test_name_yml.format(
                test_name="a" * (max_unit_test_name_length + 1)
            ),
            "models",
            "test_model_a.yml",
        )

        results = run_dbt(["run"])
        assert len(results) == 1

        run_dbt(["test"], expect_pass=False)


class TestPostgresUnitTestLongTestNames(BaseUnitTestLongTestName):
    @pytest.fixture
    def max_unit_test_name_length(self) -> int:
        return MAX_CHARACTERS_IN_IDENTIFIER
