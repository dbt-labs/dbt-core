import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture
from dbt.exceptions import DuplicateResourceNameError

from fixtures import (
    my_model_a_sql,
    my_model_b_sql,
    test_model_a_b_yml,
    test_model_a_with_duplicate_test_name_yml,
)


class TestUnitTestDuplicateTestNamesAcrossModels:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_model_a_b.yml": test_model_a_b_yml,
        }

    def test_duplicate_test_names_across_models(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        # Select duplicate tests
        results, log_output = run_dbt_and_capture(["test"], expect_pass=True)
        assert len(results) == 2
        assert ["my_model_a", "my_model_b"] == sorted([result.node.model for result in results])
        assert "my_model_a::my_test_name" in log_output
        assert "my_model_b::my_test_name" in log_output

        # Test select duplicates by by test name
        results = run_dbt(["test", "--select", "test_name:my_test_name"])
        assert len(results) == 2
        assert ["my_model_a", "my_model_b"] == sorted([result.node.model for result in results])
        assert "my_model_a::my_test_name" in log_output
        assert "my_model_b::my_test_name" in log_output

        results = run_dbt(["test", "--select", "my_model_a,test_name:my_test_name"])
        assert len(results) == 1
        assert results[0].node.model == "my_model_a"

        results = run_dbt(["test", "--select", "my_model_b,test_name:my_test_name"])
        assert len(results) == 1
        assert results[0].node.model == "my_model_b"

        # Test select by model name
        results = run_dbt(["test", "--select", "my_model_a"])
        assert len(results) == 1
        assert results[0].node.model == "my_model_a"

        results = run_dbt(["test", "--select", "my_model_b"])
        assert len(results) == 1
        assert results[0].node.model == "my_model_b"


class TestUnitTestDuplicateTestNamesWithinModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_a.sql": my_model_a_sql,
            "test_model_a.yml": test_model_a_with_duplicate_test_name_yml,
        }

    def test_duplicate_test_names_within_model(self, project):
        with pytest.raises(DuplicateResourceNameError):
            run_dbt(["run"])


test_model_a_long_test_name_yml = """
unit_tests:
  - name: my_very_reasonable_but_kind_of_long_test_name_for_model_a
    model: my_model_a
    given: []
    expect:
      rows:
        - {a: 1, id: 1, not_testing: 2, string_a: "a", date_a: "2020-01-02"}
"""


class TestUnitTestLongTestNames:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_a.sql": my_model_a_sql,
            "test_model_a.yml": test_model_a_long_test_name_yml,
        }

    def test_long_unit_test_name(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 1
        assert len(results[0].node.name) >= 50
