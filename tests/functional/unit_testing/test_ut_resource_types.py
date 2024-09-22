import pytest
from fixtures import (  # noqa: F401
    my_model_a_sql,
    my_model_b_sql,
    my_model_sql,
    test_my_model_a_yml,
    test_my_model_pass_yml,
)

from dbt.tests.util import run_dbt

EXPECTED_MODELS = [
    "test.my_model",
    "test.my_model_a",
    "test.my_model_b",
]

EXPECTED_DATA_TESTS = [
    "test.not_null_my_model_a_a",
    "test.not_null_my_model_a_id",
]

EXPECTED_UNIT_TESTS = [
    "unit_test:test.test_my_model",
]


class TestUnitTestResourceTypes:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_pass_yml,
            "test_my_model_a.yml": test_my_model_a_yml,
        }

    def test_unit_test_list(self, project):
        results = run_dbt(["run"])

        # unit tests
        results = run_dbt(["list", "--resource-type", "unit_test"])
        assert sorted(results) == EXPECTED_UNIT_TESTS

        results = run_dbt(["list", "--exclude-resource-types", "model", "test"])
        assert sorted(results) == EXPECTED_UNIT_TESTS

        results = run_dbt(["test", "--resource-type", "unit_test"])
        assert len(results) == len(EXPECTED_UNIT_TESTS)

        results = run_dbt(["test", "--exclude-resource-types", "model", "test"])
        assert len(results) == len(EXPECTED_UNIT_TESTS)

        # data tests
        results = run_dbt(["list", "--resource-type", "test"])
        assert sorted(results) == EXPECTED_DATA_TESTS

        results = run_dbt(["list", "--exclude-resource-types", "unit_test", "model"])
        assert sorted(results) == EXPECTED_DATA_TESTS

        results = run_dbt(["test", "--resource-type", "test"])
        assert len(results) == len(EXPECTED_DATA_TESTS)

        results = run_dbt(["test", "--exclude-resource-types", "unit_test", "model"])
        assert len(results) == len(EXPECTED_DATA_TESTS)

        results = run_dbt(["build", "--resource-type", "test"])
        assert len(results) == len(EXPECTED_DATA_TESTS)

        results = run_dbt(["build", "--exclude-resource-types", "unit_test", "model"])
        assert len(results) == len(EXPECTED_DATA_TESTS)

        # models
        results = run_dbt(["list", "--resource-type", "model"])
        assert sorted(results) == EXPECTED_MODELS

        results = run_dbt(["list", "--exclude-resource-type", "unit_test", "test"])
        assert sorted(results) == EXPECTED_MODELS

        results = run_dbt(["test", "--resource-type", "model"])
        assert len(results) == 0

        results = run_dbt(["test", "--exclude-resource-types", "unit_test", "test"])
        assert len(results) == 0

        results = run_dbt(["build", "--resource-type", "model"])
        assert len(results) == len(EXPECTED_MODELS)

        results = run_dbt(["build", "--exclude-resource-type", "unit_test", "test"])
        assert len(results) == len(EXPECTED_MODELS)
