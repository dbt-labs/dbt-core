import pytest
from dbt.tests.util import run_dbt

from fixtures import (  # noqa: F401
    my_model_vars_sql,
    my_model_a_sql,
    my_model_b_sql,
    test_my_model_yml,
    datetime_test,
)

class TestUnitTestList:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_vars_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_yml + datetime_test,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"my_test": "my_test_var"}}

    def test_unit_test_list(self, project):
        # make sure things are working
        results = run_dbt(["run"])
        assert len(results) == 3
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 5

        results = run_dbt(["list"])

