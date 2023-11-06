import pytest
from dbt.tests.util import run_dbt

# This is for seed tests on dbt-core code, not adapter code


class TestEmptySeed:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"empty_with_header.csv": "a,b,c"}

    def test_empty_seeds(self, project):
        # Should create an empty table and not fail
        results = run_dbt(["seed"])
        assert len(results) == 1
