import pytest

from dbt.tests.util import run_dbt
from dbt.exceptions import CompilationException

seeds__seed_csv = """id,value
4,2
"""


class TestUnusedModelConfigs:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "test-paths": ["does-not-exist"],
            "models": {
                "test": {
                    "enabled": True,
                }
            },
            "seeds": {
                "quote_columns": False,
            },
            "sources": {
                "test": {
                    "enabled": True,
                }
            },
            "tests": {
                "test": {
                    "enabled": True,
                }
            },
        }

    def test_warn_unused_configuration_paths(
        self,
        project,
    ):
        with pytest.raises(CompilationException) as excinfo:
            run_dbt(["--warn-error", "seed"])

        assert "Configuration paths exist" in str(excinfo.value)
        assert "- sources.test" in str(excinfo.value)
        assert "- models.test" in str(excinfo.value)
        assert "- models.test" in str(excinfo.value)

        run_dbt(["seed"])
