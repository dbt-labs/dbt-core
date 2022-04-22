import os
from dbt.exceptions import CompilationException
import pytest

from dbt.tests.util import run_dbt
from dbt.tests.fixtures.project import write_project_files
from tests.functional.source_overrides.fixtures import (  # noqa: F401
    dupe_models__schema2_yml,
    dupe_models__schema1_yml,
    local_dependency,
)


class TestSourceOverrideDuplicates:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root, local_dependency):  # noqa: F811
        write_project_files(project_root, "local_dependency", local_dependency)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema2.yml": dupe_models__schema2_yml,
            "schema1.yml": dupe_models__schema1_yml,
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "local": "local_dependency",
                },
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "localdep": {
                    "enabled": False,
                    "keep": {
                        "enabled": True,
                    },
                },
                "quote_columns": False,
            },
            "sources": {
                "localdep": {
                    "my_other_source": {
                        "enabled": False,
                    }
                }
            },
        }

    def test_source_duplicate_overrides(self, project):
        run_dbt(["deps"])
        with pytest.raises(CompilationException) as exc:
            run_dbt(["compile"])

        assert "dbt found two schema.yml entries for the same source named" in str(exc.value)
        assert "one of these files" in str(exc.value)
        schema1_path = os.path.join("models", "schema1.yml")
        schema2_path = os.path.join("models", "schema2.yml")
        assert schema1_path in str(exc.value)
        assert schema2_path in str(exc.value)
