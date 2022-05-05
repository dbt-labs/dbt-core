import pytest
from dbt.tests.util import run_dbt, check_relations_equal

from tests.functional.context_methods.first_dependency import FirstDependencyProject

dependency_seeds__root_model_expected_csv = """first_dep_global,from_root
dep_never_overridden,root_root_value
"""

dependency_models__inside__model_sql = """
select
    '{{ var("first_dep_override") }}' as first_dep_global,
    '{{ var("from_root_to_root") }}' as from_root

"""


class TestVarDependencyInheritance(FirstDependencyProject):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"root_model_expected.csv": dependency_seeds__root_model_expected_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"inside": {"model.sql": dependency_models__inside__model_sql}}

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {"local": "first_dependency"},
            ]
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "first_dep_override": "dep_never_overridden",
                "test": {
                    "from_root_to_root": "root_root_value",
                },
                "first_dep": {
                    "from_root_to_first": "root_first_value",
                },
            },
        }

    def test_var_mutual_overrides_v1_conversion(self, project, first_dependency):
        run_dbt(["deps"])
        assert len(run_dbt(["seed"])) == 2
        assert len(run_dbt(["run"])) == 2
        check_relations_equal(project.adapter, ["root_model_expected", "model"])
        check_relations_equal(project.adapter, ["first_dep_expected", "first_dep_model"])
