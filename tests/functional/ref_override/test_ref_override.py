import pytest

from dbt.tests.util import run_dbt, check_relations_equal
from dbt.tests.fixtures.project import write_project_files


models__ref_override_sql = """
select
    *
from {{ ref('seed_1') }}
"""

macros__ref_override_macro_sql = """
-- Macro to override ref and always return the same result
{% macro ref(modelname) %}
{% do return(builtins.ref(modelname).replace_path(identifier='seed_2')) %}
{% endmacro %}
"""

seeds__seed_2_csv = """a,b
6,2
12,4
18,6"""

seeds__seed_1_csv = """a,b
1,2
2,4
3,6"""


@pytest.fixture(scope="class")
def models():
    return {"ref_override.sql": models__ref_override_sql}


@pytest.fixture(scope="class")
def macros():
    return {"ref_override_macro.sql": macros__ref_override_macro_sql}


@pytest.fixture(scope="class")
def seeds():
    return {"seed_2.csv": seeds__seed_2_csv, "seed_1.csv": seeds__seed_1_csv}


@pytest.fixture(scope="class")
def project_files(
    project_root,
    models,
    macros,
    seeds,
):
    write_project_files(project_root, "models", models)
    write_project_files(project_root, "macros", macros)
    write_project_files(project_root, "seeds", seeds)


class TestRefOverride:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seed-paths": ["seeds"],
            "macro-paths": ["macros"],
            "seeds": {
                "quote_columns": False,
            },
        }

    def test_ref_override(
        self,
        project,
    ):
        run_dbt(["seed"])
        run_dbt(["run"])

        # We want it to equal seed_2 and not seed_1. If it's
        # still pointing at seed_1 then the override hasn't worked.
        check_relations_equal(project.adapter, ["ref_override", "seed_2"])
