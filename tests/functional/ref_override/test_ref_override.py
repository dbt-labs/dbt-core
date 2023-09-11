import pytest

from dbt.tests.util import run_dbt, check_relations_equal


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


class TestRefOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {"ref_override.sql": models__ref_override_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"ref_override_macro.sql": macros__ref_override_macro_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_2.csv": seeds__seed_2_csv, "seed_1.csv": seeds__seed_1_csv}

    def test_ref_override(
        self,
        project,
    ):
        run_dbt(["seed"])
        run_dbt(["run"])

        # We want it to equal seed_2 and not seed_1. If it's
        # still pointing at seed_1 then the override hasn't worked.
        check_relations_equal(project.adapter, ["ref_override", "seed_2"])


models__version_ref_override_sql = """
select
    *
from {{ ref('versioned_model', version=1) }}
"""

models__package_ref_override_sql = """
select
    *
from {{ ref('package', 'versioned_model') }}
"""

models__package_version_ref_override_sql = """
select
    *
from {{ ref('package', 'versioned_model', version=1) }}
"""

models__v1_sql = """
select 1
"""

models__v2_sql = """
select 2
"""

schema__versions_yml = """
models:
  - name: versioned_model
    versions:
      - v: 1
      - v: 2
"""

macros__package_version_ref_override_macro_sql = """
-- Macro to override ref and always return the same result
{% macro ref() %}
-- extract user-provided positional and keyword arguments
{% set version = kwargs.get('version') %}
{% set packagename = none %}
{%- if (varargs | length) == 1 -%}
    {% set modelname = varargs[0] %}
{%- else -%}
    {% set packagename = varargs[0] %}
    {% set modelname = varargs[1] %}
{% endif %}

{%- set version_override = 2 -%}
{%- set packagename_override = 'test' -%}
-- call builtins.ref based on provided positional arguments
{% if packagename is not none %}
    {% do return(builtins.ref(packagename_override, modelname, version=version_override)) %}
{% else %}
    {% do return(builtins.ref(modelname, version=version_override)) %}
{% endif %}

{% endmacro %}
"""


class TestAdvancedRefOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "version_ref_override.sql": models__version_ref_override_sql,
            "package_ref_override.sql": models__package_ref_override_sql,
            "package_version_ref_override.sql": models__package_version_ref_override_sql,
            "versioned_model_v1.sql": models__v1_sql,
            "versioned_model_v2.sql": models__v2_sql,
            "model.sql": models__v1_sql,
            "schema.yml": schema__versions_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"ref_override_macro.sql": macros__package_version_ref_override_macro_sql}

    def test_ref_override(
        self,
        project,
    ):
        run_dbt(["run"])

        # We want versioned_ref_override to equal to versioned_model_v2, otherwise the
        # ref override macro has not worked
        check_relations_equal(project.adapter, ["version_ref_override", "versioned_model_v2"])

        check_relations_equal(project.adapter, ["package_ref_override", "versioned_model_v2"])

        check_relations_equal(
            project.adapter, ["package_version_ref_override", "versioned_model_v2"]
        )
