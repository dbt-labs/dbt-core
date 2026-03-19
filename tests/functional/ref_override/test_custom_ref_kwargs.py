import pytest

from dbt.tests.util import run_dbt

# Custom ref macro that accepts and ignores a 'label' kwarg
macros__custom_ref_sql = """
{% macro ref() %}
{% set label = kwargs.get('label') %}
{% set version = kwargs.get('version') or kwargs.get('v') %}
{% set packagename = none %}
{%- if (varargs | length) == 1 -%}
    {% set modelname = varargs[0] %}
{%- else -%}
    {% set packagename = varargs[0] %}
    {% set modelname = varargs[1] %}
{% endif %}

{% set rel = None %}
{% if packagename is not none %}
    {% set rel = builtins.ref(packagename, modelname, version=version) %}
{% else %}
    {% set rel = builtins.ref(modelname, version=version) %}
{% endif %}

{% do return(rel) %}
{% endmacro %}
"""

models__model_a_sql = """
select 1 as id, 'alice' as name
"""

models__model_b_sql = """
select * from {{ ref('model_a', label='staging') }}
"""

models__schema_yml = """
models:
  - name: model_a
  - name: model_b
    columns:
      - name: id
        data_tests:
          - relationships:
              to: ref('model_a', label='staging')
              field: id

unit_tests:
  - name: my_unit_test
    model: model_b
    given:
      - input: ref('model_a', label='staging')
        rows:
          - {id: 1, name: 'alice'}
    expect:
      rows:
        - {id: 1, name: 'alice'}
"""

# Separate schema with only unit test for the "without flag" test
models__schema_unit_only_yml = """
models:
  - name: model_a
  - name: model_b

unit_tests:
  - name: my_unit_test
    model: model_b
    given:
      - input: ref('model_a', label='staging')
        rows:
          - {id: 1, name: 'alice'}
    expect:
      rows:
        - {id: 1, name: 'alice'}
"""


class TestCustomRefKwargsUnitTest:
    """Test that unit tests work with custom ref kwargs when flag is enabled."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "support_custom_ref_kwargs": True,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": models__model_a_sql,
            "model_b.sql": models__model_b_sql,
            "schema.yml": models__schema_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"ref.sql": macros__custom_ref_sql}

    def test_unit_test_with_custom_ref_kwargs(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 1
        assert results[0].status == "pass"


class TestCustomRefKwargsGenericTest:
    """Test that generic data tests work with custom ref kwargs when flag is enabled."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "support_custom_ref_kwargs": True,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": models__model_a_sql,
            "model_b.sql": models__model_b_sql,
            "schema.yml": models__schema_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"ref.sql": macros__custom_ref_sql}

    def test_generic_test_with_custom_ref_kwargs(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test", "--select", "test_type:data"])
        assert len(results) == 1
        assert results[0].status == "pass"


class TestCustomRefKwargsWithoutFlag:
    """Test that custom ref kwargs in unit tests fail without the behavior flag."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": models__model_a_sql,
            "model_b.sql": models__model_b_sql,
            "schema.yml": models__schema_unit_only_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"ref.sql": macros__custom_ref_sql}

    def test_unit_test_fails_without_flag(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        results = run_dbt(["test", "--select", "test_type:unit"], expect_pass=False)
        assert len(results) == 1
        assert (
            "Unit test given inputs must be either a 'ref', 'source' or 'this' call"
            in results.results[0].message
        )
