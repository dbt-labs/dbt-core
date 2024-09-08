import pytest

from dbt.tests.util import run_dbt

models__schema_yml = """
version: 2

sources:
  - name: src
    schema: "{{ target.schema }}"
    freshness:
      warn_after: {count: 24, period: hour}
    loaded_at_field: _loaded_at
    tables:
      - name: source_a
        identifier: model_c
        columns:
          - name: fun
          - name: _loaded_at
  - name: src
    schema: "{{ target.schema }}"
    freshness:
      warn_after: {count: 24, period: hour}
    loaded_at_field: _loaded_at
    tables:
      - name: source_b
        identifier: model_c
        columns:
          - name: fun
          - name: _loaded_at

models:
  - name: model_a
    columns:
      - name: fun
        tags: [marketing]
  - name: model_b
    columns:
      - name: fun
        tags: [finance]
"""

models__model_a_sql = """
SELECT 1 AS fun
"""

models__model_b_sql = """
SELECT 1 AS fun
"""

seeds__model_c_csv = """fun,_loaded_at
1,2021-04-19 01:00:00"""


@pytest.fixture(scope="class")
def models():
    return {
        "schema.yml": models__schema_yml,
        "model_b.sql": models__model_b_sql,
        "model_a.sql": models__model_a_sql,
    }


@pytest.fixture(scope="class")
def seeds():
    return {"model_c.csv": seeds__model_c_csv}


@pytest.fixture(scope="class")
def selectors():
    return """
            selectors:
            - name: default_selector
              description: test default selector
              definition:
                union:
                  - method: source
                    value: "test.src.source_a"
                  - method: fqn
                    value: "model_a"
              default: true
        """


class TestDefaultSelectors:
    def test_model__list(self, project):
        result = run_dbt(["ls", "--resource-type", "model"])
        assert "test.model_a" in result

    def test_model__compile(self, project):
        result = run_dbt(["compile"])
        assert len(result) == 1
        assert result.results[0].node.name == "model_a"

    def test_source__freshness(self, project):
        run_dbt(["seed", "-s", "test.model_c"])
        result = run_dbt(["source", "freshness"])
        assert len(result) == 1
        assert result.results[0].node.name == "source_a"
