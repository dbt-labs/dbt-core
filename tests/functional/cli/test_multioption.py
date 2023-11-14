import pytest
from dbt.tests.util import run_dbt


model_one_sql = """
select 1 as fun
"""

schema_sql = """
sources:
  - name: my_source
    description: "My source"
    schema: test_schema
    tables:
      - name: my_table
      - name: my_other_table

exposures:
  - name: weekly_jaffle_metrics
    label: By the Week
    type: dashboard
    maturity: high
    url: https://bi.tool/dashboards/1
    description: >
      Did someone say "exponential growth"?
    depends_on:
      - ref('model_one')
    owner:
      name: dbt Labs
      email: data@jaffleshop.com
"""


class TestResourceType:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": schema_sql, "model_one.sql": model_one_sql}

    def test_resource_type_single(self, project):
        result = run_dbt(["-q", "ls", "--resource-types", "model"])
        assert len(result) == 1
        assert result == ["test.model_one"]

    def test_resource_type_quoted(self, project):
        result = run_dbt(["-q", "ls", "--resource-types", "model source"])
        assert len(result) == 3
        expected_result = {
            "test.model_one",
            "source:test.my_source.my_table",
            "source:test.my_source.my_other_table",
        }
        assert set(result) == expected_result

    def test_resource_type_args(self, project):
        result = run_dbt(
            [
                "-q",
                "ls",
                "--resource-type",
                "model",
                "--resource-type",
                "source",
                "--resource-type",
                "exposure",
            ]
        )
        assert len(result) == 4
        expected_result = {
            "test.model_one",
            "source:test.my_source.my_table",
            "source:test.my_source.my_other_table",
            "exposure:test.weekly_jaffle_metrics",
        }
        assert set(result) == expected_result


class TestOutputKeys:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_one.sql": model_one_sql}

    def test_output_key_single(self, project):
        result = run_dbt(["-q", "ls", "--output", "json", "--output-keys", "name"])
        assert len(result) == 1
        assert result == ['{"name": "model_one"}']

    def test_output_key_quoted(self, project):
        result = run_dbt(["-q", "ls", "--output", "json", "--output-keys", "name resource_type"])

        assert len(result) == 1
        assert result == ['{"name": "model_one", "resource_type": "model"}']

    def test_output_key_args(self, project):
        result = run_dbt(
            [
                "-q",
                "ls",
                "--output",
                "json",
                "--output-keys",
                "name",
                "--output-keys",
                "resource_type",
            ]
        )

        assert len(result) == 1
        assert result == ['{"name": "model_one", "resource_type": "model"}']


class TestSelectExclude:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
            "model_two.sql": model_one_sql,
            "model_three.sql": model_one_sql,
        }

    def test_select_exclude_single(self, project):
        result = run_dbt(["-q", "ls", "--select", "model_one"])
        assert len(result) == 1
        assert result == ["test.model_one"]
        result = run_dbt(["-q", "ls", "--exclude", "model_one"])
        assert len(result) == 2
        assert "test.model_one" not in result

    def test_select_exclude_quoted(self, project):
        result = run_dbt(["-q", "ls", "--select", "model_one model_two"])
        assert len(result) == 2
        assert "test.model_three" not in result
        result = run_dbt(["-q", "ls", "--exclude", "model_one model_two"])
        assert len(result) == 1
        assert result == ["test.model_three"]

    def test_select_exclude_args(self, project):
        result = run_dbt(["-q", "ls", "--select", "model_one", "--select", "model_two"])
        assert len(result) == 2
        assert "test.model_three" not in result
        result = run_dbt(["-q", "ls", "--exclude", "model_one", "--exclude", "model_two"])
        assert len(result) == 1
        assert result == ["test.model_three"]
