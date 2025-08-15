import json
import pytest

from dbt.tests.util import run_dbt

model_one_sql = """
select 1 as fun
"""

model_with_materialization_sql = """
{{ config(materialized='table') }}
select 1 as fun
"""

model_with_meta_sql = """
{{ config(
    materialized='incremental',
    meta={'owner': 'data-team', 'criticality': 'high'}
) }}
select 2 as id, 'meta_test' as name
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
        return {
            "model_one.sql": model_one_sql, 
            "model_table.sql": model_with_materialization_sql,
            "model_meta.sql": model_with_meta_sql
        }

    def test_output_key_single(self, project):
        result = run_dbt(["-q", "ls", "--output", "json", "--output-keys", "name"])
        assert len(result) == 3
        expected_names = ["model_one", "model_table", "model_meta"]
        actual_names = [json.loads(r)["name"] for r in result]
        assert set(actual_names) == set(expected_names)

    def test_output_key_quoted(self, project):
        result = run_dbt(["-q", "ls", "--output", "json", "--output-keys", "name resource_type"])

        assert len(result) == 3
        # All should be models with names
        for r in result:
            result_json = json.loads(r)
            assert result_json["resource_type"] == "model"
            assert "name" in result_json

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

        assert len(result) == 3
        # All should be models with names  
        for r in result:
            result_json = json.loads(r)
            assert result_json["resource_type"] == "model"
            assert "name" in result_json

    def test_output_key_nested(self, project):
        result = run_dbt(
            [
                "-q",
                "ls",
                "--output",
                "json",
                "--output-keys",
                "name",
                "--output-keys",
                "config.materialized",
                "--select",
                "model_table",
            ]
        )

        assert len(result) == 1
        import json

        result_json = json.loads(result[0])
        assert result_json["name"] == "model_table"
        assert result_json["config.materialized"] == "table"

    def test_output_key_nested_single_arg(self, project):
        result = run_dbt(
            [
                "-q",
                "ls",
                "--output",
                "json",
                "--output-keys",
                "config.materialized name",
                "--select",
                "model_table",
            ]
        )

        assert len(result) == 1
        import json

        result_json = json.loads(result[0])
        assert result_json["name"] == "model_table"
        assert result_json["config.materialized"] == "table"

    def test_output_key_nested_nonexistent(self, project):
        """Test that non-existent nested keys return empty objects"""
        result = run_dbt(
            [
                "-q",
                "ls",
                "--output",
                "json",
                "--output-keys",
                "config.nonexistent",
                "--select",
                "model_table",
            ]
        )

        assert len(result) == 1
        import json

        result_json = json.loads(result[0])
        assert result_json == {}  # Non-existent key should result in empty object

    def test_output_key_nested_mixed_existent_nonexistent(self, project):
        """Test mixing existent and non-existent nested keys"""
        result = run_dbt(
            [
                "-q",
                "ls",
                "--output",
                "json",
                "--output-keys",
                "name",
                "--output-keys",
                "config.materialized",
                "--output-keys",
                "config.nonexistent",
                "--select",
                "model_table",
            ]
        )

        assert len(result) == 1
        import json

        result_json = json.loads(result[0])
        assert result_json["name"] == "model_table"
        assert result_json["config.materialized"] == "table"
        # Non-existent key should not appear in result
        assert "config.nonexistent" not in result_json

    def test_output_key_nested_deep_nonexistent(self, project):
        """Test deeply nested non-existent keys"""
        result = run_dbt(
            [
                "-q",
                "ls",
                "--output",
                "json",
                "--output-keys",
                "config.meta.owner.nonexistent",
                "--select",
                "model_table",
            ]
        )

        assert len(result) == 1
        import json

        result_json = json.loads(result[0])
        assert result_json == {}  # Deep non-existent key should result in empty object

    def test_output_key_nested_deep_meta(self, project):
        """Test deeply nested meta keys that exist"""
        result = run_dbt(
            [
                "-q",
                "ls",
                "--output",
                "json",
                "--output-keys",
                "name",
                "--output-keys", 
                "config.meta.owner",
                "--select",
                "model_meta",
            ]
        )

        assert len(result) == 1
        result_json = json.loads(result[0])
        assert result_json["name"] == "model_meta"
        assert result_json["config.meta.owner"] == "data-team"

    def test_output_key_nested_whole_meta_object(self, project):
        """Test getting the whole meta object as nested key"""
        result = run_dbt(
            [
                "-q",
                "ls",
                "--output",
                "json",
                "--output-keys",
                "config.meta",
                "--select",
                "model_meta",
            ]
        )

        assert len(result) == 1
        result_json = json.loads(result[0])
        expected_meta = {"owner": "data-team", "criticality": "high"}
        assert result_json["config.meta"] == expected_meta


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
