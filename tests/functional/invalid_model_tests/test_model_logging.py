import pytest

from dbt.tests.util import run_dbt_and_capture

warnings_sql = """
{{ config(group='my_group') }}
{% do exceptions.warn('warning: everything is terrible but not that terrible') %}
{{ exceptions.warn("warning: everything is terrible but not that terrible") }}
select 1 as id
"""

schema_yml = """
version: 2
groups:
  - name: my_group
    owner:
      name: group_owner
"""


class TestModelLogging:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "warnings.sql": warnings_sql,
            "schema.yml": schema_yml,
        }

    def test_warn(self, project):
        results, log_output = run_dbt_and_capture(["run", "--log-format", "json"])
        log_lines = log_output.split("\n")

        log_lines_with_warning = [line for line in log_lines if "JinjaLogWarning" in line]
        assert len(log_lines_with_warning) == 4
        assert all("everything is terrible" in line for line in log_lines_with_warning)

        log_lines_with_group = [line for line in log_lines if "LogModelResult" in line]
        assert len(log_lines_with_group) == 1
        assert "group_owner" in log_lines_with_group[0]
        assert "my_group" in log_lines_with_group[0]
