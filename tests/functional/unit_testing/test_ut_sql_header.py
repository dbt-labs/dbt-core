from pathlib import Path

import pytest

from dbt.tests.util import run_dbt

UNITTEST_MODEL_NAME = "unittest_model"
HEADER_MARKER = "-- SQL_HEADER_INJECTION_TEST_MARKER"

UNITTEST_MODEL_SQL = f"""
{{{{ config(
    materialized='table',
    sql_header='{HEADER_MARKER}'
)}}}}
select 1 as id union all select 2 as id
"""

UNITTEST_MACRO_SQL_HEADER_MODEL_SQL = f"""
{{{{ config(
    materialized='table',
)}}}}
{{% call set_sql_header(config) %}}
  {HEADER_MARKER}
{{% endcall %}}

select 1 as id union all select 2 as id
"""

UNITTEST_SCHEMA_YML = f"""
unit_tests:
  - name: test_sql_macro_header_injection
    model: {UNITTEST_MODEL_NAME}
    given: []
    expect:
      rows:
        - {{id: 1}}
        - {{id: 2}}
"""


class SQLHeaderInjectionBase:
    def test_sql_header_is_injected(self, project):
        results = run_dbt(["build"])
        assert any(r.node.name == UNITTEST_MODEL_NAME for r in results)

        # Compute path to compiled SQL
        compiled_model_path = (
            project.project_root
            / Path("target")
            / Path("run")
            / Path(project.project_name)
            / Path("models")
            / Path(f"{UNITTEST_MODEL_NAME}.sql")
        )

        assert compiled_model_path.exists(), f"Compiled SQL not found: {compiled_model_path}"
        compiled_sql = compiled_model_path.read_text()
        assert HEADER_MARKER in compiled_sql, "SQL header marker not found in compiled SQL."


class TestSQLHeaderConfigInjection(SQLHeaderInjectionBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{UNITTEST_MODEL_NAME}.sql": UNITTEST_MODEL_SQL,
            "schema.yml": UNITTEST_SCHEMA_YML,
        }


class TestSQLHeaderMacroInjection(SQLHeaderInjectionBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{UNITTEST_MODEL_NAME}.sql": UNITTEST_MACRO_SQL_HEADER_MODEL_SQL,
            "schema.yml": UNITTEST_SCHEMA_YML,
        }
