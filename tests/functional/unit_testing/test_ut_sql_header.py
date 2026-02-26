from pathlib import Path

import pytest

from dbt.tests.util import run_dbt

PROJECT_NAME = "sql_header_test"
UNITTEST_MODEL_NAME = "unittest_model"
# A comment marker to verify sql_header injection in compiled output.
# Database agnostic since we only need to confirm the header appears in compiled SQL.
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
  - name: test_sql_header_injection
    model: {UNITTEST_MODEL_NAME}
    given: []
    expect:
      rows:
        - {{id: 1}}
        - {{id: 2}}
"""


class SQLHeaderInjectionBase:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": PROJECT_NAME,
        }

    def test_sql_header_is_injected(self, project):
        results = run_dbt(["build"])
        assert any(r.node.name == UNITTEST_MODEL_NAME for r in results)

        compiled_model_path = (
            project.project_root
            / Path("target")
            / Path("run")
            / Path(PROJECT_NAME)
            / Path("models")
            / Path(f"{UNITTEST_MODEL_NAME}.sql")
        )

        with open(compiled_model_path) as f:
            assert any(
                HEADER_MARKER in line for line in f
            ), "SQL header marker not found in compiled SQL."


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
