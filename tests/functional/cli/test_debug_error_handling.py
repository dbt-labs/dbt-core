import pytest
from dbt.cli.main import dbtRunner


broken_profile = """
broken_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: myuser
      password: mypassword
      dbname: mydb
      schema: myschema
      threads: 1
      connect_timeout: 10
      # These two keys are aliases for the same parameter.
      # This misconfiguration triggers the bug.
      connect_timeout: 10
"""

dbt_project_yml = """
name: 'core_12502'
profile: 'broken_profile'
models:
  core_12502:
    +enabled: "{{ target.name == 'prod' }}"
"""


class TestDebugWithDuplicateProfileKeys:
    @pytest.fixture(scope="class")
    def dbt_profile_data(self):
        return {
            "broken_profile": {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "postgres",
                        "host": "localhost",
                        "port": 5432,
                        "user": "myuser",
                        "password": "mypassword",
                        "dbname": "mydb",
                        "schema": "myschema",
                        "threads": 1,
                    }
                },
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_debug_surfaces_profile_error_not_compilation_error(self, project):
        """
        Regression test for https://github.com/dbt-labs/dbt-core/issues/12502

        When a profile has duplicate connection attributes, dbt debug should
        surface the real profile validation error, not a misleading
        CompilationError about 'target' being undefined.
        """
        events = []
        result = dbtRunner(callbacks=[events.append]).invoke(["debug"])

        assert not result.success
        messages = [e.info.msg for e in events if hasattr(e, "info")]

        # Should NOT show the misleading CompilationError
        assert not any("'target' is undefined" in m for m in messages)
        # Should show something about the profile failure
        assert any("profile" in m.lower() for m in messages)