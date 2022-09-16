import pytest
from dbt.tests.util import run_dbt

from dbt.exceptions import CompilationException

my_model = """
select 1 as user
"""

my_model_2_enabled = """
select * from {{ ref('my_model') }}
"""

my_model_3_enabled = """
select * from {{ ref('my_model_2') }}
"""

my_model_2_disabled = """
{{ config(enabled=false) }}
select * from {{ ref('my_model') }}
"""

my_model_3_disabled = """
{{ config(enabled=false) }}
select * from {{ ref('my_model_2') }}
"""

schema_all_disabled_yml = """
version: 2
models:
  - name: my_model
  - name: my_model_2
    config:
      enabled: false
  - name: my_model_3
    config:
      enabled: false
"""


# ensure double disabled doesn't throw error when set at schema level
class TestSchemaDisabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_all_disabled_yml,
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2_enabled,
            "my_model_3.sql": my_model_3_enabled,
        }

    def test_disabled_config(self, project):
        run_dbt(["parse"])


schema_partial_disabled_yml = """
version: 2
models:
  - name: my_model
  - name: my_model_2
    config:
      enabled: false
  - name: my_model_3
"""


# ensure this throws a specific error that the model is disabled
class TestSchemaDisabledConfigsFailure:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_partial_disabled_yml,
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2_enabled,
            "my_model_3.sql": my_model_3_enabled,
        }

    def test_disabled_config(self, project):
        with pytest.raises(CompilationException) as exc:
            run_dbt(["parse"])
        exc_str = " ".join(str(exc.value).split())  # flatten all whitespace
        expected_msg = "which is disabled"
        assert expected_msg in exc_str


# ensure double disabled doesn't throw error when set in model configs
class TestModelDisabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2_disabled,
            "my_model_3.sql": my_model_3_disabled,
        }

    def test_disabled_config(self, project):
        run_dbt(["parse"])


# ensure double disabled doesn't throw error when set in project.yml
class TestProjectFileDisabledConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model,
            "my_model_2.sql": my_model_2_enabled,
            "my_model_3.sql": my_model_3_enabled,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "my_model_2": {
                        "enabled": False,
                    },
                    "my_model_3": {
                        "enabled": False,
                    },
                },
            }
        }

    def test_disabled_config(self, project):
        run_dbt(["parse"])
