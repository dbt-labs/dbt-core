import pytest
from dbt.contracts.graph.model_config import ExposureConfig

from dbt.tests.util import run_dbt, update_config_file, get_manifest


class ExposureConfigTests:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self):
        pytest.expected_config = ExposureConfig(
            enabled=True,
        )


models__people_exposure_yml = """
version: 2

exposures:
  - name: simple_exposure
    type: dashboard
    depends_on:
      - ref('model')
      - source('my_source', 'my_table')
    owner:
      email: something@example.com
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('model')
      - ref('second_model')
    owner:
      email: something@example.com
      name: Some name
    description: >
      A description of the complex exposure
    maturity: medium
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
    url: http://example.com/notebook/1
"""


models__people_sql = """
select 1 as id, 'Drew' as first_name, 'Banin' as last_name, 'yellow' as favorite_color, true as loves_dbt, 5 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Jeremy' as first_name, 'Cohen' as last_name, 'indigo' as favorite_color, true as loves_dbt, 4 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Callum' as first_name, 'McCann' as last_name, 'emerald' as favorite_color, true as loves_dbt, 0 as tenure, current_timestamp as created_at
"""


# Test enabled config for exposure in dbt_project.yml
class TestExposureEnabledConfigProjectLevel(ExposureConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models__people_sql,
            "schema.yml": models__people_exposure_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "exposure": {
                "simple_exposure": {
                    "enabled": True,
                },
            }
        }

    def test_enabled_exposure_config_dbt_project(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "exposure.test.simple_exposure" in manifest.exposures

        new_enabled_config = {
            "exposures": {
                "test": {
                    "simple_exposure": {
                        "enabled": False,
                    },
                }
            }
        }
        update_config_file(new_enabled_config, project.project_root, "dbt_project.yml")
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "exposure.test.simple_exposure" not in manifest.exposures
        assert "exposure.test.notebook_exposure" in manifest.exposures


disabled_exposure_level__schema_yml = """
version: 2

exposures:
  - name: simple_exposure
    type: dashboard
    config:
      enabled: False
    depends_on:
      - ref('model')
      - source('my_source', 'my_table')
    owner:
      email: something@example.com
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('model')
      - ref('second_model')
    owner:
      email: something@example.com
      name: Some name
    description: >
      A description of the complex exposure
    maturity: medium
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
    url: http://example.com/notebook/1
"""


# Test enabled config at exposure level in yml file
class TestConfigYamlLevel(ExposureConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models__people_sql,
            "schema.yml": disabled_exposure_level__schema_yml,
        }

    def test_exposure_config_yaml_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "exposure.test.simple_exposure" not in manifest.exposures
        assert "exposure.test.notebook_exposure" in manifest.exposures


enabled_yaml_level__schema_yml = """
version: 2

exposures:
  - name: simple_exposure
    type: dashboard
    config:
      enabled: True
    depends_on:
      - ref('model')
      - source('my_source', 'my_table')
    owner:
      email: something@example.com
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('model')
      - ref('second_model')
    owner:
      email: something@example.com
      name: Some name
    description: >
      A description of the complex exposure
    maturity: medium
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']
    url: http://example.com/notebook/1
"""

# Test inheritence - set configs at project and exposure level - expect exposure level to win
class TestExposureConfigsInheritence1(ExposureConfigTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people.sql": models__people_sql,
            "schema.yml": enabled_yaml_level__schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"exposures": {"enabled": False}}

    def test_exposure_all_configs(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        # This should be overridden
        assert "exposure.test.simple_exposure" in manifest.exposures
        # This should stay disabled
        assert "exposure.test.notebook_exposure" not in manifest.exposures

        config_test_table = manifest.exposures.get("exposure.test.number_of_people").config

        assert isinstance(config_test_table, ExposureConfig)
        assert config_test_table == pytest.expected_config


# TODO: add test with model ref'ing disabled metric, expect error
