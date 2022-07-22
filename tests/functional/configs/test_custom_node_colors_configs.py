import pytest

from tests.functional.configs.fixtures import BaseConfigProject

models__custom_node_colors__model_sql = """
{{
    config(
        materialized='view',
        node_color='#c0c0c0'
    )
}}

select 1 as id

"""

models_custom_node_colors__schema_yml = """
version: 2

models:
  - name: model
    description: "This is a model description"
    config:
        node_color: '#c0c0c0'
"""


class TestCustomNodeColorConfigs(BaseConfigProject):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {"+node_color": "#c0c0c0"},
            },
        }

    def test_custom_color_layering(
        self,
        project,
    ):
        pass
