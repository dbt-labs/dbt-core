import pytest
import os

from dbt.tests.util import run_dbt, get_manifest, write_file

from tests.functional.configs.fixtures import BaseConfigProject

CUSTOM_NODE_COLOR_MODEL_LEVEL = "red"
CUSTOM_NODE_COLOR_SCHEMA_LEVEL = "blue"
CUSTOM_NODE_COLOR_PROJECT_LEVEL_ROOT = "green"
CUSTOM_NODE_COLOR_PROJECT_LEVEL_FOLDER = "purple"

# F strings are a pain here so replacing XXX with the config above instead
models__custom_node_color__model_sql = """
{{ config(materialized='view', docs={'node_color': 'XXX'}) }}

select 1 as id

""".replace(
    "XXX", CUSTOM_NODE_COLOR_MODEL_LEVEL
)

models__non_custom_node_color__model_sql = """
{{ config(materialized='view') }}

select 1 as id

"""

models__custom_node_color__schema_yml = """
version: 2

models:
  - name: custom_color_model
    description: "This is a model description"
    config:
      docs:
        node_color: {}
""".format(
    CUSTOM_NODE_COLOR_SCHEMA_LEVEL
)


models__non_custom_node_color__schema_yml = """
version: 2

models:
  - name: non_custom_color_model
    description: "This is a model description"
    config:
      docs:
        node_color: {}
""".format(
    CUSTOM_NODE_COLOR_SCHEMA_LEVEL
)


class TestCustomNodeColorModelvsProject(BaseConfigProject):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+docs": {"node_color": CUSTOM_NODE_COLOR_PROJECT_LEVEL_ROOT},
                    "subdirectory": {
                        "+docs": {"node_color": CUSTOM_NODE_COLOR_PROJECT_LEVEL_FOLDER},
                    },
                }
            }
        }

    # validation that model level node_color configs supercede dbt_project.yml
    def test__model_override_project(
        self,
        project,
    ):
        write_file(
            models__custom_node_color__model_sql,
            project.project_root,
            "models",
            "custom_color_model.sql",
        )

        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.custom_color_model"
        my_model_config = manifest.nodes[model_id].config
        my_model_docs = manifest.nodes[model_id].docs

        node_color_actual_config = my_model_config["docs"].node_color
        node_color_actual_docs = my_model_docs.node_color

        # check node_color config is in the right spots for each model
        assert node_color_actual_config == CUSTOM_NODE_COLOR_MODEL_LEVEL
        assert node_color_actual_docs == CUSTOM_NODE_COLOR_MODEL_LEVEL

    # validation that model level node_color configs supercede schema.yml
    def test__model_override_schema(
        self,
        project,
    ):

        write_file(
            models__custom_node_color__model_sql,
            project.project_root,
            "models",
            "custom_color_model.sql",
        )

        write_file(
            models__custom_node_color__schema_yml,
            project.project_root,
            "models",
            "custom_color_schema.yml",
        )

        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.custom_color_model"
        my_model_config = manifest.nodes[model_id].config
        my_model_docs = manifest.nodes[model_id].docs

        node_color_actual_config = my_model_config["docs"].node_color
        node_color_actual_docs = my_model_docs.node_color

        # check node_color config is in the right spots for each model
        assert node_color_actual_config == CUSTOM_NODE_COLOR_MODEL_LEVEL
        assert node_color_actual_docs == CUSTOM_NODE_COLOR_MODEL_LEVEL

    # validation that node_color configured on subdirectories in dbt_project.yml supercedes project root
    def test__project_folder_override_project_root(
        self,
        project,
    ):

        # create subdirectory for non custom color model to validate dbt_project.yml settings
        if not os.path.exists("models/subdirectory"):
            os.mkdir("models/subdirectory")

        write_file(
            models__non_custom_node_color__model_sql,
            project.project_root,
            "models/subdirectory",
            "non_custom_color_model_subdirectory.sql",
        )

        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.non_custom_color_model_subdirectory"
        my_model_config = manifest.nodes[model_id].config
        my_model_docs = manifest.nodes[model_id].docs

        node_color_actual_config = my_model_config["docs"].node_color
        node_color_actual_docs = my_model_docs.node_color

        # check node_color config is in the right spots for each model
        assert node_color_actual_config == CUSTOM_NODE_COLOR_PROJECT_LEVEL_FOLDER
        assert node_color_actual_docs == CUSTOM_NODE_COLOR_PROJECT_LEVEL_FOLDER

    # validation that node_color configured in schema.yml supercedes dbt_project.yml
    def test__schema_override_project(
        self,
        project,
    ):

        write_file(
            models__non_custom_node_color__model_sql,
            project.project_root,
            "models",
            "non_custom_color_model.sql",
        )

        write_file(
            models__non_custom_node_color__schema_yml,
            project.project_root,
            "models",
            "non_custom_color_schema.yml",
        )

        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)

        model_id = "model.test.non_custom_color_model"
        my_model_config = manifest.nodes[model_id].config
        my_model_docs = manifest.nodes[model_id].docs

        node_color_actual_config = my_model_config["docs"].node_color
        node_color_actual_docs = my_model_docs.node_color

        # check node_color config is in the right spots for each model
        assert node_color_actual_config == CUSTOM_NODE_COLOR_SCHEMA_LEVEL
        assert node_color_actual_docs == CUSTOM_NODE_COLOR_SCHEMA_LEVEL
