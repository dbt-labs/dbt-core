import pytest

from dbt.tests.util import run_dbt, get_manifest

from dbt.dataclass_schema import ValidationError

CUSTOM_NODE_COLOR_MODEL_LEVEL = "red"
CUSTOM_NODE_COLOR_SCHEMA_LEVEL = "blue"
CUSTOM_NODE_COLOR_PROJECT_LEVEL_ROOT = "#121212"
CUSTOM_NODE_COLOR_PROJECT_LEVEL_FOLDER = "purple"
CUSTOM_NODE_COLOR_INVALID_HEX = '"#xxx111"'
CUSTOM_NODE_COLOR_INVALID_NAME = "notacolor"

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

models__show_docs_false__model_sql = """
{{ config(materialized='view', docs={"show": True}) }}

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
        show: True
""".format(
    CUSTOM_NODE_COLOR_SCHEMA_LEVEL
)

# To check that incorect configs are raising errors
models__non_custom_node_color_invalid_config_docs__schema_yml = """
version: 2

models:
  - name: non_custom_node_color
    description: "This is a model description"
    config:
      docs:
        node_color: {}
        show: True
""".format(
    CUSTOM_NODE_COLOR_INVALID_HEX
)

models__non_custom_node_color_invalid_docs__schema_yml = """
version: 2

models:
  - name: non_custom_node_color
    description: "This is a model description"
    docs:
      node_color: {}
      show: True
""".format(
    CUSTOM_NODE_COLOR_INVALID_NAME
)

models__custom_node_color_invalid_hex__model_sql = """
{{ config(materialized='view', docs={"show": True, "node_color": XXX }) }}

select 1 as id
""".replace(
    "XXX", CUSTOM_NODE_COLOR_INVALID_HEX
)


class BaseCustomNodeColorModelvsProject:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+docs": {"node_color": CUSTOM_NODE_COLOR_PROJECT_LEVEL_ROOT, "show": False},
                    "subdirectory": {
                        "+docs": {
                            "node_color": CUSTOM_NODE_COLOR_PROJECT_LEVEL_FOLDER,
                            "show": True,
                        },
                    },
                }
            }
        }


# validation that model level node_color configs supercede dbt_project.yml
class TestModelLevelProjectColorConfigs(BaseCustomNodeColorModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {"custom_color_model.sql": models__custom_node_color__model_sql}

    def test__model_override_project(self, project):

        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.custom_color_model"
        my_model_config = manifest.nodes[model_id].config
        my_model_docs = manifest.nodes[model_id].docs

        node_color_actual_config = my_model_config["docs"].node_color
        show_actual_config = my_model_config["docs"].show
        node_color_actual_docs = my_model_docs.node_color
        show_actual_docs = my_model_docs.show

        # check node_color config is in the right spots for each model
        assert node_color_actual_config == CUSTOM_NODE_COLOR_MODEL_LEVEL
        assert node_color_actual_docs == CUSTOM_NODE_COLOR_MODEL_LEVEL
        assert not show_actual_config
        assert not show_actual_docs


# validation that model level node_color configs supercede schema.yml
class TestModelLevelSchemaColorConfigs(BaseCustomNodeColorModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "custom_color_model.sql": models__custom_node_color__model_sql,
            "custom_color_schema.yml": models__custom_node_color__schema_yml,
        }

    def test__model_override_schema(self, project):

        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.custom_color_model"
        my_model_config = manifest.nodes[model_id].config
        my_model_docs = manifest.nodes[model_id].docs

        node_color_actual_config = my_model_config["docs"].node_color
        show_actual_config = my_model_config["docs"].show
        node_color_actual_docs = my_model_docs.node_color
        show_actual_docs = my_model_docs.show

        # check node_color config is in the right spots for each model
        assert node_color_actual_config == CUSTOM_NODE_COLOR_MODEL_LEVEL
        assert node_color_actual_docs == CUSTOM_NODE_COLOR_MODEL_LEVEL
        assert not show_actual_config
        assert not show_actual_docs


# validation that node_color configured on subdirectories in dbt_project.yml supercedes project root
class TestSubdirectoryColorConfigs(BaseCustomNodeColorModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "subdirectory": {
                "non_custom_color_model_subdirectory.sql": models__non_custom_node_color__model_sql
            }
        }

    def test__project_folder_override_project_root(self, project):
        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.non_custom_color_model_subdirectory"
        my_model_config = manifest.nodes[model_id].config
        my_model_docs = manifest.nodes[model_id].docs

        node_color_actual_config = my_model_config["docs"].node_color
        show_actual_config = my_model_config["docs"].show
        node_color_actual_docs = my_model_docs.node_color
        show_actual_docs = my_model_docs.show

        # check node_color config is in the right spots for each model
        assert node_color_actual_config == CUSTOM_NODE_COLOR_PROJECT_LEVEL_FOLDER
        assert node_color_actual_docs == CUSTOM_NODE_COLOR_PROJECT_LEVEL_FOLDER
        # in this case show should be True since the dbt_project.yml overrides the root setting for /subdirectory
        assert show_actual_config
        assert show_actual_docs


# validation that node_color configured in schema.yml supercedes dbt_project.yml
class TestSchemaOverProjectColorConfigs(BaseCustomNodeColorModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "non_custom_color_model.sql": models__non_custom_node_color__model_sql,
            "non_custom_color_schema.yml": models__non_custom_node_color__schema_yml,
        }

    def test__schema_override_project(
        self,
        project,
    ):

        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)

        model_id = "model.test.non_custom_color_model"
        my_model_config = manifest.nodes[model_id].config
        my_model_docs = manifest.nodes[model_id].docs

        node_color_actual_config = my_model_config["docs"].node_color
        show_actual_config = my_model_config["docs"].show
        node_color_actual_docs = my_model_docs.node_color
        show_actual_docs = my_model_docs.show

        # check node_color config is in the right spots for each model
        assert node_color_actual_config == CUSTOM_NODE_COLOR_SCHEMA_LEVEL
        assert node_color_actual_docs == CUSTOM_NODE_COLOR_SCHEMA_LEVEL
        # in this case show should be True since the schema.yml overrides the dbt_project.yml
        assert show_actual_config
        assert show_actual_docs


# validation that docs: show configured in model file supercedes dbt_project.yml
class TestModelOverProjectColorConfigs(BaseCustomNodeColorModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {"show_docs_override_model.sql": models__show_docs_false__model_sql}

    def test__model_show_overrides_dbt_project(
        self,
        project,
    ):

        run_dbt(["compile"])
        manifest = get_manifest(project.project_root)

        model_id = "model.test.show_docs_override_model"
        my_model_config = manifest.nodes[model_id].config
        my_model_docs = manifest.nodes[model_id].docs

        node_color_actual_config = my_model_config["docs"].node_color
        show_actual_config = my_model_config["docs"].show
        node_color_actual_docs = my_model_docs.node_color
        show_actual_docs = my_model_docs.show

        # check node_color config is in the right spots for each model
        assert node_color_actual_config == CUSTOM_NODE_COLOR_PROJECT_LEVEL_ROOT
        assert node_color_actual_docs == CUSTOM_NODE_COLOR_PROJECT_LEVEL_ROOT
        # in this case show should be True since the schema.yml overrides the dbt_project.yml
        assert show_actual_config
        assert show_actual_docs


# validation that an incorrect color in dbt_project.yml raises an exception
class TestCustomNodeColorIncorrectColorProject:
    @pytest.fixture(scope="class")
    def models(self):  # noqa: F811
        return {"non_custom_node_color.sql": models__non_custom_node_color__model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {"+docs": {"node_color": CUSTOM_NODE_COLOR_INVALID_NAME, "show": False}}
            }
        }

    def test__invalid_color_project(
        self,
        project,
    ):
        with pytest.raises(ValidationError):
            run_dbt(["compile"])


# validation that an incorrect color in the config block raises an exception
class TestCustomNodeColorIncorrectColorModelConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "custom_node_color_invalid_hex.sql": models__custom_node_color_invalid_hex__model_sql
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+docs": {"node_color": "blue", "show": False}}}

    def test__invalid_color_config_block(
        self,
        project,
    ):
        with pytest.raises(ValidationError):
            run_dbt(["compile"])


# validation that an incorrect color in the YML file raises an exception
class TestCustomNodeColorIncorrectColorNameYMLConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "non_custom_node_color.sql": models__non_custom_node_color__model_sql,
            "invalid_custom_color.yml": models__non_custom_node_color_invalid_docs__schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+docs": {"node_color": "blue", "show": False}}}

    def test__invalid_color_docs_not_under_config(
        self,
        project,
    ):
        with pytest.raises(ValidationError):
            run_dbt(["compile"])


class TestCustomNodeColorIncorrectColorHEXYMLConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "non_custom_node_color.sql": models__non_custom_node_color__model_sql,
            "invalid_custom_color.yml": models__non_custom_node_color_invalid_config_docs__schema_yml,
        }

    def test__invalid_color_docs_under_config(
        self,
        project,
    ):
        with pytest.raises(ValidationError):
            run_dbt(["compile"])
