import pytest

from dbt.tests.util import get_artifact, run_dbt
from tests.functional.generic_test_description.fixtures import (
    models__doc_block_md,
    models__my_model_sql,
    models__schema_yml,
)


class TestBuiltinGenericTestDescription:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": models__my_model_sql,
            "schema.yml": models__schema_yml,
            "doc_block.md": models__doc_block_md,
        }

    def test_compile(self, project):
        run_dbt(["compile"])
        manifest = get_artifact(project.project_root, "target", "manifest.json")
        assert len(manifest["nodes"]) == 4

        nodes = {node["alias"]: node for node in manifest["nodes"].values()}

        assert nodes["unique_my_model_id"]["description"] == "id must be unique"
        assert nodes["not_null_my_model_id"]["description"] == ""
        assert (
            nodes["accepted_values_my_model_color__blue__green__red"]["description"]
            == "The `color` column must be one of 'blue', 'green', or 'red'."
        )
