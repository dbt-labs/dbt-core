import json
import os

import pytest

from dbt.tests.util import run_dbt

schema_yml = """
models:
  - name: my_colors
    doc_blocks: 2
    columns:
      - name: id
        doc_blocks: 2
      - name: color
        doc_blocks: ["hello", 2, "world"]
"""


class TestDocBlocksBackCompat:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_colors.sql": "select 1 as id, 'blue' as color",
            "schema.yml": schema_yml,
        }

    def test_doc_blocks_back_compat(self, project):
        run_dbt(["parse"])

        assert os.path.exists("./target/manifest.json")

        with open("./target/manifest.json") as fp:
            manifest = json.load(fp)

        model_data = manifest["nodes"]["model.test.my_colors"]
        assert model_data["doc_blocks"] == []
        assert all(column["doc_blocks"] == [] for column in model_data["columns"].values())
