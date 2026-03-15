import json
import os

import pytest

from dbt.tests.util import run_dbt

docs_md = """{% docs test_doc %}
This is a test for column {test_name}
{% enddocs %}
"""

schema_yml = """
models:
  - name: my_colors
    columns:
      - name: id
        description: "{{ doc('test_doc').format(test_name = 'id') }}"
      - name: color
        description: "{{ 'This is a test for column {test_name}'.format(test_name = 'color') }}"
"""


class TestDocBlocksBackCompat:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_colors.sql": "select 1 as id, 'blue' as color",
            "schema.yml": schema_yml,
            "docs.md": docs_md,
        }

    def test_doc_blocks_back_compat(self, project):
        run_dbt(["parse"])

        assert os.path.exists("./target/manifest.json")

        with open("./target/manifest.json") as fp:
            manifest = json.load(fp)

        model_data = manifest["nodes"]["model.test.my_colors"]

        for column_name, column in model_data["columns"].items():
            assert column["description"] == f"This is a test for column {column_name}"
            assert column["doc_blocks"] == []
