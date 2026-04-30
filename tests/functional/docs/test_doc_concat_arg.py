import json

import pytest

from dbt.tests.util import run_dbt

docs_md = """{% docs test_doc %}
this is a docs block
{% enddocs %}
"""

schema_yml = """
models:
  - name: my_model
    description: "{{ doc('test_' ~ 'doc') }}"
    columns:
      - name: id
        description: "{{ doc('test_' ~ 'doc') }}"
"""


class TestDocConcatArg:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as id",
            "schema.yml": schema_yml,
            "docs.md": docs_md,
        }

    def test_concat_arg_succeeds(self, project):
        run_dbt(["parse"])

        with open("./target/manifest.json") as fp:
            manifest = json.load(fp)

        model_data = manifest["nodes"]["model.test.my_model"]
        assert model_data["description"] == "this is a docs block"
        # Ideally, this would be able to track the doc block in lineage.
        # However, Const jinja nodes are not handled statically for this resolution.
        assert model_data["doc_blocks"] == []

        column_data = model_data["columns"]["id"]
        assert column_data["description"] == "this is a docs block"
        # Ideally, this would be able to track the doc block in lineage.
        # However, Const jinja nodes are not handled statically for this resolution.
        assert column_data["doc_blocks"] == []
