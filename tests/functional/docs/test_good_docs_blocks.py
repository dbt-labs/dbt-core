import json
import os
from pathlib import Path
import pytest

from dbt.tests.util import run_dbt, update_config_file, write_file


good_docs_blocks_model_sql = "select 1 as id, 'joe' as first_name"

good_docs_blocks_docs_md = """{% docs my_model_doc %}
My model is just a copy of the seed
{% enddocs %}

{% docs my_model_doc__id %}
The user ID number
{% enddocs %}

The following doc is never used, which should be fine.
{% docs my_model_doc__first_name %}
The user's first name (should not be shown!)
{% enddocs %}

This doc is referenced by its full name
{% docs my_model_doc__last_name %}
The user's last name
{% enddocs %}
"""

good_doc_blocks_alt_docs_md = """{% docs my_model_doc %}
Alt text about the model
{% enddocs %}

{% docs my_model_doc__id %}
The user ID number with alternative text
{% enddocs %}

The following doc is never used, which should be fine.
{% docs my_model_doc__first_name %}
The user's first name - don't show this text!
{% enddocs %}

This doc is referenced by its full name
{% docs my_model_doc__last_name %}
The user's last name in this other file
{% enddocs %}
"""

good_docs_blocks_schema_yml = """version: 2

models:
  - name: model
    description: "{{ doc('my_model_doc') }}"
    columns:
      - name: id
        description: "{{ doc('my_model_doc__id') }}"
      - name: first_name
        description: The user's first name
      - name: last_name
        description: "{{ doc('test', 'my_model_doc__last_name') }}"
"""


class TestGoodDocsBlocks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": good_docs_blocks_model_sql,
            "schema.yml": good_docs_blocks_schema_yml,
            "docs.md": good_docs_blocks_docs_md,
        }

    def test_valid_doc_ref(self, project):
        result = run_dbt()
        assert len(result.results) == 1

        assert os.path.exists("./target/manifest.json")

        with open("./target/manifest.json") as fp:
            manifest = json.load(fp)

        model_data = manifest["nodes"]["model.test.model"]

        assert model_data["description"] == "My model is just a copy of the seed"

        assert {
            "name": "id",
            "description": "The user ID number",
            "data_type": None,
            "constraints": None,
            "constraints_check": None,
            "meta": {},
            "quote": None,
            "tags": [],
        } == model_data["columns"]["id"]

        assert {
            "name": "first_name",
            "description": "The user's first name",
            "data_type": None,
            "constraints": None,
            "constraints_check": None,
            "meta": {},
            "quote": None,
            "tags": [],
        } == model_data["columns"]["first_name"]

        assert {
            "name": "last_name",
            "description": "The user's last name",
            "data_type": None,
            "constraints": None,
            "constraints_check": None,
            "meta": {},
            "quote": None,
            "tags": [],
        } == model_data["columns"]["last_name"]

        assert len(model_data["columns"]) == 3


class TestGoodDocsBlocksAltPath:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": good_docs_blocks_model_sql, "schema.yml": good_docs_blocks_schema_yml}

    def test_alternative_docs_path(self, project):
        # self.use_default_project({"docs-paths": [self.dir("docs")]})
        docs_path = Path(project.project_root, "alt-docs")
        docs_path.mkdir()
        write_file(good_doc_blocks_alt_docs_md, project.project_root, "alt-docs", "docs.md")

        update_config_file(
            {"docs-paths": [str(docs_path)]}, project.project_root, "dbt_project.yml"
        )

        result = run_dbt()

        assert len(result.results) == 1

        assert os.path.exists("./target/manifest.json")

        with open("./target/manifest.json") as fp:
            manifest = json.load(fp)

        model_data = manifest["nodes"]["model.test.model"]

        assert model_data["description"] == "Alt text about the model"

        assert {
            "name": "id",
            "description": "The user ID number with alternative text",
            "data_type": None,
            "constraints": None,
            "constraints_check": None,
            "meta": {},
            "quote": None,
            "tags": [],
        } == model_data["columns"]["id"]

        assert {
            "name": "first_name",
            "description": "The user's first name",
            "data_type": None,
            "constraints": None,
            "constraints_check": None,
            "meta": {},
            "quote": None,
            "tags": [],
        } == model_data["columns"]["first_name"]

        assert {
            "name": "last_name",
            "description": "The user's last name in this other file",
            "data_type": None,
            "constraints": None,
            "constraints_check": None,
            "meta": {},
            "quote": None,
            "tags": [],
        } == model_data["columns"]["last_name"]

        assert len(model_data["columns"]) == 3
