import pytest

from dbt.tests.util import run_dbt

model_1 = """
select 1 as id, 'joe' as first_name
"""

model_versioned = """
select 1 as id, 'joe' as first_name
"""

docs_md = """
{% docs model_description %}
unversioned model
{% enddocs %}

{% docs column_id_doc %}
column id for some thing
{% enddocs %}

{% docs versioned_model_description %}
versioned model
{% enddocs %}

"""

schema_yml = """
models:
  - name: model_1
    description: '{{ doc("model_description") }}'
    columns:
        - name: id
          description: '{{ doc("column_id_doc") }}'

  - name: model_versioned
    description: '{{ doc("versioned_model_description") }}'
    latest_version: 1
    versions:
      - v: 1
        config:
          alias: my_alias
        columns:
          - name: id
            description: '{{ doc("column_id_doc") }}'
          - name: first_name
            description: 'plain text'
      - v: 2
        columns:
          - name: other_id
"""


class TestVersionedModelDocsBlock:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": model_1,
            "model_versioned.sql": model_versioned,
            "schema.yml": schema_yml,
            "docs.md": docs_md,
        }

    def test_versioned_doc_ref(self, project):
        manifest = run_dbt(["parse"])
        model_1 = manifest.nodes["model.test.model_1"]
        model_v1 = manifest.nodes["model.test.model_versioned.v1"]

        assert model_1.description == "unversioned model"
        assert model_v1.description == "versioned model"

        assert model_1.columns["id"].description == "column id for some thing"
        assert model_v1.columns["id"].description == "column id for some thing"
        assert model_v1.columns["first_name"].description == "plain text"
