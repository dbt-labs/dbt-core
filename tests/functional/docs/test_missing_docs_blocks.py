import pytest

import dbt_common.exceptions
from dbt.tests.util import run_dbt

missing_docs_blocks_model_sql = "select 1 as id, 'joe' as first_name"

missing_docs_blocks_docs_md = """{% docs my_model_doc %}
My model is just a copy of the seed
{% enddocs %}

{% docs my_model_doc__id %}
The user ID number
{% enddocs %}"""

missing_docs_blocks_schema_yml = """version: 2

models:
  - name: model
    description: "{{ doc('my_model_doc') }}"
    columns:
      - name: id
        description: "{{ doc('my_model_doc__id') }}"
      - name: first_name
      # invalid reference
        description: "{{ doc('my_model_doc__first_name') }}"
"""


class TestMissingDocsBlocks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": missing_docs_blocks_model_sql,
            "schema.yml": missing_docs_blocks_schema_yml,
            "docs.md": missing_docs_blocks_docs_md,
        }

    def test_missing_doc_ref(self, project):
        # The run should fail since we could not find the docs reference.
        with pytest.raises(dbt_common.exceptions.CompilationError):
            run_dbt()
