import pytest

import dbt_common.exceptions
from dbt.tests.util import run_dbt

duplicate_doc_blocks_model_sql = "select 1 as id, 'joe' as first_name"

duplicate_doc_blocks_docs_md = """{% docs my_model_doc %}
    a doc string
{% enddocs %}

{% docs my_model_doc %}
    duplicate doc string
{% enddocs %}"""

duplicate_doc_blocks_schema_yml = """version: 2

models:
  - name: model
    description: "{{ doc('my_model_doc') }}"
"""


class TestDuplicateDocsBlock:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": duplicate_doc_blocks_model_sql,
            "schema.yml": duplicate_doc_blocks_schema_yml,
        }

    def test_duplicate_doc_ref(self, project):
        with pytest.raises(dbt_common.exceptions.CompilationError):
            run_dbt(expect_pass=False)
