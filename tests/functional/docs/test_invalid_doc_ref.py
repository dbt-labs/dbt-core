import pytest

import dbt_common.exceptions
from dbt.tests.util import run_dbt

invalid_doc_ref_model_sql = "select 1 as id, 'joe' as first_name"

invalid_doc_ref_docs_md = """{% docs my_model_doc %}
My model is just a copy of the seed
{% enddocs %}

{% docs my_model_doc__id %}
The user ID number
{% enddocs %}

The following doc is never used, which should be fine.
{% docs my_model_doc__first_name %}
The user's first name
{% enddocs %}"""

invalid_doc_ref_schema_yml = """version: 2

models:
  - name: model
    description: "{{ doc('my_model_doc') }}"
    columns:
      - name: id
        description: "{{ doc('my_model_doc__id') }}"
      - name: first_name
        description: "{{ doc('foo.bar.my_model_doc__id') }}"
"""


class TestInvalidDocRef:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": invalid_doc_ref_model_sql,
            "docs.md": invalid_doc_ref_docs_md,
            "schema.yml": invalid_doc_ref_schema_yml,
        }

    def test_invalid_doc_ref(self, project):
        # The run should fail since we could not find the docs reference.
        with pytest.raises(dbt_common.exceptions.CompilationError):
            run_dbt(expect_pass=False)
