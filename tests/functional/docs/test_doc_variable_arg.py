import pytest

from dbt.exceptions import DocTargetNotFoundError
from dbt.tests.util import run_dbt

schema_yml = """
models:
  - name: my_model
    description: "{{ doc(my_variable) }}"
    columns:
      - name: id
        description: "{{ doc(some_var) }}"
"""


class TestDocVariableArg:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as id",
            "schema.yml": schema_yml,
        }

    def test_variable_arg_raises_doc_not_found_not_attribute_error(self, project):
        with pytest.raises(DocTargetNotFoundError):
            run_dbt(["parse"])
