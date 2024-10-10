import pytest

from dbt.exceptions import ParsingError
from dbt.tests.util import run_dbt

schema_yml_model_no_name = """
data_tests:
  - description: "{{ doc('my_singular_test_documentation') }}"
    config:
      error_if: ">10"
    meta:
      some_key: some_val
"""


class TestParsingErrors:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yml_model_no_name,
        }

    def test_parsing_error_no_entry_name(self, project):
        with pytest.raises(
            ParsingError, match="Entry in 'models/schema.yml' did not contain a name"
        ):
            run_dbt(["parse"])
