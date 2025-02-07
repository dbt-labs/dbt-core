import pytest

from dbt.tests.util import run_dbt

schema_yml = """
models:
  - name: test_model
    columns:
      - name: test
        doc_blocks: 2
"""


class TestDocBlocksBackCompat:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_model.sql": "select 1 as fun",
            "schema.yml": schema_yml,
        }

    def test_doc_blocks_back_compat(self, project):
        run_dbt(["parse"])
