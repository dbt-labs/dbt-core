import pytest

from dbt.exceptions import CompilationError
from dbt.tests.util import run_dbt

source_dupes_schema_yml = """
version: 2
sources:
  - name: something
    tables:
     - name: dupe
     - name: dupe

"""


class TestDuplicateSourceEnabled:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": source_dupes_schema_yml}

    def test_duplicate_source_enabled(self, project):
        message = "dbt found two sources with the name"
        with pytest.raises(CompilationError) as exc:
            run_dbt(["compile"])
        assert message in str(exc.value)
