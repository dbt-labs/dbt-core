import pytest

from dbt.exceptions import YamlParseDictError
from dbt.tests.util import get_manifest, run_dbt, write_file

loaded_at_field_null_schema_yml = """
sources:
  - name: test_source
    freshness:
      warn_after:
        count: 1
        period: day
      error_after:
        count: 4
        period: day
    loaded_at_field: updated_at
    tables:
      - name: table1
        loaded_at_field: null
"""

loaded_at_field_blank_schema_yml = """
sources:
  - name: test_source
    freshness:
      warn_after:
        count: 1
        period: day
      error_after:
        count: 4
        period: day
    loaded_at_field: updated_at
    tables:
      - name: table1
        loaded_at_field: null
"""

loaded_at_field_missing_schema_yml = """
sources:
  - name: test_source
    freshness:
      warn_after:
        count: 1
        period: day
      error_after:
        count: 4
        period: day
    loaded_at_field: updated_at
    tables:
      - name: table1
"""

loaded_at_field_defined_schema_yml = """
sources:
  - name: test_source
    freshness:
      warn_after:
        count: 1
        period: day
      error_after:
        count: 4
        period: day
    loaded_at_field: updated_at
    tables:
      - name: table1
        loaded_at_field: updated_at_another_place
"""

loaded_at_field_empty_string_schema_yml = """
sources:
  - name: test_source
    freshness:
      warn_after:
        count: 1
        period: day
      error_after:
        count: 4
        period: day
    loaded_at_field: updated_at
    tables:
      - name: table1
        loaded_at_field: ""
"""


class TestParsingLoadedAtField:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": loaded_at_field_null_schema_yml}

    def test_loaded_at_field(self, project):
        # test setting loaded_at_field to null explicitly at table level
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)

        assert "source.test.test_source.table1" in manifest.sources
        assert manifest.sources.get("source.test.test_source.table1").loaded_at_field is None

        # test setting loaded_at_field at source level, do not set at table level
        # end up with source level loaded_at_field
        write_file(
            loaded_at_field_missing_schema_yml, project.project_root, "models", "schema.yml"
        )
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.table1" in manifest.sources
        assert (
            manifest.sources.get("source.test.test_source.table1").loaded_at_field == "updated_at"
        )

        # test setting loaded_at_field to nothing, should override Source value for None
        write_file(loaded_at_field_blank_schema_yml, project.project_root, "models", "schema.yml")
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)

        assert "source.test.test_source.table1" in manifest.sources
        assert manifest.sources.get("source.test.test_source.table1").loaded_at_field is None

        # test setting loaded_at_field at table level to a value - it should override source level
        write_file(
            loaded_at_field_defined_schema_yml, project.project_root, "models", "schema.yml"
        )
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.table1" in manifest.sources
        assert (
            manifest.sources.get("source.test.test_source.table1").loaded_at_field
            == "updated_at_another_place"
        )

        # test setting loaded_at_field at table level to an empty string - should error
        write_file(
            loaded_at_field_empty_string_schema_yml, project.project_root, "models", "schema.yml"
        )
        with pytest.raises(YamlParseDictError):
            run_dbt(["parse"])
