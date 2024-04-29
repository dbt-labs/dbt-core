import pytest
from dbt.tests.util import run_dbt, get_manifest, write_file


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
        identifier: example
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
        identifier: example
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
        identifier: example
        loaded_at_field: updated_at_another_place
"""


class TestLoadedAtSourceLevel:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": loaded_at_field_null_schema_yml}

    def test_source_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)

        # test setting loaded_at_field at source level, should trickle to table
        assert "source.test.test_source.table1" in manifest.sources
        assert manifest.sources.get("source.test.test_source.table1").loaded_at_field is None

        # test setting loaded_at_field at source level, and explicitly set to
        # null at table level, end up with source level being None
        write_file(
            loaded_at_field_missing_schema_yml, project.project_root, "models", "schema.yml"
        )
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "source.test.test_source.table1" in manifest.sources
        assert (
            manifest.sources.get("source.test.test_source.table1").loaded_at_field == "updated_at"
        )

        # test setting loaded_at_field at table level overrides source level
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
