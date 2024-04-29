import pytest
from dbt.tests.util import run_dbt, get_manifest


loaded_at_field_source_schema_yml = """
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
      - name: table_null
        identifier: example
        loaded_at_field: null
      - name: table_none
        identifier: example
      - name: table_override
        identifier: example
        loaded_at_field: updated_at_another_place
"""


class TestLoadedAtSourceLevel:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": loaded_at_field_source_schema_yml}

    def test_source_level(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)

        # test setting loaded_at_field at source level, should trickle to table
        assert "source.test.test_source.table_null" in manifest.sources
        assert manifest.sources.get("source.test.test_source.table_null").loaded_at_field is None

        # test setting loaded_at_field at source level, and explicitly set to
        # null at table level, end up with source level being None
        assert "source.test.test_source.table_none" in manifest.sources
        assert (
            manifest.sources.get("source.test.test_source.table_none").loaded_at_field
            == "updated_at"
        )

        # test setting loaded_at_field at table level overrides source level
        assert "source.test.test_source.table_override" in manifest.sources
        assert (
            manifest.sources.get("source.test.test_source.table_override").loaded_at_field
            == "updated_at_another_place"
        )
