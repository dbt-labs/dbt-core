import os
import pytest
from dbt.tests.util import run_dbt
from dbt.tests.tables import TableComparison
from tests.functional.simple_snapshot.fixtures import (
    seeds__seed_newcol_csv,
    seeds__seed_csv,
    models__schema_yml,
    models__ref_snapshot_sql,
    macros__test_no_overlaps_sql,
    snapshots_pg__snapshot_sql,
    snapshots_select__snapshot_sql,
    snapshots_select_noconfig__snapshot_sql,
)


def all_snapshots(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)

    results = run_dbt(["snapshot"])
    assert len(results) == 4

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("snapshot_castillo", "snapshot_castillo_expected")
    table_comp.assert_tables_equal("snapshot_alvarez", "snapshot_alvarez_expected")
    table_comp.assert_tables_equal("snapshot_kelly", "snapshot_kelly_expected")
    table_comp.assert_tables_equal("snapshot_actual", "snapshot_expected")

    path = os.path.join(project.test_data_dir, "invalidate_postgres.sql")
    project.run_sql_file(path)

    path = os.path.join(project.test_data_dir, "update.sql")
    project.run_sql_file(path)

    results = run_dbt(["snapshot"])
    assert len(results) == 4
    table_comp.assert_tables_equal("snapshot_castillo", "snapshot_castillo_expected")
    table_comp.assert_tables_equal("snapshot_alvarez", "snapshot_alvarez_expected")
    table_comp.assert_tables_equal("snapshot_kelly", "snapshot_kelly_expected")
    table_comp.assert_tables_equal("snapshot_actual", "snapshot_expected")


def exclude_snapshots(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(["snapshot", "--exclude", "snapshot_castillo"])
    assert len(results) == 3

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_table_does_not_exist("snapshot_castillo")
    table_comp.assert_tables_equal("snapshot_alvarez", "snapshot_alvarez_expected")
    table_comp.assert_tables_equal("snapshot_kelly", "snapshot_kelly_expected")
    table_comp.assert_tables_equal("snapshot_actual", "snapshot_expected")


def select_snapshots(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(["snapshot", "--select", "snapshot_castillo"])
    assert len(results) == 1

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("snapshot_castillo", "snapshot_castillo_expected")
    table_comp.assert_table_does_not_exist("snapshot_alvarez")
    table_comp.assert_table_does_not_exist("snapshot_kelly")
    table_comp.assert_table_does_not_exist("snapshot_actual")


# all of the tests below use one of both of the above tests with
# various combinations of snapshots and macros
class SelectBasicSetup:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot.sql": snapshots_pg__snapshot_sql,
            "snapshot_select.sql": snapshots_select__snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "ref_snapshot.sql": models__ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}


class TestAllBasic(SelectBasicSetup):
    def test_all_snapshots(self, project):
        all_snapshots(project)


class TestExcludeBasic(SelectBasicSetup):
    def test_exclude_snapshots(self, project):
        exclude_snapshots(project)


class TestSelectBasic(SelectBasicSetup):
    def test_select_snapshots(self, project):
        select_snapshots(project)


class SelectConfiguredSetup:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshots_select_noconfig__snapshot_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "ref_snapshot.sql": models__ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}

    # TODO: don't have access to project here so this breaks
    @pytest.fixture(scope="class")
    def project_config_update(self):
        snapshot_config = {
            "snapshots": {
                "test": {
                    "target_schema": "{{ target.schema }}",
                    "unique_key": "id || '-' || first_name",
                    "strategy": "timestamp",
                    "updated_at": "updated_at",
                }
            }
        }
        return snapshot_config


class TestConfigured(SelectConfiguredSetup):
    def test_all_configured_snapshots(self, project):
        all_snapshots(project)


class TestConfiguredExclude(SelectConfiguredSetup):
    def test_exclude_configured_snapshots(self, project):
        exclude_snapshots(project)


class TestConfiguredSelect(SelectConfiguredSetup):
    def test_select_configured_snapshots(self, project):
        select_snapshots(project)
