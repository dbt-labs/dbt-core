import os
import pytest
from dbt.tests.util import run_dbt, check_relations_equal, check_table_does_not_exist
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

    check_relations_equal(project.adapter, ["snapshot_castillo", "snapshot_castillo_expected"])
    check_relations_equal(project.adapter, ["snapshot_alvarez", "snapshot_alvarez_expected"])
    check_relations_equal(project.adapter, ["snapshot_kelly", "snapshot_kelly_expected"])
    check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])

    path = os.path.join(project.test_data_dir, "invalidate_postgres.sql")
    project.run_sql_file(path)

    path = os.path.join(project.test_data_dir, "update.sql")
    project.run_sql_file(path)

    results = run_dbt(["snapshot"])
    assert len(results) == 4
    check_relations_equal(project.adapter, ["snapshot_castillo", "snapshot_castillo_expected"])
    check_relations_equal(project.adapter, ["snapshot_alvarez", "snapshot_alvarez_expected"])
    check_relations_equal(project.adapter, ["snapshot_kelly", "snapshot_kelly_expected"])
    check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


def exclude_snapshots(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(["snapshot", "--exclude", "snapshot_castillo"])
    assert len(results) == 3

    check_table_does_not_exist(project.adapter, "snapshot_castillo")
    check_relations_equal(project.adapter, ["snapshot_alvarez", "snapshot_alvarez_expected"])
    check_relations_equal(project.adapter, ["snapshot_kelly", "snapshot_kelly_expected"])
    check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


def select_snapshots(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(["snapshot", "--select", "snapshot_castillo"])
    assert len(results) == 1

    check_relations_equal(project.adapter, ["snapshot_castillo", "snapshot_castillo_expected"])
    check_table_does_not_exist(project.adapter, "snapshot_alvarez")
    check_table_does_not_exist(project.adapter, "snapshot_kelly")
    check_table_does_not_exist(project.adapter, "snapshot_actual")


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
