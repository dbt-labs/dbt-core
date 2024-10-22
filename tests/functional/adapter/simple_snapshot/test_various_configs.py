import datetime

import pytest

from dbt.tests.util import (
    check_relations_equal,
    get_manifest,
    run_dbt,
    run_dbt_and_capture,
    run_sql_with_adapter,
    update_config_file,
)
from tests.functional.adapter.simple_snapshot.fixtures import (
    create_multi_key_seed_sql,
    create_multi_key_snapshot_expected_sql,
    create_seed_sql,
    create_snapshot_expected_sql,
    invalidate_multi_key_sql,
    invalidate_sql,
    model_seed_sql,
    populate_multi_key_snapshot_expected_sql,
    populate_snapshot_expected_sql,
    populate_snapshot_expected_valid_to_current_sql,
    ref_snapshot_sql,
    seed_insert_sql,
    seed_multi_key_insert_sql,
    snapshot_actual_sql,
    snapshots_multi_key_yml,
    snapshots_no_column_names_yml,
    snapshots_valid_to_current_yml,
    snapshots_yml,
    update_multi_key_sql,
    update_sql,
    update_with_current_sql,
)


class BaseSnapshotColumnNames:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": snapshots_yml,
            "ref_snapshot.sql": ref_snapshot_sql,
        }

    def test_snapshot_column_names(self, project):
        project.run_sql(create_seed_sql)
        project.run_sql(create_snapshot_expected_sql)
        project.run_sql(seed_insert_sql)
        project.run_sql(populate_snapshot_expected_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        project.run_sql(invalidate_sql)
        project.run_sql(update_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # run_dbt(["test"])
        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


class TestSnapshotColumnNames(BaseSnapshotColumnNames):
    pass


class BaseSnapshotColumnNamesFromDbtProject:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": snapshots_no_column_names_yml,
            "ref_snapshot.sql": ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "snapshots": {
                "test": {
                    "+snapshot_meta_column_names": {
                        "dbt_valid_to": "test_valid_to",
                        "dbt_valid_from": "test_valid_from",
                        "dbt_scd_id": "test_scd_id",
                        "dbt_updated_at": "test_updated_at",
                    }
                }
            }
        }

    def test_snapshot_column_names_from_project(self, project):
        project.run_sql(create_seed_sql)
        project.run_sql(create_snapshot_expected_sql)
        project.run_sql(seed_insert_sql)
        project.run_sql(populate_snapshot_expected_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        project.run_sql(invalidate_sql)
        project.run_sql(update_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # run_dbt(["test"])
        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


class TestSnapshotColumnNamesFromDbtProject(BaseSnapshotColumnNamesFromDbtProject):
    pass


class BaseSnapshotInvalidColumnNames:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": snapshots_no_column_names_yml,
            "ref_snapshot.sql": ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "snapshots": {
                "test": {
                    "+snapshot_meta_column_names": {
                        "dbt_valid_to": "test_valid_to",
                        "dbt_valid_from": "test_valid_from",
                        "dbt_scd_id": "test_scd_id",
                        "dbt_updated_at": "test_updated_at",
                    }
                }
            }
        }

    def test_snapshot_invalid_column_names(self, project):
        project.run_sql(create_seed_sql)
        project.run_sql(create_snapshot_expected_sql)
        project.run_sql(seed_insert_sql)
        project.run_sql(populate_snapshot_expected_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        snapshot_node = manifest.nodes["snapshot.test.snapshot_actual"]
        snapshot_node.config.snapshot_meta_column_names == {
            "dbt_valid_to": "test_valid_to",
            "dbt_valid_from": "test_valid_from",
            "dbt_scd_id": "test_scd_id",
            "dbt_updated_at": "test_updated_at",
        }

        project.run_sql(invalidate_sql)
        project.run_sql(update_sql)

        # Change snapshot_meta_columns and look for an error
        different_columns = {
            "snapshots": {
                "test": {
                    "+snapshot_meta_column_names": {
                        "dbt_valid_to": "test_valid_to",
                        "dbt_updated_at": "test_updated_at",
                    }
                }
            }
        }
        update_config_file(different_columns, "dbt_project.yml")

        results, log_output = run_dbt_and_capture(["snapshot"], expect_pass=False)
        assert len(results) == 1
        assert "Compilation Error in snapshot snapshot_actual" in log_output
        assert "Snapshot target is missing configured columns" in log_output


class TestSnapshotInvalidColumnNames(BaseSnapshotInvalidColumnNames):
    pass


class BaseSnapshotDbtValidToCurrent:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": snapshots_valid_to_current_yml,
            "ref_snapshot.sql": ref_snapshot_sql,
        }

    def test_valid_to_current(self, project):
        project.run_sql(create_seed_sql)
        project.run_sql(create_snapshot_expected_sql)
        project.run_sql(seed_insert_sql)
        project.run_sql(populate_snapshot_expected_valid_to_current_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        original_snapshot = run_sql_with_adapter(
            project.adapter,
            "select id, test_scd_id, test_valid_to from {schema}.snapshot_actual",
            "all",
        )
        assert original_snapshot[0][2] == datetime.datetime(2099, 12, 31, 0, 0)
        assert original_snapshot[9][2] == datetime.datetime(2099, 12, 31, 0, 0)

        project.run_sql(invalidate_sql)
        project.run_sql(update_with_current_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        updated_snapshot = run_sql_with_adapter(
            project.adapter,
            "select id, test_scd_id, test_valid_to from {schema}.snapshot_actual",
            "all",
        )
        assert updated_snapshot[0][2] == datetime.datetime(2099, 12, 31, 0, 0)
        # Original row that was updated now has a non-current (2099/12/31) date
        assert updated_snapshot[9][2] == datetime.datetime(2016, 8, 20, 16, 44, 49)
        # Updated row has a current date
        assert updated_snapshot[20][2] == datetime.datetime(2099, 12, 31, 0, 0)

        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


class TestSnapshotDbtValidToCurrent(BaseSnapshotDbtValidToCurrent):
    pass


# This uses snapshot_meta_column_names, yaml-only snapshot def,
# and multiple keys
class BaseSnapshotMultiUniqueKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "seed.sql": model_seed_sql,
            "snapshots.yml": snapshots_multi_key_yml,
            "ref_snapshot.sql": ref_snapshot_sql,
        }

    def test_multi_column_unique_key(self, project):
        project.run_sql(create_multi_key_seed_sql)
        project.run_sql(create_multi_key_snapshot_expected_sql)
        project.run_sql(seed_multi_key_insert_sql)
        project.run_sql(populate_multi_key_snapshot_expected_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        project.run_sql(invalidate_multi_key_sql)
        project.run_sql(update_multi_key_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # run_dbt(["test"])
        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


class TestSnapshotMultiUniqueKey(BaseSnapshotMultiUniqueKey):
    pass
