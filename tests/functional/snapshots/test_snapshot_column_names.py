import datetime
import os

import pytest

from dbt.tests.util import (
    check_relations_equal,
    get_manifest,
    run_dbt,
    run_dbt_and_capture,
    run_sql_with_adapter,
    update_config_file,
)

snapshot_actual_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
        )
    }}

    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
"""

snapshots_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      snapshot_meta_column_names:
          dbt_valid_to: test_valid_to
          dbt_valid_from: test_valid_from
          dbt_scd_id: test_scd_id
          dbt_updated_at: test_updated_at
"""

snapshots_no_column_names_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
"""

ref_snapshot_sql = """
select * from {{ ref('snapshot_actual') }}
"""


invalidate_sql = """
-- update records 11 - 21. Change email and updated_at field
update {schema}.seed set
    updated_at = updated_at + interval '1 hour',
    email      =  case when id = 20 then 'pfoxj@creativecommons.org' else 'new_' || email end
where id >= 10 and id <= 20;


-- invalidate records 11 - 21
update {schema}.snapshot_expected set
    test_valid_to   = updated_at + interval '1 hour'
where id >= 10 and id <= 20;

"""

update_sql = """
-- insert v2 of the 11 - 21 records

insert into {database}.{schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    null::timestamp as test_valid_to,
    updated_at as test_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as test_scd_id
from {database}.{schema}.seed
where id >= 10 and id <= 20;
"""


class TestSnapshotColumnNames:
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
        path = os.path.join(project.test_data_dir, "seed_cn.sql")
        project.run_sql_file(path)
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        project.run_sql(invalidate_sql)
        project.run_sql(update_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # run_dbt(["test"])
        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


class TestSnapshotColumnNamesFromDbtProject:
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
        path = os.path.join(project.test_data_dir, "seed_cn.sql")
        project.run_sql_file(path)
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        project.run_sql(invalidate_sql)
        project.run_sql(update_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # run_dbt(["test"])
        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


class TestSnapshotInvalidColumnNames:
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
        path = os.path.join(project.test_data_dir, "seed_cn.sql")
        project.run_sql_file(path)
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


snapshots_valid_to_current_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      dbt_valid_to_current: "date('2099-12-31')"
      snapshot_meta_column_names:
          dbt_valid_to: test_valid_to
          dbt_valid_from: test_valid_from
          dbt_scd_id: test_scd_id
          dbt_updated_at: test_updated_at
"""

update_with_current_sql = """
-- insert v2 of the 11 - 21 records

insert into {database}.{schema}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    test_valid_from,
    test_valid_to,
    test_updated_at,
    test_scd_id
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as test_valid_from,
    date('2099-12-31') as test_valid_to,
    updated_at as test_updated_at,
    md5(id || '-' || first_name || '|' || updated_at::text) as test_scd_id
from {database}.{schema}.seed
where id >= 10 and id <= 20;
"""


class TestSnapshotDbtValidToCurrent:
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
        path = os.path.join(project.test_data_dir, "seed_dbt_valid_to.sql")
        project.run_sql_file(path)
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        original_snapshot = run_sql_with_adapter(
            project.adapter,
            "select id, test_scd_id, test_valid_to from {database}.{schema}.snapshot_actual",
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
            "select id, test_scd_id, test_valid_to from {database}.{schema}.snapshot_actual",
            "all",
        )
        assert updated_snapshot[0][2] == datetime.datetime(2099, 12, 31, 0, 0)
        # Original row that was updated now has a non-current (2099/12/31) date
        assert updated_snapshot[9][2] == datetime.datetime(2016, 8, 20, 16, 44, 49)
        # Updated row has a current date
        assert updated_snapshot[20][2] == datetime.datetime(2099, 12, 31, 0, 0)

        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])
