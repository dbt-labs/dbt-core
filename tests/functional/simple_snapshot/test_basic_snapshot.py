import os
from datetime import datetime
import pytz
import pytest
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name, write_file
from tests.functional.simple_snapshot.fixtures import (
    models__schema_yml,
    models__schema_with_target_schema_yml,
    models__ref_snapshot_sql,
    seeds__seed_newcol_csv,
    seeds__seed_csv,
    snapshots_pg__snapshot_sql,
    snapshots_pg__snapshot_no_target_schema_sql,
    macros__test_no_overlaps_sql,
    macros_custom_snapshot__custom_sql,
    snapshots_pg_custom_namespaced__snapshot_sql,
    snapshots_pg_custom__snapshot_sql,
)

snapshots_check_col__snapshot_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='check',
            check_cols=['email'],
        )
    }}
    select * from {{target.database}}.{{schema}}.seed

{% endsnapshot %}

{# This should be exactly the same #}
{% snapshot snapshot_checkall %}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='check',
            check_cols='all',
        )
    }}
    select * from {{target.database}}.{{schema}}.seed
{% endsnapshot %}
"""


snapshots_check_col_noconfig__snapshot_sql = """
{% snapshot snapshot_actual %}
    select * from {{target.database}}.{{schema}}.seed
{% endsnapshot %}

{# This should be exactly the same #}
{% snapshot snapshot_checkall %}
    {{ config(check_cols='all') }}
    select * from {{target.database}}.{{schema}}.seed
{% endsnapshot %}
"""


def snapshot_setup(project, num_snapshot_models=1):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(["snapshot"])
    assert len(results) == num_snapshot_models

    run_dbt(["test"])
    check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])

    path = os.path.join(project.test_data_dir, "invalidate_postgres.sql")
    project.run_sql_file(path)

    path = os.path.join(project.test_data_dir, "update.sql")
    project.run_sql_file(path)

    results = run_dbt(["snapshot"])
    assert len(results) == num_snapshot_models

    run_dbt(["test"])
    check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


def ref_setup(project, num_snapshot_models=1):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(["snapshot"])
    assert len(results) == num_snapshot_models

    results = run_dbt(["run"])
    assert len(results) == 1


class Basic:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshots_pg__snapshot_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "ref_snapshot.sql": models__ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}


class TestBasicSnapshot(Basic):
    def test_basic_snapshot(self, project):
        snapshot_setup(project, num_snapshot_models=1)


class TestBasicRef(Basic):
    def test_basic_ref(self, project):
        ref_setup(project, num_snapshot_models=1)


class TestBasicTargetSchemaConfig(Basic):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshots_pg__snapshot_no_target_schema_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "snapshots": {
                "test": {
                    "target_schema": unique_schema + "_alt",
                }
            }
        }

    def test_target_schema(self, project):
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 5
        # ensure that the schema in the snapshot node is the same as target_schema
        snapshot_id = "snapshot.test.snapshot_actual"
        snapshot_node = manifest.nodes[snapshot_id]
        assert snapshot_node.schema == f"{project.test_schema}_alt"
        assert (
            snapshot_node.relation_name
            == f'"{project.database}"."{project.test_schema}_alt"."snapshot_actual"'
        )
        assert snapshot_node.meta == {"owner": "a_owner"}

        # write out schema.yml file and check again
        write_file(models__schema_with_target_schema_yml, "models", "schema.yml")
        manifest = run_dbt(["parse"])
        snapshot_node = manifest.nodes[snapshot_id]
        assert snapshot_node.schema == "schema_from_schema_yml"


class CustomNamespace:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshots_pg_custom_namespaced__snapshot_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "test_no_overlaps.sql": macros__test_no_overlaps_sql,
            "custom.sql": macros_custom_snapshot__custom_sql,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "ref_snapshot.sql": models__ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}


class TestBasicCustomNamespace(CustomNamespace):
    def test_custom_namespace_snapshot(self, project):
        snapshot_setup(project, num_snapshot_models=1)


class TestRefCustomNamespace(CustomNamespace):
    def test_custom_namespace_ref(self, project):
        ref_setup(project, num_snapshot_models=1)


class CustomSnapshot:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshots_pg_custom__snapshot_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "test_no_overlaps.sql": macros__test_no_overlaps_sql,
            "custom.sql": macros_custom_snapshot__custom_sql,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "ref_snapshot.sql": models__ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}


class TestBasicCustomSnapshot(CustomSnapshot):
    def test_custom_snapshot(self, project):
        snapshot_setup(project, num_snapshot_models=1)


class TestRefCustomSnapshot(CustomSnapshot):
    def test_custom_ref(self, project):
        ref_setup(project, num_snapshot_models=1)


class CheckCols:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshots_check_col__snapshot_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "ref_snapshot.sql": models__ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}


class TestBasicCheckCols(CheckCols):
    def test_basic_snapshot(self, project):
        snapshot_setup(project, num_snapshot_models=2)


class TestRefCheckCols(CheckCols):
    def test_check_cols_ref(self, project):
        ref_setup(project, num_snapshot_models=2)


class ConfiguredCheckCols:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshots_check_col_noconfig__snapshot_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "ref_snapshot.sql": models__ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        snapshot_config = {
            "snapshots": {
                "test": {
                    "target_schema": "{{ target.schema }}",
                    "unique_key": "id || '-' || first_name",
                    "strategy": "check",
                    "check_cols": ["email"],
                }
            }
        }
        return snapshot_config


class TestBasicConfiguredCheckCols(ConfiguredCheckCols):
    def test_configured_snapshot(self, project):
        snapshot_setup(project, num_snapshot_models=2)


class TestRefConfiguredCheckCols(ConfiguredCheckCols):
    def test_configured_ref(self, project):
        ref_setup(project, num_snapshot_models=2)


class UpdatedAtCheckCols:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshots_check_col_noconfig__snapshot_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "ref_snapshot.sql": models__ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        snapshot_config = {
            "snapshots": {
                "test": {
                    "target_schema": "{{ target.schema }}",
                    "unique_key": "id || '-' || first_name",
                    "strategy": "check",
                    "check_cols": "all",
                    "updated_at": "updated_at",
                }
            }
        }
        return snapshot_config


class TestBasicUpdatedAtCheckCols(UpdatedAtCheckCols):
    def test_updated_at_snapshot(self, project):
        snapshot_setup(project, num_snapshot_models=2)

        snapshot_expected_relation = relation_from_name(project.adapter, "snapshot_expected")
        revived_records = project.run_sql(
            """
            select id, updated_at, dbt_valid_from from {}
            """.format(
                snapshot_expected_relation
            ),
            fetch="all",
        )
        for result in revived_records:
            # result is a tuple, the updated_at is second and dbt_valid_from is latest
            assert isinstance(result[1], datetime)
            assert isinstance(result[2], datetime)
            assert result[1].replace(tzinfo=pytz.UTC) == result[2].replace(tzinfo=pytz.UTC)


class TestRefUpdatedAtCheckCols(UpdatedAtCheckCols):
    def test_updated_at_ref(self, project):
        ref_setup(project, num_snapshot_models=2)
