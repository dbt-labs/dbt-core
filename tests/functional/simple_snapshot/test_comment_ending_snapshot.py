import pytest
from dbt.tests.util import run_dbt
import os
from tests.functional.simple_snapshot.fixtures import (
    models__schema_yml,
    models__ref_snapshot_sql,
    macros__test_no_overlaps_sql,
    seeds__seed_csv,
)

snapshots_with_comment_at_end__snapshot_sql = """
{% snapshot snapshot_actual %}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['first_name'],
        )
    }}
    select * from {{target.database}}.{{schema}}.seed
    -- Test comment to prevent reccurence of https://github.com/dbt-labs/dbt-core/issues/6781
{% endsnapshot %}
"""


class SnapshotsWithCommentAtEnd:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshots_with_comment_at_end__snapshot_sql}

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
        return {"seed.csv": seeds__seed_csv}


class TestSnapshotsWithCommentAtEnd(SnapshotsWithCommentAtEnd):
    def test_comment_ending(self, project):
        path = os.path.join(project.test_data_dir, "seed_pg.sql")
        project.run_sql_file(path)
        # N.B. Snapshot is run twice to ensure snapshot_check_all_get_existing_columns is fully run
        # (it exits early if the table doesn't already exist)
        results = run_dbt(["snapshot"])
        results = run_dbt(["snapshot"])
        assert len(results) == 1
