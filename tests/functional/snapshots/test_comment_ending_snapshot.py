import os

import pytest

from dbt.tests.util import run_dbt

snapshots_with_comment_at_end__snapshot_sql = """
{% snapshot snapshot_actual %}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['email'],
        )
    }}
    select * from {{target.database}}.{{schema}}.seed
    -- Test comment to prevent recurrence of https://github.com/dbt-labs/dbt-core/issues/6781
{% endsnapshot %}
"""


class TestSnapshotsWithCommentAtEnd:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshots_with_comment_at_end__snapshot_sql}

    def test_comment_ending(self, project):
        path = os.path.join(project.test_data_dir, "seed_pg.sql")
        project.run_sql_file(path)
        # N.B. Snapshot is run twice to ensure snapshot_check_all_get_existing_columns is fully run
        # (it exits early if the table doesn't already exist)
        run_dbt(["snapshot"])
        results = run_dbt(["snapshot"])
        assert len(results) == 1
