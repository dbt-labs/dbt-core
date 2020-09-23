import os
import pytest
from .util import (
    assert_has_threads,
    get_querier,
    get_write_manifest,
    ProjectDefinition,
)

snapshot_data = '''
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select 1 as id, cast('2019-10-31 23:59:40' as timestamp) as updated_at

{% endsnapshot %}
'''

snapshot_data_2 = '''
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select 2 as id, cast('2019-10-31 23:59:40' as timestamp) as updated_at

{% endsnapshot %}
'''


@pytest.mark.supported('postgres')
def test_snapshots(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        snapshots={'my_snapshots.sql': snapshot_data},
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )

    with querier_ctx as querier:
        results = querier.async_wait_for_result(querier.snapshot())
        assert len(results['results']) == 1

        results = querier.async_wait_for_result(querier.snapshot(
            exclude=['snapshot_actual'])
        )

        results = querier.async_wait_for_result(
            querier.snapshot(select=['snapshot_actual'])
        )
        assert len(results['results']) == 1


@pytest.mark.supported('postgres')
def test_snapshots_cli(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        snapshots={'my_snapshots.sql': snapshot_data},
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )

    with querier_ctx as querier:
        results = querier.async_wait_for_result(
            querier.cli_args(cli='snapshot')
        )
        assert len(results['results']) == 1

        results = querier.async_wait_for_result(
            querier.cli_args(cli='snapshot --exclude=snapshot_actual')
        )
        assert len(results['results']) == 0

        results = querier.async_wait_for_result(
            querier.cli_args(cli='snapshot --select=snapshot_actual')
        )
        assert len(results['results']) == 1


@pytest.mark.supported('postgres')
def test_rpc_snapshot_threads(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        snapshots={'my_snapshots.sql': snapshot_data},
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )

    with querier_ctx as querier:
        results = querier.async_wait_for_result(querier.snapshot(threads=5))
        assert_has_threads(results, 5)

        results = querier.async_wait_for_result(
            querier.cli_args('snapshot --threads=7')
        )
        assert_has_threads(results, 7)


@pytest.mark.supported('postgres')
def test_rpc_snapshot_state(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        snapshots={'my_snapshots.sql': snapshot_data},
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )
    with querier_ctx as querier:
        state_dir = os.path.join(project_root, 'state')
        os.makedirs(state_dir)

        results = querier.async_wait_for_result(
            querier.snapshot()
        )
        assert len(results['results']) == 1

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        project.snapshots['my_snapshots.sql'] = snapshot_data_2
        project.write_snapshots(project_root, remove=True)

        querier.sighup()
        assert querier.wait_for_status('ready') is True

        results = querier.async_wait_for_result(
            querier.snapshot(state='./state', select=['state:modified'])
        )
        assert len(results['results']) == 1

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        results = querier.async_wait_for_result(
            querier.snapshot(state='./state', select=['state:modified']),
        )
        assert len(results['results']) == 0