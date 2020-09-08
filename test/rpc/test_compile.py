import os
import pytest
from .util import (
    assert_has_threads,
    get_querier,
    get_write_manifest,
    ProjectDefinition,
)


@pytest.mark.supported('postgres')
def test_rpc_compile_threads(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'}
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )
    with querier_ctx as querier:
        results = querier.async_wait_for_result(querier.compile(threads=5))
        assert_has_threads(results, 5)

        results = querier.async_wait_for_result(
            querier.cli_args('compile --threads=7')
        )
        assert_has_threads(results, 7)


@pytest.mark.supported('postgres')
def test_rpc_compile_state(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(models={'my_model.sql': 'select 1 as id'})
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
            querier.compile()
        )
        assert len(results['results']) == 1

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        project.models['my_model.sql'] = 'select 2 as id'
        project.write_models(project_root, remove=True)

        querier.sighup()
        assert querier.wait_for_status('ready') is True

        results = querier.async_wait_for_result(
            querier.compile(state='./state', models=['state:modified'])
        )
        assert len(results['results']) == 1

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        results = querier.async_wait_for_result(
            querier.compile(state='./state', models=['state:modified']),
        )
        assert len(results['results']) == 0
