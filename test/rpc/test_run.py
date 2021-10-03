import os
import pytest
from .util import (
    assert_has_threads,
    get_querier,
    get_write_manifest,
    ProjectDefinition,
)


@pytest.mark.supported('postgres')
def test_rpc_run_threads(
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
        results = querier.async_wait_for_result(querier.run(threads=5))
        assert_has_threads(results, 5)

        results = querier.async_wait_for_result(
            querier.cli_args('run --threads=7')
        )
        assert_has_threads(results, 7)


@pytest.mark.supported('postgres')
def test_rpc_run_vars(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        models={
            'my_model.sql': 'select {{ var("param") }} as id',
        },
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )

    with querier_ctx as querier:
        results = querier.async_wait_for_result(querier.cli_args('run --vars "{param: 100}"'))
        assert len(results['results']) == 1
        assert results['results'][0]['node']['compiled_sql'] == 'select 100 as id'


@pytest.mark.supported('postgres')
def test_rpc_run_vars_compiled(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        models={
            'my_model.sql': '{{ config(materialized=var("materialized_var", "view")) }} select 1 as id',
        },
    )

    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )
    with querier_ctx as querier:
        results = querier.async_wait_for_result(querier.cli_args('--no-partial-parse run --vars "{materialized_var: table}"'))
        assert len(results['results']) == 1
        assert results['results'][0]['node']['config']['materialized'] == 'table'
        # make sure that `--vars` doesn't update global state - if it does,
        # this run() will result in a view!
        results = querier.async_wait_for_result(querier.cli_args('--no-partial-parse run'))
        assert len(results['results']) == 1
        assert results['results'][0]['node']['config']['materialized'] == 'view'


@pytest.mark.supported('postgres')
def test_rpc_run_state_defer(
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
            querier.run()
        )
        assert len(results['results']) == 1

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        project.models['my_model.sql'] = 'select 2 as id'
        project.write_models(project_root, remove=True)
        querier.sighup()
        assert querier.wait_for_status('ready') is True

        results = querier.async_wait_for_result(
            querier.run(state='./state', models=['state:modified'])
        )
        assert len(results['results']) == 1

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        results = querier.async_wait_for_result(
            querier.run(state='./state', models=['state:modified']),
        )
        assert len(results['results']) == 0

        project.models['my_model.sql'] = '{% if execute %}{% do exceptions.raise_compiler_error("should not see this") %}{% endif %}select 2 as id'
        project.models['my_second_model.sql'] = 'select * from {{ ref("my_model") }}'
        project.write_models(project_root, remove=True)
        querier.sighup()
        assert querier.wait_for_status('ready') is True

        # if 'defer' is ignored, this will fail
        results = querier.async_wait_for_result(
            querier.run(state='./state', models=['my_second_model'], defer=True)
        )
        assert len(results['results']) == 1
