from .util import (
    assert_has_threads,
    get_querier,
    ProjectDefinition,
)


def test_rpc_run_threads(
    project_root, profiles_root, postgres_profile, unique_schema
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


def test_rpc_run_vars(
    project_root, profiles_root, postgres_profile, unique_schema
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


def test_rpc_run_vars_compiled(
    project_root, profiles_root, postgres_profile, unique_schema
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
        results = querier.async_wait_for_result(querier.cli_args('run --vars "{materialized_var: table}"'))
        assert len(results['results']) == 1
        assert results['results'][0]['node']['config']['materialized'] == 'table'
        # make sure that `--vars` doesn't update global state - if it does,
        # this run() will result in a view!
        results = querier.async_wait_for_result(querier.cli_args('run'))
        assert len(results['results']) == 1
        assert results['results'][0]['node']['config']['materialized'] == 'view'
