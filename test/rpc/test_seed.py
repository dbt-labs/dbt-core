from .util import (
    assert_has_threads,
    get_querier,
    ProjectDefinition,
)


def test_rpc_seed_threads(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        project_data={'seeds': {'config': {'quote_columns': False}}},
        seeds={'data.csv': 'a,b\n1,hello\n2,goodbye'},
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )

    with querier_ctx as querier:
        results = querier.async_wait_for_result(querier.seed(threads=5))
        assert_has_threads(results, 5)

        results = querier.async_wait_for_result(
            querier.cli_args('seed --threads=7')
        )
        assert_has_threads(results, 7)


def test_rpc_seed_include_exclude(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        project_data={'seeds': {'config': {'quote_columns': False}}},
        seeds={
            'data_1.csv': 'a,b\n1,hello\n2,goodbye',
            'data_2.csv': 'a,b\n1,data',
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
        results = querier.async_wait_for_result(querier.seed(select=['data_1']))
        assert len(results['results']) == 1
        results = querier.async_wait_for_result(querier.seed(select='data_1'))
        assert len(results['results']) == 1
        results = querier.async_wait_for_result(querier.cli_args('seed --select=data_1'))
        assert len(results['results']) == 1

        results = querier.async_wait_for_result(querier.seed(exclude=['data_2']))
        assert len(results['results']) == 1
        results = querier.async_wait_for_result(querier.seed(exclude='data_2'))
        assert len(results['results']) == 1
        results = querier.async_wait_for_result(querier.cli_args('seed --exclude=data_2'))
        assert len(results['results']) == 1
