import pytest
import os
from .util import (
    assert_has_threads,
    get_querier,
    get_write_manifest,
    ProjectDefinition,
)


@pytest.mark.supported('postgres')
def test_rpc_seed_threads(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        project_data={'seeds': {'+quote_columns': False}},
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


@pytest.mark.supported('postgres')
def test_rpc_seed_include_exclude(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(
        project_data={'seeds': {'+quote_columns': False}},
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


@pytest.mark.supported('postgres')
def test_rpc_seed_state(
    project_root, profiles_root, dbt_profile, unique_schema
):
    project = ProjectDefinition(seeds={'my_seed.csv': 'a,b\n1,hello'})
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
            querier.seed()
        )
        assert len(results['results']) == 1

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        project.seeds['my_seed.csv'] = 'a,b\n1,hello\n2,goodbye'
        project.write_seeds(project_root, remove=True)

        querier.sighup()
        assert querier.wait_for_status('ready') is True

        results = querier.async_wait_for_result(
            querier.seed(state='./state', select=['state:modified'])
        )
        assert len(results['results']) == 1

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        results = querier.async_wait_for_result(
            querier.seed(state='./state', select=['state:modified']),
        )
        assert len(results['results']) == 0
