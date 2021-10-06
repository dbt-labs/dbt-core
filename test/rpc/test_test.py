import os
import pytest
import yaml
from .util import (
    assert_has_threads,
    get_querier,
    get_write_manifest,
    ProjectDefinition,
)


@pytest.mark.supported('postgres')
def test_rpc_test_threads(
    project_root, profiles_root, dbt_profile, unique_schema
):
    schema_yaml = {
        'version': 2,
        'models': [{
            'name': 'my_model',
            'columns': [
                {
                    'name': 'id',
                    'tests': ['not_null', 'unique'],
                },
            ],
        }],
    }
    project = ProjectDefinition(
        models={
            'my_model.sql': 'select 1 as id',
            'schema.yml': yaml.safe_dump(schema_yaml)}
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )
    with querier_ctx as querier:
        # first run dbt to get the model built
        querier.async_wait_for_result(querier.run())

        results = querier.async_wait_for_result(querier.test(threads=5))
        assert_has_threads(results, 5)

        results = querier.async_wait_for_result(
            querier.cli_args('test --threads=7')
        )
        assert_has_threads(results, 7)


@pytest.mark.supported('postgres')
def test_rpc_test_state(
    project_root, profiles_root, dbt_profile, unique_schema
):
    schema_yaml = {
        'version': 2,
        'models': [{
            'name': 'my_model',
            'columns': [
                {
                    'name': 'id',
                    'tests': ['not_null', 'unique'],
                },
            ],
        }],
    }
    project = ProjectDefinition(
        models={
            'my_model.sql': 'select 1 as id',
            'schema.yml': yaml.safe_dump(schema_yaml)
        }
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

        results = querier.async_wait_for_result(querier.run())
        results = querier.async_wait_for_result(querier.test())
        assert len(results['results']) == 2

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        project.models['my_model.sql'] = 'select 2 as id'
        project.write_models(project_root, remove=True)
        querier.sighup()
        assert querier.wait_for_status('ready') is True

        results = querier.async_wait_for_result(
            querier.test(state='./state', models=['state:modified'])
        )
        assert len(results['results']) == 2

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        results = querier.async_wait_for_result(
            querier.test(state='./state', models=['state:modified']),
        )
        assert len(results['results']) == 0

        # a better test of defer would require multiple targets
        results = querier.async_wait_for_result(
            querier.run(state='./state', models=['state:modified'], defer=True)
        )
        assert len(results['results']) == 0
