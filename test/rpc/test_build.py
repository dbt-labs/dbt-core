import os
import pytest
import yaml
from .util import (
    assert_has_threads,
    get_querier,
    get_write_manifest,
    ProjectDefinition,
)

snapshot_data = '''
{% snapshot my_snapshot %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select id, cast('2019-10-31 23:59:40' as timestamp) as updated_at
    from {{ ref('my_model') }}

{% endsnapshot %}
'''


@pytest.mark.supported('postgres')
def test_rpc_build_threads(
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
        project_data={'seeds': {'+quote_columns': False}},
        models={
            'my_model.sql': 'select * from {{ ref("data") }}',
            'schema.yml': yaml.safe_dump(schema_yaml)
        },
        seeds={'data.csv': 'id,message\n1,hello\n2,goodbye'},
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
        # first run dbt to get the model built
        querier.async_wait_for_result(querier.build())

        results = querier.async_wait_for_result(querier.test(threads=5))
        assert_has_threads(results, 5)

        results = querier.async_wait_for_result(
            querier.cli_args('build --threads=7')
        )
        assert_has_threads(results, 7)


@pytest.mark.supported('postgres')
def test_rpc_build_state(
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
        project_data={'seeds': {'+quote_columns': False}},
        models={
            'my_model.sql': 'select * from {{ ref("data") }}',
            'schema.yml': yaml.safe_dump(schema_yaml)
        },
        seeds={'data.csv': 'id,message\n1,hello\n2,goodbye'},
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

        results = querier.async_wait_for_result(querier.build())
        assert len(results['results']) == 5

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        project.models['my_model.sql'] =\
            'select * from {{ ref("data" )}} where id = 2'
        project.write_models(project_root, remove=True)
        querier.sighup()
        assert querier.wait_for_status('ready') is True

        results = querier.async_wait_for_result(
            querier.build(state='./state', select=['state:modified'])
        )
        assert len(results['results']) == 3

        get_write_manifest(querier, os.path.join(state_dir, 'manifest.json'))

        results = querier.async_wait_for_result(
            querier.build(state='./state', select=['state:modified']),
        )
        assert len(results['results']) == 0

        # a better test of defer would require multiple targets
        results = querier.async_wait_for_result(
            querier.build(
                state='./state',
                select=['state:modified'],
                defer=True
            )
        )
        assert len(results['results']) == 0


@pytest.mark.supported('postgres')
def test_rpc_build_selectors(
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
        name='test',
        project_data={
            'seeds': {'+quote_columns': False},
            'models': {'test': {'my_model': {'+tags': 'example_tag'}}}
        },
        models={
            'my_model.sql': 'select * from {{ ref("data") }}',
            'schema.yml': yaml.safe_dump(schema_yaml)
        },
        seeds={'data.csv': 'id,message\n1,hello\n2,goodbye'},
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
        # test simple resource_types param
        results = querier.async_wait_for_result(
            querier.build(resource_types=['seed'])
        )
        assert len(results['results']) == 1
        assert results['results'][0]['node']['resource_type'] == 'seed'

        # test simple select param (should select tagged model and its tests)
        results = querier.async_wait_for_result(
            querier.build(select=['tag:example_tag'])
        )
        assert len(results['results']) == 3
        assert sorted(
            [result['node']['resource_type'] for result in results['results']]
        ) == ['model', 'test', 'test']
