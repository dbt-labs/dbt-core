import pytest
from datetime import datetime, timedelta
from .util import (
    get_querier,
    ProjectDefinition,
)

source_freshness_schema_yml = '''
version: 2
sources:
  - name: test_source
    loaded_at_field: b
    schema: {schema}
    freshness:
      warn_after: {{count: 10, period: hour}}
      error_after: {{count: 1, period: day}}
    tables:
      - name: test_table
        identifier: source
      - name: failure_table
        identifier: other_source
'''

@pytest.mark.supported('postgres')
def test_source_snapshot_freshness(
    project_root, profiles_root, dbt_profile, unique_schema
):
    start_time = datetime.utcnow()
    warn_me = start_time - timedelta(hours=18)
    error_me = start_time - timedelta(days=2)
    # this should trigger a 'warn'
    project = ProjectDefinition(
        project_data={'seeds': {'config': {'quote_columns': False}}},
        seeds={
            'source.csv': 'a,b\n1,{}\n'.format(error_me.strftime('%Y-%m-%d %H:%M:%S')),
            'other_source.csv': 'a,b\n1,{}\n'.format(error_me.strftime('%Y-%m-%d %H:%M:%S'))
        },
        models={
            'sources.yml': source_freshness_schema_yml.format(schema=unique_schema),
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
        seeds = querier.async_wait_for_result(querier.seed())
        assert len(seeds['results']) == 2
        # should error
        error_results = querier.async_wait_for_result(querier.snapshot_freshness(), state='failed')
        assert len(error_results['results']) == 2
        for result in error_results['results']:
            assert result['status'] == 'error'
        error_results = querier.async_wait_for_result(querier.cli_args('source snapshot-freshness'), state='failed')
        assert len(error_results['results']) == 2
        for result in error_results['results']:
            assert result['status'] == 'error'

        project.seeds['source.csv'] += '2,{}\n'.format(warn_me.strftime('%Y-%m-%d %H:%M:%S'))
        project.write_seeds(project_root, remove=True)
        querier.async_wait_for_result(querier.seed())
        # should warn
        warn_results = querier.async_wait_for_result(querier.snapshot_freshness(select='source:test_source.test_table'))
        assert len(warn_results['results']) == 1
        assert warn_results['results'][0]['status'] == 'warn'
        warn_results = querier.async_wait_for_result(querier.cli_args('source snapshot-freshness -s source:test_source.test_table'))
        assert len(warn_results['results']) == 1
        assert warn_results['results'][0]['status'] == 'warn'

        project.seeds['source.csv'] += '3,{}\n'.format(start_time.strftime('%Y-%m-%d %H:%M:%S'))
        project.write_seeds(project_root, remove=True)
        querier.async_wait_for_result(querier.seed())
        # should pass!
        pass_results = querier.async_wait_for_result(querier.snapshot_freshness(select=['source:test_source.test_table']))
        assert len(pass_results['results']) == 1
        assert pass_results['results'][0]['status'] == 'pass'
        pass_results = querier.async_wait_for_result(querier.cli_args('source snapshot-freshness --select source:test_source.test_table'))
        assert len(pass_results['results']) == 1
        assert pass_results['results'][0]['status'] == 'pass'

@pytest.mark.supported('postgres')
def test_source_freshness(
    project_root, profiles_root, dbt_profile, unique_schema
):
    start_time = datetime.utcnow()
    warn_me = start_time - timedelta(hours=18)
    error_me = start_time - timedelta(days=2)
    # this should trigger a 'warn'
    project = ProjectDefinition(
        project_data={'seeds': {'config': {'quote_columns': False}}},
        seeds={
            'source.csv': 'a,b\n1,{}\n'.format(error_me.strftime('%Y-%m-%d %H:%M:%S')),
            'other_source.csv': 'a,b\n1,{}\n'.format(error_me.strftime('%Y-%m-%d %H:%M:%S'))
        },
        models={
            'sources.yml': source_freshness_schema_yml.format(schema=unique_schema),
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
        seeds = querier.async_wait_for_result(querier.seed())
        assert len(seeds['results']) == 2
        # should error
        error_results = querier.async_wait_for_result(querier.source_freshness(), state='failed')
        assert len(error_results['results']) == 2
        for result in error_results['results']:
            assert result['status'] == 'error'
        error_results = querier.async_wait_for_result(querier.cli_args('source freshness'), state='failed')
        assert len(error_results['results']) == 2
        for result in error_results['results']:
            assert result['status'] == 'error'

        project.seeds['source.csv'] += '2,{}\n'.format(warn_me.strftime('%Y-%m-%d %H:%M:%S'))
        project.write_seeds(project_root, remove=True)
        querier.async_wait_for_result(querier.seed())
        # should warn
        warn_results = querier.async_wait_for_result(querier.source_freshness(select='source:test_source.test_table'))
        assert len(warn_results['results']) == 1
        assert warn_results['results'][0]['status'] == 'warn'
        warn_results = querier.async_wait_for_result(querier.cli_args('source freshness -s source:test_source.test_table'))
        assert len(warn_results['results']) == 1
        assert warn_results['results'][0]['status'] == 'warn'

        project.seeds['source.csv'] += '3,{}\n'.format(start_time.strftime('%Y-%m-%d %H:%M:%S'))
        project.write_seeds(project_root, remove=True)
        querier.async_wait_for_result(querier.seed())
        # should pass!
        pass_results = querier.async_wait_for_result(querier.source_freshness(select=['source:test_source.test_table']))
        assert len(pass_results['results']) == 1
        assert pass_results['results'][0]['status'] == 'pass'
        pass_results = querier.async_wait_for_result(querier.cli_args('source freshness --select source:test_source.test_table'))
        assert len(pass_results['results']) == 1
        assert pass_results['results'][0]['status'] == 'pass'
