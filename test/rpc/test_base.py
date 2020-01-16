# flake8: disable=redefined-outer-name
from datetime import datetime, timedelta
import time
import yaml
from .util import (
    ProjectDefinition, get_querier,
)


def test_rpc_basics(
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
        querier.async_wait_for_result(querier.run_sql('select 1 as id'))

        querier.async_wait_for_result(querier.run())

        querier.async_wait_for_result(
            querier.run_sql('select * from {{ ref("my_model") }}')
        )

        querier.async_wait_for_error(
            querier.run_sql('select * from {{ reff("my_model") }}')
        )


def deps_with_packages(packages, bad_packages, project_dir, profiles_dir, schema):
    project = ProjectDefinition(
        models={
            'my_model.sql': 'select 1 as id',
        },
        packages={'packages': packages},
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        schema=schema,
        test_kwargs={},
    )

    with querier_ctx as querier:
        # we should be able to run sql queries at startup
        querier.async_wait_for_result(querier.run_sql('select 1 as id'))

        # the status should be something positive
        querier.is_result(querier.status())

        # deps should pass
        querier.async_wait_for_result(querier.deps())

        # queries should work after deps
        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))

        # now break the project
        project.packages['packages'] = bad_packages
        project.write_packages(project_dir, remove=True)

        # queries should still work because we haven't reloaded
        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))

        # now run deps again, it should be sad
        querier.async_wait_for_error(querier.deps())
        # it should also not be running.
        result = querier.is_result(querier.ps(active=True, completed=False))
        assert result['rows'] == []

        # fix packages again
        project.packages['packages'] = packages
        project.write_packages(project_dir, remove=True)
        # keep queries broken, we haven't run deps yet
        querier.is_error(querier.run())

        # deps should pass now
        querier.async_wait_for_result(querier.deps())
        querier.is_result(querier.status())

        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))


def test_rpc_deps_packages(project_root, profiles_root, postgres_profile, unique_schema):
    packages = [{
        'package': 'fishtown-analytics/dbt_utils',
        'version': '0.2.1',
    }]
    bad_packages = [{
        'package': 'fishtown-analytics/dbt_util',
        'version': '0.2.1',
    }]
    deps_with_packages(packages, bad_packages, project_root, profiles_root, unique_schema)


def test_rpc_deps_git(project_root, profiles_root, postgres_profile, unique_schema):
    packages = [{
        'git': 'https://github.com/fishtown-analytics/dbt-utils.git',
        'revision': '0.2.1'
    }]
    # if you use a bad URL, git thinks it's a private repo and prompts for auth
    bad_packages = [{
        'git': 'https://github.com/fishtown-analytics/dbt-utils.git',
        'revision': 'not-a-real-revision'
    }]
    deps_with_packages(packages, bad_packages, project_root, profiles_root, unique_schema)


bad_schema_yml = '''
version: 2
sources:
  - name: test_source
    loader: custom
    schema: "{{ var('test_run_schema') }}"
    tables:
      - name: test_table
        identifier: source
        tests:
          - relationships:
            # this is invalid
              - column_name: favorite_color
              - to: ref('descendant_model')
              - field: favorite_color
'''

fixed_schema_yml = '''
version: 2
sources:
  - name: test_source
    loader: custom
    schema: "{{ var('test_run_schema') }}"
    tables:
      - name: test_table
        identifier: source
'''


def test_rpc_status_error(project_root, profiles_root, postgres_profile, unique_schema):
    project = ProjectDefinition(
        models={
            'descendant_model.sql': 'select * from {{ source("test_source", "test_table") }}',
            'schema.yml': bad_schema_yml,
        }
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
        criteria='error',
    )
    with querier_ctx as querier:

        # the status should be an error result
        result = querier.is_result(querier.status())
        assert 'error' in result
        assert 'message' in result['error']
        assert 'Invalid test config' in result['error']['message']
        assert 'state' in result
        assert result['state'] == 'error'
        assert 'logs' in result
        logs = result['logs']
        assert len(logs) > 0
        for key in ('message', 'timestamp', 'levelname', 'level'):
            assert key in logs[0]
        assert 'pid' in result
        assert querier.server.pid == result['pid']

        error = querier.is_error(querier.compile_sql('select 1 as id'))
        assert 'code' in error
        assert error['code'] == 10011
        assert 'message' in error
        assert error['message'] == 'RPC server failed to compile project, call the "status" method for compile status'
        assert 'data' in error
        assert 'message' in error['data']
        assert 'Invalid test config' in error['data']['message']

        # deps should fail because it still can't parse the manifest
        querier.async_wait_for_error(querier.deps())

        # and not resolve the issue
        result = querier.is_result(querier.status())
        assert 'error' in result
        assert 'message' in result['error']
        assert 'Invalid test config' in result['error']['message']

        error = querier.is_error(querier.compile_sql('select 1 as id'))
        assert 'code' in error
        assert error['code'] == 10011

        project.models['schema.yml'] = fixed_schema_yml
        project.write_models(project_root, remove=True)

        # deps should work
        querier.async_wait_for_result(querier.deps())

        result = querier.is_result(querier.status())
        assert result.get('error') is None
        assert 'state' in result
        assert result['state'] == 'ready'

        querier.is_result(querier.compile_sql('select 1 as id'))


def test_gc_change_interval(project_root, profiles_root, postgres_profile, unique_schema):
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

        for _ in range(10):
            querier.async_wait_for_result(querier.run())

        result = querier.is_result(querier.ps(True, True))
        assert len(result['rows']) == 10

        result = querier.is_result(querier.gc(settings=dict(maxsize=1000, reapsize=5, auto_reap_age=0.1)))

        for k in ('deleted', 'missing', 'running'):
            assert k in result
            assert len(result[k]) == 0

        time.sleep(0.5)

        result = querier.is_result(querier.ps(True, True))
        assert len(result['rows']) == 0

        result = querier.is_result(querier.gc(settings=dict(maxsize=2, reapsize=5, auto_reap_age=100000)))
        for k in ('deleted', 'missing', 'running'):
            assert k in result
            assert len(result[k]) == 0

        time.sleep(0.5)

        for _ in range(10):
            querier.async_wait_for_result(querier.run())

        time.sleep(0.5)
        result = querier.is_result(querier.ps(True, True))
        assert len(result['rows']) == 2


def test_ps_poll_output_match(project_root, profiles_root, postgres_profile, unique_schema):
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

        poll_result = querier.async_wait_for_result(querier.run())

        result = querier.is_result(querier.ps(active=True, completed=True))
        assert 'rows' in result
        rows = result['rows']
        assert len(rows) == 1
        ps_result = rows[0]

        for key in ('start', 'end', 'elapsed', 'state'):
            assert ps_result[key] == poll_result[key]


macros_data = '''
{% macro foo() %}
    {{ return(1) }}
{% endmacro %}
{% macro bar(value) %}
    {{ return(value + 1) }}
{% endmacro %}
{% macro quux(value) %}
    {{ return(asdf) }}
{% endmacro %}
'''


def test_run_operation(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'},
        macros={
            'my_macros.sql': macros_data,
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
        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='foo', args={})
        )

        assert 'success' in poll_result
        assert poll_result['success'] is True

        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='bar', args={'value': 10})
        )

        assert 'success' in poll_result
        assert poll_result['success'] is True

        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='baz', args={}),
            state='failed',
        )
        assert 'state' in poll_result
        assert poll_result['state'] == 'failed'

        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='quux', args={})
        )
        assert 'success' in poll_result
        assert poll_result['success'] is True


def test_run_operation_cli(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'},
        macros={
            'my_macros.sql': macros_data,
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
        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli='run-operation foo')
        )

        assert 'success' in poll_result
        assert poll_result['success'] is True

        bar_cmd = '''run-operation bar --args="{'value': 10}"'''
        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli=bar_cmd)
        )

        assert 'success' in poll_result
        assert poll_result['success'] is True

        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli='run-operation baz'),
            state='failed',
        )
        assert 'state' in poll_result
        assert poll_result['state'] == 'failed'

        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli='run-operation quux')
        )
        assert 'success' in poll_result
        assert poll_result['success'] is True


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
    select 1 as id, '2019-10-31 23:59:40' as updated_at

{% endsnapshot %}
'''


def test_snapshots(
    project_root, profiles_root, postgres_profile, unique_schema
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


def test_snapshots_cli(
    project_root, profiles_root, postgres_profile, unique_schema
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


def assert_has_threads(results, num_threads):
    assert 'logs' in results
    c_logs = [l for l in results['logs'] if 'Concurrency' in l['message']]
    assert len(c_logs) == 1, \
        f'Got invalid number of concurrency logs ({len(c_logs)})'
    assert 'message' in c_logs[0]
    assert f'Concurrency: {num_threads} threads' in c_logs[0]['message']


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


def test_rpc_compile_threads(
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
        results = querier.async_wait_for_result(querier.compile(threads=5))
        assert_has_threads(results, 5)

        results = querier.async_wait_for_result(
            querier.cli_args('compile --threads=7')
        )
        assert_has_threads(results, 7)


def test_rpc_test_threads(
    project_root, profiles_root, postgres_profile, unique_schema
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


def test_rpc_snapshot_threads(
    project_root, profiles_root, postgres_profile, unique_schema
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


def test_rpc_seed_threads(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        project_data={'seeds': {'quote_columns': False}},
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


sleeper_sql = '''
{{ log('test output', info=True) }}
{{ run_query('select * from pg_sleep(20)') }}
select 1 as id
'''

logger_sql = '''
{{ log('test output', info=True) }}
select 1 as id
'''


def find_log_ordering(logs, *messages) -> bool:
    log_iter = iter(logs)
    found = 0

    while found < len(messages):
        try:
            log = next(log_iter)
        except StopIteration:
            return False
        if messages[found] in log['message']:
            found += 1
    return True


def poll_logs(querier, token):
    has_log = querier.is_result(querier.poll(token))
    assert 'logs' in has_log
    return has_log['logs']


def wait_for_log_ordering(querier, token, attempts, *messages) -> int:
    for _ in range(attempts):
        time.sleep(1)
        logs = poll_logs(querier, token)
        if find_log_ordering(logs, *messages):
            return len(logs)

    msg = 'Never got expected messages {} in {}'.format(
        messages,
        [log['message'] for log in logs],
    )
    assert False, msg


def test_get_status(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'},
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )

    with querier_ctx as querier:
        # make sure that logs_start/logs are honored during a task
        token = querier.is_async_result(querier.run_sql(sleeper_sql))

        no_log = querier.is_result(querier.poll(token, logs=False))
        assert 'logs' in no_log
        assert len(no_log['logs']) == 0

        num_logs = wait_for_log_ordering(querier, token, 10)

        trunc_log = querier.is_result(querier.poll(token, logs_start=num_logs))
        assert 'logs' in trunc_log
        assert len(trunc_log['logs']) == 0

        querier.kill(token)

        # make sure that logs_start/logs are honored after a task has finished
        token = querier.is_async_result(querier.run_sql(logger_sql))
        result = querier.is_result(querier.async_wait(token))
        assert 'logs' in result
        num_logs = len(result['logs'])
        assert num_logs > 0

        result = querier.is_result(querier.poll(token, logs_start=num_logs))
        assert 'logs' in result
        assert len(result['logs']) == 0

        result = querier.is_result(querier.poll(token, logs=False))
        assert 'logs' in result
        assert len(result['logs']) == 0


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


def test_source_freshness(
    project_root, profiles_root, postgres_profile, unique_schema
):
    start_time = datetime.utcnow()
    warn_me = start_time - timedelta(hours=18)
    error_me = start_time - timedelta(days=2)
    # this should trigger a 'warn'
    project = ProjectDefinition(
        project_data={'seeds': {'quote_columns': False}},
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
        warn_results = querier.async_wait_for_result(querier.snapshot_freshness(select='test_source.test_table'))
        assert len(warn_results['results']) == 1
        assert warn_results['results'][0]['status'] == 'warn'
        warn_results = querier.async_wait_for_result(querier.cli_args('source snapshot-freshness -s test_source.test_table'))
        assert len(warn_results['results']) == 1
        assert warn_results['results'][0]['status'] == 'warn'

        project.seeds['source.csv'] += '3,{}\n'.format(start_time.strftime('%Y-%m-%d %H:%M:%S'))
        project.write_seeds(project_root, remove=True)
        querier.async_wait_for_result(querier.seed())
        # should pass!
        pass_results = querier.async_wait_for_result(querier.snapshot_freshness(select=['test_source.test_table']))
        assert len(pass_results['results']) == 1
        assert pass_results['results'][0]['status'] == 'pass'
        pass_results = querier.async_wait_for_result(querier.cli_args('source snapshot-freshness --select test_source.test_table'))
        assert len(pass_results['results']) == 1
        assert pass_results['results'][0]['status'] == 'pass'
