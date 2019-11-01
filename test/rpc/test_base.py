# flake8: disable=redefined-outer-name
import time
from .util import (
    ProjectDefinition, rpc_server, Querier, built_schema, get_querier,
)


def test_rpc_basics(project_root, profiles_root, postgres_profile, unique_schema):
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
        token = querier.is_async_result(querier.run_sql('select 1 as id'))
        querier.is_result(querier.async_wait(token))

        token = querier.is_async_result(querier.run())
        querier.is_result(querier.async_wait(token))

        token = querier.is_async_result(querier.run_sql('select * from {{ ref("my_model") }}'))
        querier.is_result(querier.async_wait(token))

        token = querier.is_async_result(querier.run_sql('select * from {{ reff("my_model") }}'))
        querier.is_error(querier.async_wait(token))


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
        token = querier.is_async_result(querier.run_sql('select 1 as id'))
        querier.is_result(querier.async_wait(token))

        # the status should be something positive
        querier.is_result(querier.status())

        # deps should pass
        token = querier.is_async_result(querier.deps())
        querier.is_result(querier.async_wait(token))

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
        token = querier.is_async_result(querier.deps())
        querier.is_error(querier.async_wait(token))
        # it should also not be running.
        result = querier.is_result(querier.ps(active=True, completed=False))
        assert result['rows'] == []

        # fix packages again
        project.packages['packages'] = packages
        project.write_packages(project_dir, remove=True)
        # keep queries broken, we haven't run deps yet
        querier.is_error(querier.run())

        # deps should pass now
        token = querier.is_async_result(querier.deps())
        querier.is_result(querier.async_wait(token))
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
        token = querier.is_async_result(querier.deps())
        querier.is_error(querier.async_wait(token))

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
        token = querier.is_async_result(querier.deps())
        querier.is_result(querier.async_wait(token))

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
            token = querier.is_async_result(querier.run())
            querier.is_result(querier.async_wait(token))

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
            token = querier.is_async_result(querier.run())
            querier.is_result(querier.async_wait(token))

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

        token = querier.is_async_result(querier.run())
        poll_result = querier.is_result(querier.async_wait(token))

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


def test_run_operation(project_root, profiles_root, postgres_profile, unique_schema):
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
        token = querier.is_async_result(querier.run_operation(macro='foo', args={}))
        poll_result = querier.is_result(querier.async_wait(token))

        assert 'success' in poll_result
        assert poll_result['success'] is True

        token = querier.is_async_result(querier.run_operation(macro='bar', args={'value': 10}))
        poll_result = querier.is_result(querier.async_wait(token))

        assert 'success' in poll_result
        assert poll_result['success'] is True

        token = querier.is_async_result(querier.run_operation(macro='baz', args={}))
        poll_result = querier.is_result(querier.async_wait(token, state='failed'))
        assert 'state' in poll_result
        assert poll_result['state'] == 'failed'

        token = querier.is_async_result(querier.run_operation(macro='quux', args={}))
        poll_result = querier.is_result(querier.async_wait(token))
        assert 'success' in poll_result
        assert poll_result['success'] is True


def test_run_operation_cli(project_root, profiles_root, postgres_profile, unique_schema):
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
        token = querier.is_async_result(querier.cli_args(cli='run-operation foo'))
        poll_result = querier.is_result(querier.async_wait(token))

        assert 'success' in poll_result
        assert poll_result['success'] is True

        token = querier.is_async_result(querier.cli_args(cli='''run-operation bar --args="{'value': 10}"'''))
        poll_result = querier.is_result(querier.async_wait(token))

        assert 'success' in poll_result
        assert poll_result['success'] is True

        token = querier.is_async_result(querier.cli_args(cli='run-operation baz'))
        poll_result = querier.is_result(querier.async_wait(token, state='failed'))
        assert 'state' in poll_result
        assert poll_result['state'] == 'failed'

        token = querier.is_async_result(querier.cli_args(cli='run-operation quux'))
        poll_result = querier.is_result(querier.async_wait(token))
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


def test_snapshots(project_root, profiles_root, postgres_profile, unique_schema):
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
        token = querier.is_async_result(querier.snapshot())
        results = querier.is_result(querier.async_wait(token))
        assert len(results['results']) == 1

        token = querier.is_async_result(querier.snapshot(exclude=['snapshot_actual']))
        results = querier.is_result(querier.async_wait(token))

        token = querier.is_async_result(querier.snapshot(select=['snapshot_actual']))
        results = querier.is_result(querier.async_wait(token))
        assert len(results['results']) == 1


def test_snapshots_cli(project_root, profiles_root, postgres_profile, unique_schema):
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
        token = querier.is_async_result(querier.cli_args(cli='snapshot'))
        results = querier.is_result(querier.async_wait(token))
        assert len(results['results']) == 1

        token = querier.is_async_result(querier.cli_args(cli='snapshot --exclude=snapshot_actual'))
        results = querier.is_result(querier.async_wait(token))
        assert len(results['results']) == 0

        token = querier.is_async_result(querier.cli_args(cli='snapshot --select=snapshot_actual'))
        results = querier.is_result(querier.async_wait(token))
        assert len(results['results']) == 1
