import time
from .util import (
    get_querier,
    ProjectDefinition,
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


def test_missing_tag_sighup(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        models={
            'my_docs.md': '{% docs asdf %}have a close tag{% enddocs %}',
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
        # everything is fine
        assert querier.wait_for_status('ready') is True

        # write a junk docs file
        project.models['my_docs.md'] = '{% docs asdf %}do not have a close tag'
        project.write_models(project_root, remove=True)

        querier.sighup()

        assert querier.wait_for_status('error') is True
        result = querier.is_result(querier.status())
        assert 'error' in result
        assert 'message' in result['error']
        assert 'without finding a close tag for docs' in result['error']['message']

        project.models['my_docs.md'] = '{% docs asdf %}have a close tag again{% enddocs %}'
        project.write_models(project_root, remove=True)

        querier.sighup()

        assert querier.wait_for_status('ready') is True


def test_get_manifest(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        models={
            'my_model.sql': 'select 1 as id',
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
        results = querier.async_wait_for_result(querier.cli_args('run'))
        assert len(results['results']) == 1
        assert results['results'][0]['node']['compiled_sql'] == 'select 1 as id'
        result = querier.async_wait_for_result(querier.get_manifest())
        assert 'manifest' in result
        manifest = result['manifest']
        assert manifest['nodes']['model.test.my_model']['raw_sql'] == 'select 1 as id'
        assert 'manifest' in result
        manifest = result['manifest']
        assert manifest['nodes']['model.test.my_model']['compiled_sql'] == 'select 1 as id'
