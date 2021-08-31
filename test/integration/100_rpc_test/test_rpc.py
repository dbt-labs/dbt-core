import json
import os
import random
import shutil
import signal
import socket
import sys
import time
from base64 import standard_b64encode as b64
from datetime import datetime

import requests
from pytest import mark

from test.integration.base import DBTIntegrationTest, use_profile
import dbt.flags
from dbt.version import __version__
from dbt.logger import log_manager
from dbt.main import handle_and_check


class ServerProcess(dbt.flags.MP_CONTEXT.Process):
    def __init__(self, port, profiles_dir, cli_vars=None):
        self.port = port
        handle_and_check_args = [
            'rpc', '--log-cache-events',
            '--port', str(self.port),
            '--profiles-dir', profiles_dir
        ]
        if cli_vars:
            handle_and_check_args.extend(['--vars', cli_vars])
        super().__init__(
            target=handle_and_check,
            args=(handle_and_check_args,),
            name='ServerProcess')

    def run(self):
        log_manager.reset_handlers()
        # run server tests in stderr mode
        log_manager.stderr_console()
        return super().run()

    def can_connect(self):
        sock = socket.socket()
        try:
            sock.connect(('localhost', self.port))
        except socket.error:
            return False
        sock.close()
        return True

    def _compare_result(self, result):
        return result['result']['state'] == 'ready'

    def status_ok(self):
        result = query_url(
            'http://localhost:{}/jsonrpc'.format(self.port),
            {'method': 'status', 'id': 1, 'jsonrpc': '2.0'}
        ).json()
        return self._compare_result(result)

    def is_up(self):
        if not self.can_connect():
            return False
        return self.status_ok()

    def start(self):
        super().start()
        for _ in range(240):
            if self.is_up():
                break
            time.sleep(0.5)
        if not self.can_connect():
            raise Exception('server never appeared!')
        status_result = query_url(
            'http://localhost:{}/jsonrpc'.format(self.port),
            {'method': 'status', 'id': 1, 'jsonrpc': '2.0'}
        ).json()
        if not self._compare_result(status_result):
            raise Exception(
                'Got invalid status result: {}'.format(status_result)
            )


def query_url(url, query):
    headers = {'content-type': 'application/json'}
    return requests.post(url, headers=headers, data=json.dumps(query))


_select_from_ephemeral = '''with __dbt__cte__ephemeral_model as (


select 1 as id
)select * from __dbt__cte__ephemeral_model'''


def addr_in_use(err, *args):
    msg = str(err)
    if 'Address already in use' in msg:
        return True
    if 'server never appeared!' in msg:
        return True  # this can happen because of the above
    return False


@mark.skipif(os.name == 'nt', reason='"dbt rpc" not supported on windows')
class HasRPCServer(DBTIntegrationTest):
    ServerProcess = ServerProcess
    should_seed = True

    def setUp(self):
        super().setUp()
        os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE'] = 'test_run_schema'
        if self.should_seed:
            self.run_dbt_with_vars(['seed'], strict=False)
        port = random.randint(49152, 61000)
        self._server = self.ServerProcess(
            cli_vars='{{test_run_schema: {}}}'.format(self.unique_schema()),
            profiles_dir=self.test_root_dir,
            port=port
        )
        self._server.start()
        self.background_queries = []

    def tearDown(self):
        del os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE']
        self._server.terminate()
        for query in self.background_queries:
            query.terminate()
        super().tearDown()

    @property
    def schema(self):
        return "rpc_100"

    @property
    def models(self):
        return "models"

    def run_dbt_with_vars(self, cmd, *args, **kwargs):
        cmd.extend(['--vars',
                    '{{test_run_schema: {}}}'.format(self.unique_schema())])
        return self.run_dbt(cmd, *args, **kwargs)

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'quoting': {'database': True, 'schema': True, 'identifier': True},
            'macro-paths': ['macros'],
            'seeds': {
                'quote_columns': False,
            },
        }

    def build_query(
        self, method, kwargs, sql=None, test_request_id=1, macros=None
    ):
        body_data = ''
        if sql is not None:
            body_data += sql

        if macros is not None:
            body_data += macros

        if sql is not None or macros is not None:
            kwargs['sql'] = b64(body_data.encode('utf-8')).decode('utf-8')

        return {
            'jsonrpc': '2.0',
            'method': method,
            'params': kwargs,
            'id': test_request_id
        }

    @property
    def url(self):
        return 'http://localhost:{}/jsonrpc'.format(self._server.port)

    def poll_for_result(self, request_token, request_id=1, timeout=180, state='success', logs=None):
        start = time.time()
        kwargs = {
            'request_token': request_token,
        }
        if logs is not None:
            kwargs['logs'] = logs

        while True:
            time.sleep(0.5)
            response = self.query(
                'poll', _test_request_id=request_id, **kwargs)
            response_json = response.json()
            if 'error' in response_json:
                return response
            result = self.assertIsResult(response_json, request_id)
            self.assertIn('state', result)
            if result['state'] == state:
                return response
            if timeout is not None:
                delta = (time.time() - start)
                self.assertGreater(
                    timeout, delta,
                    'At time {}, never saw {}.\nLast response: {}'
                    .format(delta, state, result)
                )

    def async_query(self, _method, _sql=None, _test_request_id=1, _poll_timeout=180, macros=None, **kwargs):
        response = self.query(
            _method, _sql, _test_request_id, macros, **kwargs).json()
        result = self.assertIsResult(response, _test_request_id)
        self.assertIn('request_token', result)
        return self.poll_for_result(
            result['request_token'],
            request_id=_test_request_id,
            timeout=_poll_timeout,
        )

    def query(self, _method, _sql=None, _test_request_id=1, macros=None, **kwargs):
        built = self.build_query(
            _method, kwargs, _sql, _test_request_id, macros)
        return query_url(self.url, built)

    def handle_result(self, bg_query, pipe):
        result_type, result = pipe.recv()
        bg_query.join()
        if result_type == 'error':
            raise result
        else:
            return result

    def assertResultHasTimings(self, result, *names):
        self.assertIn('timing', result)
        timings = result['timing']
        self.assertEqual(len(timings), len(names))
        for expected_name, timing in zip(names, timings):
            self.assertIn('name', timing)
            self.assertEqual(timing['name'], expected_name)
            self.assertIn('started_at', timing)
            self.assertIn('completed_at', timing)
            datetime.strptime(timing['started_at'], '%Y-%m-%dT%H:%M:%S.%fZ')
            datetime.strptime(timing['completed_at'], '%Y-%m-%dT%H:%M:%S.%fZ')

    def assertIsResult(self, data, id_=1):
        self.assertEqual(data['id'], id_)
        self.assertEqual(data['jsonrpc'], '2.0')
        self.assertIn('result', data)
        self.assertNotIn('error', data)
        return data['result']

    def assertIsError(self, data, id_=1):
        self.assertEqual(data['id'], id_)
        self.assertEqual(data['jsonrpc'], '2.0')
        self.assertIn('error', data)
        self.assertNotIn('result', data)
        return data['error']

    def assertIsErrorWithCode(self, data, code, id_=1):
        error = self.assertIsError(data, id_)
        self.assertIn('code', error)
        self.assertIn('message', error)
        self.assertEqual(error['code'], code)
        return error

    def assertIsErrorWith(self, data, code, message, error_data):
        error = self.assertIsErrorWithCode(data, code)
        if message is not None:
            self.assertEqual(error['message'], message)

        if error_data is not None:
            return self.assertHasErrorData(error, error_data)
        else:
            return error.get('data')

    def assertResultHasSql(self, data, raw_sql, compiled_sql=None):
        if compiled_sql is None:
            compiled_sql = raw_sql
        result = self.assertIsResult(data)
        self.assertIn('logs', result)
        self.assertTrue(len(result['logs']) > 0)
        self.assertIn('results', result)
        self.assertEqual(len(result['results']), 1)
        result = result['results'][0]
        self.assertIn('raw_sql', result)
        self.assertIn('compiled_sql', result)
        self.assertEqual(result['raw_sql'], raw_sql)
        self.assertEqual(result['compiled_sql'], compiled_sql)
        return result

    def assertSuccessfulCompilationResult(self, data, raw_sql, compiled_sql=None):
        result = self.assertResultHasSql(data, raw_sql, compiled_sql)
        self.assertNotIn('table', result)
        # compile results still have an 'execute' timing, it just represents
        # the time to construct a result object.
        self.assertResultHasTimings(result, 'compile', 'execute')

    def assertSuccessfulRunResult(self, data, raw_sql, compiled_sql=None, table=None):
        result = self.assertResultHasSql(data, raw_sql, compiled_sql)
        self.assertIn('table', result)
        if table is not None:
            self.assertEqual(result['table'], table)
        self.assertResultHasTimings(result, 'compile', 'execute')

    def assertHasErrorData(self, error, expected_error_data):
        self.assertIn('data', error)
        error_data = error['data']
        for key, value in expected_error_data.items():
            self.assertIn(key, error_data)
            self.assertEqual(error_data[key], value)
        return error_data

    def assertRunning(self, sleepers):
        sleeper_ps_result = self.query(
            'ps', completed=False, active=True).json()
        result = self.assertIsResult(sleeper_ps_result)
        self.assertEqual(len(result['rows']), len(sleepers))
        result_map = {rd['request_id']: rd for rd in result['rows']}
        for _, request_id in sleepers:
            found = result_map[request_id]
            self.assertEqual(found['request_id'], request_id)
            self.assertEqual(found['method'], 'run_sql')
            self.assertEqual(found['state'], 'running')
            self.assertEqual(found['timeout'], None)

    def kill_and_assert(self, request_token, request_id):
        kill_response = self.query('kill', task_id=request_token).json()
        result = self.assertIsResult(kill_response)
        self.assertEqual(result['state'], 'killed')

        poll_id = 90891

        poll_response = self.poll_for_result(
            request_token, request_id=poll_id, state='killed', logs=True
        ).json()

        result = self.assertIsResult(poll_response, id_=poll_id)
        self.assertIn('logs', result)
        return result

    def get_sleep_query(self, duration=15, request_id=90890):
        sleep_query = self.query(
            'run_sql',
            'select * from pg_sleep({})'.format(duration),
            name='sleeper',
            _test_request_id=request_id
        ).json()
        result = self.assertIsResult(sleep_query, id_=request_id)
        self.assertIn('request_token', result)
        request_token = result['request_token']
        return request_token, request_id

    def wait_for_state(
        self, state, timestamp, timeout=180, raise_on_timeout=True
    ):
        started = time.time()
        time.sleep(0.5)
        elapsed = time.time() - started

        while elapsed < timeout:
            status = self.assertIsResult(self.query('status').json())
            self.assertTrue(status['timestamp'] >= timestamp)
            if status['timestamp'] != timestamp and status['state'] == state:
                return status
            time.sleep(0.5)
            elapsed = time.time() - started

        status = self.assertIsResult(self.query('status').json())
        self.assertTrue(status['timestamp'] >= timestamp)
        if raise_on_timeout:
            self.assertEqual(
                status['state'],
                state,
                f'exceeded max time of {timeout}: {elapsed} seconds elapsed'
            )
        return status

    def run_command_with_id(self, cmd, id_):
        self.assertIsResult(self.async_query(
            cmd, _test_request_id=id_).json(), id_)

    def make_many_requests(self, num_requests):
        stored = []
        for idx in range(1, num_requests+1):
            response = self.query(
                'run_sql', 'select 1 as id', name='run', _test_request_id=idx
            ).json()
            result = self.assertIsResult(response, id_=idx)
            self.assertIn('request_token', result)
            token = result['request_token']
            self.poll_for_result(token)
            stored.append(token)
        return stored


class TestRPCServerCompileRun(HasRPCServer):
    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_compile_sql_postgres(self):
        trivial = self.async_query(
            'compile_sql',
            'select 1 as id',
            name='foo'
        ).json()
        self.assertSuccessfulCompilationResult(
            trivial, 'select 1 as id'
        )

        ref = self.async_query(
            'compile_sql',
            'select * from {{ ref("descendant_model") }}',
            name='foo'
        ).json()
        self.assertSuccessfulCompilationResult(
            ref,
            'select * from {{ ref("descendant_model") }}',
            compiled_sql='select * from "{}"."{}"."descendant_model"'.format(
                self.default_database,
                self.unique_schema())
        )

        source = self.async_query(
            'compile_sql',
            'select * from {{ source("test_source", "test_table") }}',
            name='foo'
        ).json()
        self.assertSuccessfulCompilationResult(
            source,
            'select * from {{ source("test_source", "test_table") }}',
            compiled_sql='select * from "{}"."{}"."source"'.format(
                self.default_database,
                self.unique_schema())
        )

        macro = self.async_query(
            'compile_sql',
            'select {{ my_macro() }}',
            name='foo',
            macros='{% macro my_macro() %}1 as id{% endmacro %}'
        ).json()
        self.assertSuccessfulCompilationResult(
            macro,
            'select {{ my_macro() }}',
            compiled_sql='select 1 as id'
        )

        macro_override = self.async_query(
            'compile_sql',
            'select {{ happy_little_macro() }}',
            name='foo',
            macros='{% macro override_me() %}2 as id{% endmacro %}'
        ).json()
        self.assertSuccessfulCompilationResult(
            macro_override,
            'select {{ happy_little_macro() }}',
            compiled_sql='select 2 as id'
        )

        macro_override_with_if_statement = self.async_query(
            'compile_sql',
            '{% if True %}select {{ happy_little_macro() }}{% endif %}',
            name='foo',
            macros='{% macro override_me() %}2 as id{% endmacro %}'
        ).json()
        self.assertSuccessfulCompilationResult(
            macro_override_with_if_statement,
            '{% if True %}select {{ happy_little_macro() }}{% endif %}',
            compiled_sql='select 2 as id'
        )

        ephemeral = self.async_query(
            'compile_sql',
            'select * from {{ ref("ephemeral_model") }}',
            name='foo'
        ).json()
        self.assertSuccessfulCompilationResult(
            ephemeral,
            'select * from {{ ref("ephemeral_model") }}',
            compiled_sql=_select_from_ephemeral
        )

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_run_sql_postgres(self):
        # seed + run dbt to make models before using them!
        self.run_dbt_with_vars(['seed'])
        self.run_dbt_with_vars(['run'])
        data = self.async_query(
            'run_sql',
            'select 1 as id',
            name='foo'
        ).json()
        self.assertSuccessfulRunResult(
            data, 'select 1 as id', table={'column_names': ['id'], 'rows': [[1.0]]}
        )

        ref = self.async_query(
            'run_sql',
            'select * from {{ ref("descendant_model") }} order by updated_at limit 1',
            name='foo'
        ).json()
        self.assertSuccessfulRunResult(
            ref,
            'select * from {{ ref("descendant_model") }} order by updated_at limit 1',
            compiled_sql='select * from "{}"."{}"."descendant_model" order by updated_at limit 1'.format(
                self.default_database,
                self.unique_schema()),
            table={
                'column_names': ['favorite_color', 'id', 'first_name', 'email', 'ip_address', 'updated_at'],
                'rows': [['blue', 38.0, 'Gary',  'gray11@statcounter.com', "'40.193.124.56'", '1970-01-27T10:04:51']],
            }
        )

        source = self.async_query(
            'run_sql',
            'select * from {{ source("test_source", "test_table") }} order by updated_at limit 1',
            name='foo'
        ).json()
        self.assertSuccessfulRunResult(
            source,
            'select * from {{ source("test_source", "test_table") }} order by updated_at limit 1',
            compiled_sql='select * from "{}"."{}"."source" order by updated_at limit 1'.format(
                self.default_database,
                self.unique_schema()),
            table={
                'column_names': ['favorite_color', 'id', 'first_name', 'email', 'ip_address', 'updated_at'],
                'rows': [['blue', 38.0, 'Gary',  'gray11@statcounter.com', "'40.193.124.56'", '1970-01-27T10:04:51']],
            }
        )

        macro = self.async_query(
            'run_sql',
            'select {{ my_macro() }}',
            name='foo',
            macros='{% macro my_macro() %}1 as id{% endmacro %}'
        ).json()
        self.assertSuccessfulRunResult(
            macro,
            raw_sql='select {{ my_macro() }}',
            compiled_sql='select 1 as id',
            table={'column_names': ['id'], 'rows': [[1.0]]}
        )

        macro_override = self.async_query(
            'run_sql',
            'select {{ happy_little_macro() }}',
            name='foo',
            macros='{% macro override_me() %}2 as id{% endmacro %}'
        ).json()
        self.assertSuccessfulRunResult(
            macro_override,
            raw_sql='select {{ happy_little_macro() }}',
            compiled_sql='select 2 as id',
            table={'column_names': ['id'], 'rows': [[2.0]]}
        )

        macro_override_with_if_statement = self.async_query(
            'run_sql',
            '{% if True %}select {{ happy_little_macro() }}{% endif %}',
            name='foo',
            macros='{% macro override_me() %}2 as id{% endmacro %}'
        ).json()
        self.assertSuccessfulRunResult(
            macro_override_with_if_statement,
            '{% if True %}select {{ happy_little_macro() }}{% endif %}',
            compiled_sql='select 2 as id',
            table={'column_names': ['id'], 'rows': [[2.0]]}
        )

        macro_with_raw_statement = self.async_query(
            'run_sql',
            '{% raw %}select 1 as{% endraw %}{{ test_macros() }}{% macro test_macros() %} id{% endmacro %}',
            name='foo'
        ).json()
        self.assertSuccessfulRunResult(
            macro_with_raw_statement,
            '{% raw %}select 1 as{% endraw %}{{ test_macros() }}',
            compiled_sql='select 1 as id',
            table={'column_names': ['id'], 'rows': [[1.0]]}
        )

        macro_with_comment = self.async_query(
            'run_sql',
            '{% raw %}select 1 {% endraw %}{{ test_macros() }} {# my comment #}{% macro test_macros() -%} as{% endmacro %} id{# another comment #}',
            name='foo'
        ).json()
        self.assertSuccessfulRunResult(
            macro_with_comment,
            '{% raw %}select 1 {% endraw %}{{ test_macros() }} {# my comment #} id{# another comment #}',
            compiled_sql='select 1 as  id',
            table={'column_names': ['id'], 'rows': [[1.0]]}
        )

        ephemeral = self.async_query(
            'run_sql',
            'select * from {{ ref("ephemeral_model") }}',
            name='foo'
        ).json()
        self.assertSuccessfulRunResult(
            ephemeral,
            raw_sql='select * from {{ ref("ephemeral_model") }}',
            compiled_sql=_select_from_ephemeral,
            table={'column_names': ['id'], 'rows': [[1.0]]}
        )

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_ps_kill_postgres(self):
        task_tags = {
            'dbt_version': __version__,
            'my_custom_tag': True,
        }
        done_query = self.async_query(
            'compile_sql', 'select 1 as id', name='done', task_tags=task_tags
        ).json()
        done_result = self.assertIsResult(done_query)
        self.assertIn('tags', done_result)
        self.assertEqual(done_result['tags'], task_tags)

        request_token, request_id = self.get_sleep_query()

        empty_ps_result = self.query(
            'ps', completed=False, active=False).json()
        result = self.assertIsResult(empty_ps_result)
        self.assertEqual(len(result['rows']), 0)

        sleeper_ps_result = self.query(
            'ps', completed=False, active=True).json()
        result = self.assertIsResult(sleeper_ps_result)
        self.assertEqual(len(result['rows']), 1)
        rowdict = result['rows']
        self.assertEqual(rowdict[0]['request_id'], request_id)
        self.assertEqual(rowdict[0]['method'], 'run_sql')
        self.assertEqual(rowdict[0]['state'], 'running')
        self.assertIsNone(rowdict[0]['timeout'])
        self.assertEqual(rowdict[0]['task_id'], request_token)
        self.assertGreater(rowdict[0]['elapsed'], 0)
        self.assertIsNone(rowdict[0]['tags'])

        complete_ps_result = self.query(
            'ps', completed=True, active=False).json()
        result = self.assertIsResult(complete_ps_result)
        self.assertEqual(len(result['rows']), 1)
        rowdict = result['rows']
        self.assertEqual(rowdict[0]['request_id'], 1)
        self.assertEqual(rowdict[0]['method'], 'compile_sql')
        self.assertEqual(rowdict[0]['state'], 'success')
        self.assertIsNone(rowdict[0]['timeout'])
        self.assertGreater(rowdict[0]['elapsed'], 0)
        self.assertEqual(rowdict[0]['tags'], task_tags)

        all_ps_result = self.query('ps', completed=True, active=True).json()
        result = self.assertIsResult(all_ps_result)
        self.assertEqual(len(result['rows']), 2)
        rowdict = result['rows']
        rowdict.sort(key=lambda r: r['start'])
        self.assertEqual(rowdict[0]['request_id'], 1)
        self.assertEqual(rowdict[0]['method'], 'compile_sql')
        self.assertEqual(rowdict[0]['state'], 'success')
        self.assertIsNone(rowdict[0]['timeout'])
        self.assertGreater(rowdict[0]['elapsed'], 0)
        self.assertEqual(rowdict[0]['tags'], task_tags)
        self.assertEqual(rowdict[1]['request_id'], request_id)
        self.assertEqual(rowdict[1]['method'], 'run_sql')
        self.assertEqual(rowdict[1]['state'], 'running')
        self.assertIsNone(rowdict[1]['timeout'])
        self.assertGreater(rowdict[1]['elapsed'], 0)
        self.assertIsNone(rowdict[1]['tags'])

        # try to GC our running task
        gc_response = self.query('gc', task_ids=[request_token]).json()
        gc_result = self.assertIsResult(gc_response)
        self.assertIn('running', gc_result)
        self.assertEqual(gc_result['running'], [request_token])

        self.kill_and_assert(request_token, request_id)

        all_ps_result = self.query('ps', completed=True, active=True).json()
        result = self.assertIsResult(all_ps_result)
        self.assertEqual(len(result['rows']), 2)
        rowdict = result['rows']
        rowdict.sort(key=lambda r: r['start'])
        self.assertEqual(rowdict[0]['request_id'], 1)
        self.assertEqual(rowdict[0]['method'], 'compile_sql')
        self.assertEqual(rowdict[0]['state'], 'success')
        self.assertIsNone(rowdict[0]['timeout'])
        self.assertGreater(rowdict[0]['elapsed'], 0)
        self.assertEqual(rowdict[0]['tags'], task_tags)
        self.assertEqual(rowdict[1]['request_id'], request_id)
        self.assertEqual(rowdict[1]['method'], 'run_sql')
        self.assertEqual(rowdict[1]['state'], 'killed')
        self.assertIsNone(rowdict[1]['timeout'])
        self.assertGreater(rowdict[1]['elapsed'], 0)
        self.assertIsNone(rowdict[1]['tags'])

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_ps_kill_longwait_postgres(self):
        request_token, request_id = self.get_sleep_query()

        # the test above frequently kills the process during parsing of the
        # requested node. That's also a useful test, but we should test that
        # we cancel the in-progress sleep query.
        time.sleep(3)

        result_data = self.kill_and_assert(request_token, request_id)
        self.assertTrue(len(result_data['logs']) > 0)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_invalid_requests_postgres(self):
        # invalid method -> error on the initial query
        data = self.query(
            'xxxxxnotamethodxxxxx',
            'hi this is not sql'
        ).json()
        self.assertIsErrorWith(data, -32601, 'Method not found', None)

        data = self.async_query(
            'compile_sql',
            'select * from {{ reff("nonsource_descendant") }}',
            name='mymodel',
            task_tags={'some_tag': True, 'another_tag': 'blah blah blah'}
        ).json()
        error_data = self.assertIsErrorWith(data, 10004, 'Compilation Error', {
            'type': 'UndefinedMacroException',
            'message': "Compilation Error in rpc mymodel (from remote system)\n  'reff' is undefined. This can happen when calling a macro that does not exist. Check for typos and/or install package dependencies with \"dbt deps\".",
            'compiled_sql': None,
            'raw_sql': 'select * from {{ reff("nonsource_descendant") }}',
            'tags': {'some_tag': True, 'another_tag': 'blah blah blah'}
        })
        self.assertIn('logs', error_data)
        self.assertTrue(len(error_data['logs']) > 0)

        data = self.async_query(
            'run_sql',
            'hi this is not sql',
            name='foo'
        ).json()
        # this is "1" if the multiprocessing context is "spawn" and "2" if
        # it's fork.
        lineno = '1'
        error_data = self.assertIsErrorWith(data, 10003, 'Database Error', {
            'type': 'DatabaseException',
            'message': f'Database Error in rpc foo (from remote system)\n  syntax error at or near "hi"\n  LINE {lineno}: hi this is not sql\n          ^',
            'compiled_sql': 'hi this is not sql',
            'raw_sql': 'hi this is not sql',
        })
        self.assertIn('logs', error_data)
        self.assertTrue(len(error_data['logs']) > 0)

        macro_no_override = self.async_query(
            'run_sql',
            'select {{ happy_little_macro() }}',
            name='foo',
        ).json()
        error_data = self.assertIsErrorWith(macro_no_override, 10004, 'Compilation Error', {
            'type': 'CompilationException',
            'raw_sql': 'select {{ happy_little_macro() }}',
            'compiled_sql': None
        })
        self.assertIn('logs', error_data)
        self.assertTrue(len(error_data['logs']) > 0)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_timeout_postgres(self):
        data = self.async_query(
            'run_sql',
            'select from pg_sleep(5)',
            name='foo',
            timeout=1
        ).json()
        error = self.assertIsErrorWithCode(data, 10008)
        self.assertEqual(error['message'], 'RPC timeout error')
        self.assertIn('data', error)
        error_data = error['data']
        self.assertIn('timeout', error_data)
        self.assertEqual(error_data['timeout'], 1)
        self.assertIn('message', error_data)
        self.assertEqual(error_data['message'], 'RPC timed out after 1.0s')
        self.assertIn('logs', error_data)
        # because fork() without exec() is broken, we use 'spawn' so we don't
        # get logs back because we didn't fork()
        return


class TestRPCServerProjects(HasRPCServer):
    def assertHasResults(self, result, expected, *, missing=None, num_expected=None):
        dct = self.assertIsResult(result)
        self.assertIn('results', dct)
        results = dct['results']

        if num_expected is None:
            num_expected = len(expected)
        actual = {r['node']['name'] for r in results}
        self.assertEqual(len(actual), num_expected)
        self.assertTrue(expected.issubset(actual))
        if missing:
            for item in missing:
                self.assertNotIn(item, actual)

    def correct_seed_result(self, result):
        self.assertTablesEqual('source', 'seed_expected')
        self.assertHasResults(
            result,
            {'expected_multi_source', 'other_source_table', 'other_table', 'source'}
        )

    def assertHasTestResults(self, results, expected, pass_results=None):
        self.assertEqual(len(results), expected)

        if pass_results is None:
            pass_results = expected

        passes = 0
        for result in results:
            # TODO: should this be included even when it's 'none'? Should
            # results have all these crazy keys? (no)
            if result['status'] == "pass":
                passes += 1

        self.assertEqual(passes, pass_results)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_seed_project_postgres(self):
        # testing "dbt seed" is tricky so we'll just jam some sql in there
        self.run_sql_file("seed.sql")

        result = self.async_query('seed', show=True).json()
        self.correct_seed_result(result)

        result = self.async_query('seed', show=False).json()
        self.correct_seed_result(result)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_seed_project_cli_postgres(self):
        self.run_sql_file("seed.sql")

        result = self.async_query('cli_args', cli='seed --show').json()
        self.correct_seed_result(result)
        result = self.async_query('cli_args', cli='seed').json()
        self.correct_seed_result(result)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_compile_project_postgres(self):

        result = self.async_query('compile').json()
        self.assertHasResults(
            result,
            {'descendant_model', 'multi_source_model', 'nonsource_descendant'},
            missing=['ephemeral_model'],
            num_expected=11,
        )

        result = self.async_query(
            'compile', models=['source:test_source+']).json()
        self.assertHasResults(
            result,
            {'descendant_model', 'multi_source_model'},
            missing=['ephemeral_model', 'nonsource_descendant'],
            num_expected=6,
        )

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_compile_project_cli_postgres(self):
        self.run_dbt_with_vars(['compile'])
        result = self.async_query('cli_args', cli='compile').json()
        self.assertHasResults(
            result,
            {'descendant_model', 'multi_source_model', 'nonsource_descendant'},
            missing=['ephemeral_model'],
            num_expected=11,
        )

        result = self.async_query(
            'cli_args', cli='compile --models=source:test_source+').json()
        self.assertHasResults(
            result,
            {'descendant_model', 'multi_source_model'},
            missing=['ephemeral_model', 'nonsource_descendant'],
            num_expected=6,
        )

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_run_project_postgres(self):
        result = self.async_query('run').json()
        assert 'args' in result['result']
        self.assertHasResults(
            result, {'descendant_model', 'multi_source_model', 'nonsource_descendant'})
        self.assertTablesEqual('multi_source_model', 'expected_multi_source')

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_run_project_cli_postgres(self):
        result = self.async_query('cli_args', cli='run').json()
        self.assertHasResults(
            result, {'descendant_model', 'multi_source_model', 'nonsource_descendant'})
        self.assertTablesEqual('multi_source_model', 'expected_multi_source')

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_test_project_postgres(self):
        self.run_dbt_with_vars(['run'])
        data = self.async_query('test').json()
        result = self.assertIsResult(data)
        self.assertIn('results', result)
        self.assertHasTestResults(result['results'], 4)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_test_project_cli_postgres(self):
        self.run_dbt_with_vars(['run'])
        data = self.async_query('cli_args', cli='test').json()
        result = self.assertIsResult(data)
        self.assertIn('results', result)
        self.assertHasTestResults(result['results'], 4)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    def assertManifestExists(self, nodes_length, sources_length):
        self.assertTrue(os.path.exists('target/manifest.json'))
        with open('target/manifest.json') as fp:
            manifest = json.load(fp)
        self.assertIn('nodes', manifest)
        self.assertEqual(len(manifest['nodes']), nodes_length)
        self.assertIn('sources', manifest)
        self.assertEqual(len(manifest['sources']), sources_length)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    def assertHasDocsGenerated(self, result, expected):
        dct = self.assertIsResult(result)
        self.assertIn('state', dct)
        self.assertTrue(dct['state'])
        self.assertIn('nodes', dct)
        nodes = dct['nodes']
        self.assertEqual(set(nodes), expected['nodes'])
        self.assertIn('sources', dct)
        sources = dct['sources']
        self.assertEqual(set(sources), expected['sources'])

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    def assertCatalogExists(self):
        self.assertTrue(os.path.exists('target/catalog.json'))
        with open('target/catalog.json') as fp:
            catalog = json.load(fp)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    def _correct_docs_generate_result(self, result):
        expected = {
            'nodes': {
                'model.test.descendant_model',
                'model.test.multi_source_model',
                'model.test.nonsource_descendant',
                'seed.test.expected_multi_source',
                'seed.test.other_source_table',
                'seed.test.other_table',
                'seed.test.source',
            },
            'sources': {
                'source.test.other_source.test_table',
                'source.test.test_source.other_test_table',
                'source.test.test_source.test_table',
            },
        }
        self.assertHasDocsGenerated(result, expected)
        self.assertCatalogExists()
        self.assertManifestExists(12, 5)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_docs_generate_postgres(self):
        self.run_dbt_with_vars(['run'])
        self.assertFalse(os.path.exists('target/catalog.json'))
        if os.path.exists('target/manifest.json'):
            os.remove('target/manifest.json')
        result = self.async_query('docs.generate').json()
        self._correct_docs_generate_result(result)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_docs_generate_postgres_cli(self):
        self.run_dbt_with_vars(['run'])
        self.assertFalse(os.path.exists('target/catalog.json'))
        if os.path.exists('target/manifest.json'):
            os.remove('target/manifest.json')
        result = self.async_query('cli_args', cli='docs generate').json()
        self._correct_docs_generate_result(result)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_deps_postgres(self):
        self.async_query('deps').json()

    @mark.skip(reason='cli_args + deps not supported for now')
    @use_profile('postgres')
    def test_deps_postgres_cli(self):
        self.async_query('cli_args', cli='deps').json()


class TestRPCTaskManagement(HasRPCServer):
    """
    TODO: fix flaky test: issue #3475 
    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_sighup_postgres(self):
        status = self.assertIsResult(self.query('status').json())
        self.assertEqual(status['state'], 'ready')
        self.assertIn('logs', status)
        logs = status['logs']
        self.assertTrue(len(logs) > 0)
        for key in ('message', 'timestamp', 'levelname', 'level'):
            self.assertIn(key, logs[0])

        self.assertIn('timestamp', status)

        done_query = self.async_query(
            'compile_sql', 'select 1 as id', name='done').json()
        self.assertIsResult(done_query)
        sleepers = []

        sleepers.append(self.get_sleep_query(duration=180, request_id=1000))
        self.assertRunning(sleepers)

        self.run_command_with_id('seed', 20)
        self.run_command_with_id('run', 21)

        # sighup a few times
        for _ in range(10):
            os.kill(status['pid'], signal.SIGHUP)

        self.wait_for_state('ready', timestamp=status['timestamp'])

        # we should still still see our service:
        self.assertRunning(sleepers)

        self.run_command_with_id('seed', 30)
        self.run_command_with_id('run', 31)

        # start a new one too
        sleepers.append(self.get_sleep_query(duration=60, request_id=1001))

        # now we should see both
        self.assertRunning(sleepers)

        # now pluck out the oldest one and kill it
        dead, alive = sleepers
        self.kill_and_assert(*dead)
        self.assertRunning([alive])
        self.kill_and_assert(*alive)
    """
    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_gc_by_time_postgres(self):
        # make a few normal requests
        num_requests = 10
        self.make_many_requests(num_requests)

        resp = self.query('ps', completed=True, active=True).json()
        result = self.assertIsResult(resp)
        self.assertEqual(len(result['rows']), num_requests)
        # force a GC
        resp = self.query('gc', before=datetime.utcnow().isoformat()).json()
        result = self.assertIsResult(resp)
        self.assertEqual(len(result['deleted']), num_requests)
        self.assertEqual(len(result['missing']), 0)
        self.assertEqual(len(result['running']), 0)
        # now there should be none
        resp = self.query('ps', completed=True, active=True).json()
        result = self.assertIsResult(resp)
        self.assertEqual(len(result['rows']), 0)

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_gc_by_id_postgres(self):
        # make 10 requests, then gc half of them
        num_requests = 10
        stored = self.make_many_requests(num_requests)

        resp = self.query('ps', completed=True, active=True).json()
        result = self.assertIsResult(resp)
        self.assertEqual(len(result['rows']), num_requests)

        resp = self.query('gc', task_ids=stored[:num_requests//2]).json()
        result = self.assertIsResult(resp)
        self.assertEqual(len(result['deleted']), num_requests//2)
        self.assertEqual(sorted(result['deleted']),
                         sorted(stored[:num_requests//2]))
        self.assertEqual(len(result['missing']), 0)
        self.assertEqual(len(result['running']), 0)
        # we should have total - what we removed still there
        resp = self.query('ps', completed=True, active=True).json()
        result = self.assertIsResult(resp)
        self.assertEqual(len(result['rows']), (num_requests - num_requests//2))

        resp = self.query('gc', task_ids=stored[num_requests//2:]).json()
        result = self.assertIsResult(resp)
        self.assertEqual(len(result['deleted']), num_requests//2)
        self.assertEqual(sorted(result['deleted']),
                         sorted(stored[num_requests//2:]))
        self.assertEqual(len(result['missing']), 0)
        self.assertEqual(len(result['running']), 0)
        # all gone!
        resp = self.query('ps', completed=True, active=True).json()
        result = self.assertIsResult(resp)
        self.assertEqual(len(result['rows']), 0)


class CompletingServerProcess(ServerProcess):
    def _compare_result(self, result):
        return result['result']['state'] in ('error', 'ready')


class TestRPCServerDeps(HasRPCServer):
    ServerProcess = CompletingServerProcess
    should_seed = False

    def setUp(self):
        super().setUp()
        if os.path.exists('./dbt_modules'):
            shutil.rmtree('./dbt_modules')

    def tearDown(self):
        if os.path.exists('./dbt_modules'):
            shutil.rmtree('./dbt_modules')
        self.adapter.cleanup_connections()
        super().tearDown()

    @property
    def packages_config(self):
        return {
            # this is config-version 2, but with no upper bound
            'packages': [
                {'package': 'dbt-labs/dbt_utils', 'version': '0.5.0'},
            ]
        }

    @property
    def models(self):
        return "deps_models"

    def _check_start_predeps(self):
        self.assertFalse(os.path.exists('./dbt_modules'))
        status = self.assertIsResult(self.query('status').json())
        # will return an error because defined dependency is missing
        self.assertEqual(status['state'], 'error')
        return status

    def _check_deps_ok(self, status):
        os.kill(status['pid'], signal.SIGHUP)

        self.wait_for_state('ready', timestamp=status['timestamp'])

        self.assertTrue(os.path.exists('./dbt_modules'))
        self.assertEqual(len(os.listdir('./dbt_modules')), 1)
        self.assertIsResult(self.async_query('compile').json())

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_deps_compilation_postgres(self):
        status = self._check_start_predeps()

        # do a dbt deps, wait for the result
        self.assertIsResult(self.async_query('deps', _poll_timeout=180).json())

        self._check_deps_ok(status)

    @mark.skip(reason='cli_args + deps not supported for now')
    @use_profile('postgres')
    def test_deps_cli_compilation_postgres(self):
        status = self._check_start_predeps()

        # do a dbt deps, wait for the result
        self.assertIsResult(self.async_query(
            'cli_args', cli='deps', _poll_timeout=180).json())

        self._check_deps_ok(status)


class TestRPCServerList(HasRPCServer):
    should_seed = False

    @property
    def models(self):
        return "models"

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_list_base_postgres(self):
        result = self.query('list').json()
        self.assertIsResult(result)
        self.assertEqual(len(result["result"]["output"]), 17)
        self.assertEqual(
            [x["name"] for x in result["result"]["output"]],
            [
                'descendant_model', 
                'ephemeral_model', 
                'multi_source_model', 
                'nonsource_descendant', 
                'expected_multi_source', 
                'other_source_table', 
                'other_table', 
                'source', 
                'table', 
                'test_table', 
                'disabled_test_table', 
                'other_test_table', 
                'test_table', 
                'relationships_descendant_model_favorite_color__favorite_color__source_test_source_test_table_', 
                'source_not_null_test_source_test_table_id', 
                'source_relationships_test_source_test_table_favorite_color__favorite_color__ref_descendant_model_', 
                'source_unique_test_source_test_table_id'
                ]
        )

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_list_resource_type_postgres(self):
        result = self.query('list', resource_types=['model']).json()
        self.assertIsResult(result)
        self.assertEqual(len(result["result"]["output"]), 4)
        self.assertEqual(
            [x['name'] for x in result["result"]["output"]],
            [
                'descendant_model', 
                'ephemeral_model', 
                'multi_source_model', 
                'nonsource_descendant']
        )

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_list_models_postgres(self):
        result = self.query('list', models=['descendant_model']).json()
        self.assertIsResult(result)
        self.assertEqual(len(result["result"]["output"]), 1)
        self.assertEqual(result["result"]["output"][0]["name"], 'descendant_model')

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_list_exclude_postgres(self):
        result = self.query('list', exclude=['+descendant_model']).json()
        self.assertIsResult(result)
        self.assertEqual(len(result["result"]["output"]), 11)
        self.assertEqual(
            [x['name'] for x in result['result']['output']],
            [
                'ephemeral_model', 
                'multi_source_model', 
                'nonsource_descendant', 
                'expected_multi_source', 
                'other_source_table', 
                'other_table', 
                'source', 
                'table', 
                'test_table', 
                'disabled_test_table', 
                'other_test_table'
                ]
        )

    @mark.flaky(rerun_filter=addr_in_use, max_runs=3)
    @use_profile('postgres')
    def test_list_select_postgres(self):
        result = self.query('list', select=[
            'relationships_descendant_model_favorite_color__favorite_color__source_test_source_test_table_'
            ]).json()
        self.assertIsResult(result)
        self.assertEqual(len(result["result"]["output"]), 1)
        self.assertEqual(
            result["result"]["output"][0]["name"], 
            'relationships_descendant_model_favorite_color__favorite_color__source_test_source_test_table_'
        )
