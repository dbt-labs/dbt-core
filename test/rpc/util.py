import base64
import json
import os
import pytest
import random
import signal
import socket
import time
from contextlib import contextmanager
from typing import Dict, Any, Optional, Union, List

import requests
import yaml

import dbt.flags
from dbt.adapters.factory import get_adapter, register_adapter
from dbt.logger import log_manager
from dbt.main import handle_and_check
from dbt.config import RuntimeConfig


def query_url(url, query: Dict[str, Any]):
    headers = {'content-type': 'application/json'}
    return requests.post(url, headers=headers, data=json.dumps(query))


class NoServerException(Exception):
    pass


class ServerProcess(dbt.flags.MP_CONTEXT.Process):
    def __init__(
        self,
        cwd,
        port,
        profiles_dir,
        cli_vars=None,
        criteria=('ready',),
        target=None,
    ):
        self.cwd = cwd
        self.port = port
        self.criteria = criteria
        self.error = None
        handle_and_check_args = [
            'rpc', '--log-cache-events',
            '--port', str(self.port),
            '--profiles-dir', profiles_dir
        ]
        dbt.flags.PROFILES_DIR = profiles_dir
        if cli_vars:
            handle_and_check_args.extend(['--vars', cli_vars])
        if target is not None:
            handle_and_check_args.extend(['--target', target])

        super().__init__(
            target=handle_and_check,
            args=(handle_and_check_args,),
            name='ServerProcess',
        )

    def run(self):
        os.chdir(self.cwd)
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
        return result['result']['state'] in self.criteria

    def status_ok(self):
        result = self.query(
            {'method': 'status', 'id': 1, 'jsonrpc': '2.0'}
        ).json()
        return self._compare_result(result)

    def is_up(self):
        if not self.can_connect():
            return False
        return self.status_ok()

    def start(self):
        super().start()
        for _ in range(30):
            if self.is_up():
                break
            time.sleep(0.5)
        if not self.can_connect():
            raise NoServerException('server never appeared!')
        status_result = self.query(
            {'method': 'status', 'id': 1, 'jsonrpc': '2.0'}
        ).json()
        if not self._compare_result(status_result):
            raise NoServerException(
                'Got invalid status result: {}'.format(status_result)
            )

    @property
    def url(self):
        return 'http://localhost:{}/jsonrpc'.format(self.port)

    def query(self, query):
        headers = {'content-type': 'application/json'}
        return requests.post(self.url, headers=headers, data=json.dumps(query))


class Querier:
    def __init__(self, server: ServerProcess):
        self.server = server

    def sighup(self):
        os.kill(self.server.pid, signal.SIGHUP)

    def build_request_data(self, method, params, request_id):
        return {
            'jsonrpc': '2.0',
            'method': method,
            'params': params,
            'id': request_id,
        }

    def request(self, method, params=None, request_id=1):
        if params is None:
            params = {}

        data = self.build_request_data(
            method=method, params=params, request_id=request_id
        )
        response = self.server.query(data)
        assert response.ok, f'invalid response from server: {response.text}'
        return response.json()

    def status(self, request_id: int = 1):
        return self.request(method='status', request_id=request_id)

    def wait_for_status(self, expected, times=120) -> bool:
        for _ in range(times):
            time.sleep(0.5)
            status = self.is_result(self.status())
            if status['state'] == expected:
                return True
        return False

    def ps(self, active=True, completed=False, request_id=1):
        params = {}
        if active is not None:
            params['active'] = active
        if completed is not None:
            params['completed'] = completed

        return self.request(method='ps', params=params, request_id=request_id)

    def kill(self, task_id: str, request_id: int = 1):
        params = {'task_id': task_id}
        return self.request(
            method='kill', params=params, request_id=request_id
        )

    def poll(
        self,
        request_token: str,
        logs: Optional[bool] = None,
        logs_start: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {
            'request_token': request_token,
        }
        if logs is not None:
            params['logs'] = logs
        if logs_start is not None:
            params['logs_start'] = logs_start
        return self.request(
            method='poll', params=params, request_id=request_id
        )

    def gc(
        self,
        task_ids: Optional[List[str]] = None,
        before: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
        request_id: int = 1,
    ):
        params = {}
        if task_ids is not None:
            params['task_ids'] = task_ids
        if before is not None:
            params['before'] = before
        if settings is not None:
            params['settings'] = settings
        return self.request(
            method='gc', params=params, request_id=request_id
        )

    def cli_args(self, cli: str, request_id: int = 1):
        return self.request(
            method='cli_args', params={'cli': cli}, request_id=request_id
        )

    def deps(self, request_id: int = 1):
        return self.request(method='deps', request_id=request_id)

    def list(self, request_id: int = 1):
        return self.request(method='list', request_id=request_id)

    def compile(
        self,
        models: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
        state: Optional[bool] = None,
    ):
        params = {}
        if models is not None:
            params['models'] = models
        if exclude is not None:
            params['exclude'] = exclude
        if threads is not None:
            params['threads'] = threads
        if state is not None:
            params['state'] = state
        return self.request(
            method='compile', params=params, request_id=request_id
        )

    def run(
        self,
        models: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
        defer: Optional[bool] = None,
        state: Optional[str] = None,
    ):
        params = {}
        if models is not None:
            params['models'] = models
        if exclude is not None:
            params['exclude'] = exclude
        if threads is not None:
            params['threads'] = threads
        if defer is not None:
            params['defer'] = defer
        if state is not None:
            params['state'] = state
        return self.request(
            method='run', params=params, request_id=request_id
        )

    def run_operation(
        self,
        macro: str,
        args: Optional[Dict[str, Any]] = None,
        request_id: int = 1,
    ):
        params: Dict[str, Any] = {'macro': macro}
        if args is not None:
            params['args'] = args
        return self.request(
            method='run-operation', params=params, request_id=request_id
        )

    def seed(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        show: bool = None,
        threads: Optional[int] = None,
        request_id: int = 1,
        state: Optional[str] = None,
    ):
        params = {}
        if select is not None:
            params['select'] = select
        if exclude is not None:
            params['exclude'] = exclude
        if show is not None:
            params['show'] = show
        if threads is not None:
            params['threads'] = threads
        if state is not None:
            params['state'] = state
        return self.request(
            method='seed', params=params, request_id=request_id
        )

    def snapshot(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
        state: Optional[str] = None,
    ):
        params = {}
        if select is not None:
            params['select'] = select
        if exclude is not None:
            params['exclude'] = exclude
        if threads is not None:
            params['threads'] = threads
        if state is not None:
            params['state'] = state
        return self.request(
            method='snapshot', params=params, request_id=request_id
        )

    def snapshot_freshness(
        self,
        select: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        """ Deprecated rpc command `snapshot-freshness` -> `source-freshness` """
        params = {}
        if select is not None:
            params['select'] = select
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='snapshot-freshness', params=params, request_id=request_id
        )

    def source_freshness(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
        state: Optional[str] = None,
    ):
        params = {}
        if select is not None:
            params['select'] = select
        if exclude is not None:
            params['exclude'] = exclude
        if threads is not None:
            params['threads'] = threads
        if state is not None:
            params['state'] = state
        return self.request(
            method='source-freshness', params=params, request_id=request_id
        )

    def test(
        self,
        models: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        data: bool = None,
        schema: bool = None,
        request_id: int = 1,
        defer: Optional[bool] = None,
        state: Optional[str] = None,
    ):
        params = {}
        if models is not None:
            params['models'] = models
        if exclude is not None:
            params['exclude'] = exclude
        if data is not None:
            params['data'] = data
        if schema is not None:
            params['schema'] = schema
        if threads is not None:
            params['threads'] = threads
        if defer is not None:
            params['defer'] = defer
        if state is not None:
            params['state'] = state
        return self.request(
            method='test', params=params, request_id=request_id
        )

    def build(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        resource_types: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
        defer: Optional[bool] = None,
        state: Optional[str] = None,
    ):
        params = {}
        if select is not None:
            params['select'] = select
        if exclude is not None:
            params['exclude'] = exclude
        if resource_types is not None:
            params['resource_types'] = resource_types
        if threads is not None:
            params['threads'] = threads
        if defer is not None:
            params['defer'] = defer
        if state is not None:
            params['state'] = state
        return self.request(
            method='build', params=params, request_id=request_id
        )

    def docs_generate(self, compile: bool = None, request_id: int = 1):
        params = {}
        if compile is not None:
            params['compile'] = True
        return self.request(
            method='docs.generate', params=params, request_id=request_id
        )

    def compile_sql(
        self,
        sql: str,
        name: str = 'test_compile',
        macros: Optional[str] = None,
        request_id: int = 1,
    ):
        sql = base64.b64encode(sql.encode('utf-8')).decode('utf-8')
        params = {
            'name': name,
            'sql': sql,
            'macros': macros,
        }
        return self.request(
            method='compile_sql', params=params, request_id=request_id
        )

    def run_sql(
        self,
        sql: str,
        name: str = 'test_run',
        macros: Optional[str] = None,
        request_id: int = 1,
    ):
        sql = base64.b64encode(sql.encode('utf-8')).decode('utf-8')
        params = {
            'name': name,
            'sql': sql,
            'macros': macros,
        }
        return self.request(
            method='run_sql', params=params, request_id=request_id
        )

    def get_manifest(self, request_id=1):
        return self.request(
            method='get-manifest', params={}, request_id=request_id
        )

    def is_result(self, data: Dict[str, Any], id=None) -> Dict[str, Any]:

        if id is not None:
            assert data['id'] == id
        assert data['jsonrpc'] == '2.0'
        if 'error' in data:
            print(data['error']['message'])
        assert 'error' not in data
        assert 'result' in data
        return data['result']

    def is_async_result(self, data: Dict[str, Any], id=None) -> str:
        result = self.is_result(data, id)
        assert 'request_token' in result
        return result['request_token']

    def is_error(self, data: Dict[str, Any], id=None) -> Dict[str, Any]:
        if id is not None:
            assert data['id'] == id
        assert data['jsonrpc'] == '2.0'
        assert 'result' not in data
        assert 'error' in data
        return data['error']

    def async_wait(
        self, token: str, timeout: int = 60, state='success'
    ) -> Dict[str, Any]:
        start = time.time()
        while True:
            time.sleep(0.5)
            response = self.poll(token)
            if 'error' in response:
                return response
            result = self.is_result(response)
            assert 'state' in result
            if result['state'] == state:
                return response
            delta = (time.time() - start)
            assert timeout > delta, \
                f'At time {delta}, never saw {state}.\nLast response: {result}'

    def async_wait_for_result(self, data: Dict[str, Any], state='success'):
        token = self.is_async_result(data)
        return self.is_result(self.async_wait(token, state=state))

    def async_wait_for_error(self, data: Dict[str, Any], state='success'):
        token = self.is_async_result(data)
        return self.is_error(self.async_wait(token, state=state))


def _first_server(cwd, cli_vars, profiles_dir, criteria, target):
    stored = None
    for _ in range(5):
        port = random.randint(20000, 65535)

        proc = ServerProcess(
            cwd=cwd,
            cli_vars=cli_vars,
            profiles_dir=str(profiles_dir),
            port=port,
            criteria=criteria,
            target=target,
        )
        try:
            proc.start()
        except NoServerException as exc:
            stored = exc
        else:
            return proc
    if stored:
        raise stored


@contextmanager
def rpc_server(
    project_dir, schema, profiles_dir, criteria='ready', target=None
):
    if isinstance(criteria, str):
        criteria = (criteria,)
    else:
        criteria = tuple(criteria)

    cli_vars = '{{test_run_schema: {}}}'.format(schema)

    proc = _first_server(project_dir, cli_vars, profiles_dir, criteria, target)
    yield proc
    if proc.is_alive():
        os.kill(proc.pid, signal.SIGKILL)
        proc.join()


class ProjectDefinition:
    def __init__(
        self,
        name='test',
        version='0.1.0',
        profile='test',
        project_data=None,
        packages=None,
        models=None,
        macros=None,
        snapshots=None,
        seeds=None,
    ):
        self.project = {
            'config-version': 2,
            'name': name,
            'version': version,
            'profile': profile,
        }
        if project_data:
            self.project.update(project_data)
        self.packages = packages
        self.models = models
        self.macros = macros
        self.snapshots = snapshots
        self.seeds = seeds

    def _write_recursive(self, path, inputs):
        for name, value in inputs.items():
            if name.endswith('.sql') or name.endswith('.csv') or name.endswith('.md'):
                path.join(name).write(value)
            elif name.endswith('.yml'):
                if isinstance(value, str):
                    data = value
                else:
                    data = yaml.safe_dump(value)
                path.join(name).write(data)
            else:
                self._write_recursive(path.mkdir(name), value)

    def write_packages(self, project_dir, remove=False):
        if remove:
            project_dir.join('packages.yml').remove()
        if self.packages is not None:
            if isinstance(self.packages, str):
                data = self.packages
            else:
                data = yaml.safe_dump(self.packages)
            project_dir.join('packages.yml').write(data)

    def write_config(self, project_dir, remove=False):
        cfg = project_dir.join('dbt_project.yml')
        if remove:
            cfg.remove()
        cfg.write(yaml.safe_dump(self.project))

    def _write_values(self, project_dir, remove, name, value):
        if remove:
            project_dir.join(name).remove()

        if value is not None:
            self._write_recursive(project_dir.mkdir(name), value)

    def write_models(self, project_dir, remove=False):
        self._write_values(project_dir, remove, 'models', self.models)

    def write_macros(self, project_dir, remove=False):
        self._write_values(project_dir, remove, 'macros', self.macros)

    def write_snapshots(self, project_dir, remove=False):
        self._write_values(project_dir, remove, 'snapshots', self.snapshots)

    def write_seeds(self, project_dir, remove=False):
        self._write_values(project_dir, remove, 'data', self.seeds)

    def write_to(self, project_dir, remove=False):
        if remove:
            project_dir.remove()
            project_dir.mkdir()
        self.write_packages(project_dir)
        self.write_config(project_dir)
        self.write_models(project_dir)
        self.write_macros(project_dir)
        self.write_snapshots(project_dir)
        self.write_seeds(project_dir)


class TestArgs:
    def __init__(self, profiles_dir, which='run-operation', kwargs={}):
        self.which = which
        self.single_threaded = False
        self.profiles_dir = profiles_dir
        self.project_dir = None
        self.profile = None
        self.target = None
        self.__dict__.update(kwargs)


def execute(adapter, sql):
    with adapter.connection_named('rpc-tests'):
        conn = adapter.connections.get_thread_connection()
        with conn.handle.cursor() as cursor:
            try:
                cursor.execute(sql)
                conn.handle.commit()

            except Exception as e:
                if conn.handle and conn.handle.closed == 0:
                    conn.handle.rollback()
                print(sql)
                print(e)
                raise
            finally:
                conn.transaction_open = False


@contextmanager
def built_schema(project_dir, schema, profiles_dir, test_kwargs, project_def):
    # make our args, write our project out
    args = TestArgs(profiles_dir=profiles_dir, kwargs=test_kwargs)
    project_def.write_to(project_dir)
    # build a config of our own
    os.chdir(project_dir)
    start = os.getcwd()
    try:
        cfg = RuntimeConfig.from_args(args)
    finally:
        os.chdir(start)
    register_adapter(cfg)
    adapter = get_adapter(cfg)
    execute(adapter, 'drop schema if exists {} cascade'.format(schema))
    execute(adapter, 'create schema {}'.format(schema))
    yield
    adapter = get_adapter(cfg)
    adapter.cleanup_connections()
    execute(adapter, 'drop schema if exists {} cascade'.format(schema))


@contextmanager
def get_querier(
    project_def,
    project_dir,
    profiles_dir,
    schema,
    test_kwargs,
    criteria='ready',
    target=None,
):
    server_ctx = rpc_server(
        project_dir=project_dir, schema=schema, profiles_dir=profiles_dir,
        criteria=criteria, target=target,
    )
    schema_ctx = built_schema(
        project_dir=project_dir, schema=schema, profiles_dir=profiles_dir,
        test_kwargs={}, project_def=project_def,
    )
    with schema_ctx, server_ctx as server:
        yield Querier(server)


def assert_has_threads(results, num_threads):
    assert 'logs' in results
    c_logs = [l for l in results['logs'] if 'Concurrency' in l['message']]
    assert len(c_logs) == 1, \
        f'Got invalid number of concurrency logs ({len(c_logs)})'
    assert 'message' in c_logs[0]
    assert f'Concurrency: {num_threads} threads' in c_logs[0]['message']


def get_write_manifest(querier, dest):
    result = querier.async_wait_for_result(querier.get_manifest())
    manifest = result['manifest']

    with open(dest, 'w') as fp:
        json.dump(manifest, fp)
