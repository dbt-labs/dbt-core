# NOTE: primer on pytest fixtures and fixture scope -- https://docs.pytest.org/en/stable/fixture.html
import os
import pytest
import random
import time
from typing import Dict, Any
from argparse import Namespace
import dbt.flags as flags
from dbt.config.runtime import RuntimeConfig
from dbt.adapters.factory import get_adapter, register_adapter
from dbt.tests.util import run_sql_file

import yaml


@pytest.fixture
def unique_schema() -> str:
    return 'test{}{:04}'.format(int(time.time()), random.randint(0, 9999))


@pytest.fixture
def profiles_root(tmpdir):
    # tmpdir docs - https://docs.pytest.org/en/6.2.x/tmpdir.html
    return tmpdir.mkdir('profile')


# This used the pytest 'tmpdir' fixture to create a directory for the project,
# called 'project_dir'
@pytest.fixture
def project_root(tmpdir):
    # tmpdir docs - https://docs.pytest.org/en/6.2.x/tmpdir.html
    return tmpdir.mkdir('project')


# Make this a separate fixture so that if we want to change the location
# of data files (like seed.sql), we can.
@pytest.fixture
def data_dir(request):
    return request.fspath.dirname


@pytest.fixture
def database_host():
    return os.environ.get('DOCKER_TEST_DATABASE_HOST', 'localhost')


@pytest.fixture
def dbt_profile_data(unique_schema, database_host):

    return {
        'config': {'send_anonymous_usage_stats': False},
        'test': {
            'outputs': {
                'default': {
                    'type': 'postgres',
                    'threads': 4,
                    'host': database_host,
                    'port': int(os.getenv('POSTGRES_TEST_PORT', 5432)),
                    'user': os.getenv('POSTGRES_TEST_USER', 'root'),
                    'pass': os.getenv('POSTGRES_TEST_PASS', 'password'),
                    'dbname': os.getenv('POSTGRES_TEST_DATABASE', 'dbt'),
                    'schema': unique_schema,
                },
                'other_schema': {
                    'type': 'postgres',
                    'threads': 4,
                    'host': database_host,
                    'port': int(os.getenv('POSTGRES_TEST_PORT', 5432)),
                    'user': 'noaccess',
                    'pass': 'password',
                    'dbname': os.getenv('POSTGRES_TEST_DATABASE', 'dbt'),
                    'schema': unique_schema + '_alt',  # Should this be the same unique_schema?
                },
            },
            'target': 'default',
        },
    }


@pytest.fixture
def profiles_yml(profiles_root, dbt_profile_data):
    os.environ['DBT_PROFILES_DIR'] = str(profiles_root)
    flags.PROFILES_DIR = str(profiles_root)
    path = os.path.join(profiles_root, 'profiles.yml')
    with open(path, 'w') as fp:
        fp.write(yaml.safe_dump(dbt_profile_data))
    yield dbt_profile_data
    del os.environ['DBT_PROFILES_DIR']


@pytest.fixture
def project_config_update():
    return {}


@pytest.fixture
def dbt_project_yml(project_root, project_config_update):
    project_config = {
        'config-version': 2,
        'name': 'test',
        'version': '0.1.0',
        'profile': 'test',
    }
    if project_config_update:
        project_config.update(project_config_update)
    runtime_config_file = project_root.join('dbt_project.yml')
    runtime_config_file.write(yaml.safe_dump(project_config))
    os.chdir(project_root)


@pytest.fixture
def packages():
    return {}


@pytest.fixture
def packages_yml(project_root, packages):
    if packages:
        if isinstance(packages, str):
            data = packages
        else:
            data = yaml.safe_dump(packages)
        project_root.join('packages.yml').write(data)


@pytest.fixture
def selectors():
    return {}


@pytest.fixture
def selectors_yml(project_root, selectors):
    if selectors:
        if isinstance(selectors, str):
            data = selectors
        else:
            data = yaml.safe_dump(selectors)
        project_root.join('selectors.yml').write(data)


@pytest.fixture
def schema(unique_schema, project_root, profiles_root):
    # Dummy args just to get adapter up and running
    args = Namespace(profiles_dir=str(profiles_root), project_dir=str(project_root))
    runtime_config = RuntimeConfig.from_args(args)

    register_adapter(runtime_config)
    adapter = get_adapter(runtime_config)
    execute(adapter, 'drop schema if exists {} cascade'.format(unique_schema))
    execute(adapter, 'create schema {}'.format(unique_schema))
    yield adapter
    adapter = get_adapter(runtime_config)
    adapter.cleanup_connections()
    execute(adapter, 'drop schema if exists {} cascade'.format(unique_schema))


def execute(adapter, sql, connection_name='tests'):
    with adapter.connection_named(connection_name):
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


# Start at directory level.
def write_project_files(project_root, dir_name, file_dict):
    path = project_root.mkdir(dir_name)
    if file_dict:
        write_project_files_recursively(path, file_dict)


# Write files out from file_dict. Can be nested directories...
def write_project_files_recursively(path, file_dict):
    for name, value in file_dict.items():
        if name.endswith('.sql') or name.endswith('.csv') or name.endswith('.md'):
            path.join(name).write(value)
        elif name.endswith('.yml') or name.endswith('.yaml'):
            if isinstance(value, str):
                data = value
            else:
                data = yaml.safe_dump(value)
            path.join(name).write(data)
        else:
            write_project_files_recursively(path.mkdir(name), value)


@pytest.fixture
def models():
    return {}


@pytest.fixture
def macros():
    return {}


@pytest.fixture
def seeds():
    return {}


@pytest.fixture
def snapshots():
    return {}


@pytest.fixture
def tests():
    return {}


@pytest.fixture
def project_files(project_root, models, macros, snapshots, seeds, tests):
    write_project_files(project_root, 'models', models)
    write_project_files(project_root, 'macros', macros)
    write_project_files(project_root, 'snapshots', snapshots)
    write_project_files(project_root, 'seeds', seeds)
    write_project_files(project_root, 'tests', tests)


class TestProjInfo:
    def __init__(
        self, project_root, profiles_dir, adapter, test_dir, data_dir, test_schema, database
    ):
        self.project_root = project_root
        self.profiles_dir = profiles_dir
        self.adapter = adapter
        self.test_dir = test_dir
        self.data_dir = data_dir
        self.test_schema = test_schema
        self.database = database


@pytest.fixture
def project(
    project_root,
    profiles_root,
    request,
    unique_schema,
    profiles_yml,
    dbt_project_yml,
    packages_yml,
    selectors_yml,
    schema,
    project_files,
    data_dir,
):
    # Return whatever is needed later in tests but can only come from fixtures, so we can keep the signatures
    # in the test signature to a minimum. Also... database? ...?
    return TestProjInfo(
        project_root=project_root,
        profiles_dir=profiles_root,
        adapter=schema,
        test_dir=request.fspath.dirname,
        data_dir=data_dir,
        test_schema=unique_schema,
        # the following feels kind of fragile. TODO: better way of getting database
        database=profiles_yml['test']['outputs']['default']['dbname'],
    )


@pytest.fixture
def create_tables(data_dir, unique_schema):
    path = os.path.join(data_dir, 'seed.sql')
    run_sql_file(path, unique_schema)