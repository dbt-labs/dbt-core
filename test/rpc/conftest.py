import os
import sys
import pytest
import random
import time
from typing import Dict, Any, Set

import yaml


def pytest_addoption(parser):
    parser.addoption(
        '--profile', default='postgres', help='Use the postgres profile',
    )

def pytest_runtest_setup(item):
    # this is a hack in place to work around marking tests at the module level
    # https://github.com/pytest-dev/pytest/issues/5830
    if os.name == 'nt':
        pytest.skip('"dbt rpc" not supported on windows')

def _get_item_profiles(item) -> Set[str]:
    supported = set()
    for mark in item.iter_markers(name='supported'):
        supported.update(mark.args)
    return supported


def pytest_collection_modifyitems(config, items):
    selected_profile = config.getoption('profile')

    to_remove = []

    for item in items:
        item_profiles = _get_item_profiles(item)
        if selected_profile not in item_profiles and 'any' not in item_profiles:
            to_remove.append(item)

    for item in to_remove:
        items.remove(item)


def pytest_configure(config):
    # the '(plugin, ...)' part isn't really important: any positional arguments
    # to `pytest.mark.supported` will be consumed as plugin names.
    helptxt = 'Marks supported test types ("postgres", "snowflake", "any")'
    config.addinivalue_line(
        'markers', f'supported(plugin, ...): {helptxt}'
    )


@pytest.fixture
def unique_schema() -> str:
    return "test{}{:04}".format(int(time.time()), random.randint(0, 9999))


@pytest.fixture
def profiles_root(tmpdir):
    return tmpdir.mkdir('profile')


@pytest.fixture
def project_root(tmpdir):
    return tmpdir.mkdir('project')

def postgres_profile_data(unique_schema):

    return {
        'config': {
            'send_anonymous_usage_stats': False
        },
        'test': {
            'outputs': {
                'default': {
                    'type': 'postgres',
                    'threads': 4,
                    'host': os.environ.get('POSTGRES_TEST_HOST', 'localhost'),
                    'port': int(os.environ.get('POSTGRES_TEST_PORT', 5432)),
                    'user': os.environ.get('POSTGRES_TEST_USER', 'root'),
                    'pass': os.environ.get('POSTGRES_TEST_PASS', 'password'),
                    'dbname': os.environ.get('POSTGRES_TEST_DATABASE', 'dbt'),
                    'schema': unique_schema,
                },
                'other_schema': {
                    'type': 'postgres',
                    'threads': 4,
                    'host': os.environ.get('POSTGRES_TEST_HOST', 'localhost'),
                    'port': int(os.environ.get('POSTGRES_TEST_PORT', 5432)),
                    'user': os.environ.get('POSTGRES_TEST_USER', 'root'),
                    'pass': os.environ.get('POSTGRES_TEST_PASS', 'password'),
                    'dbname': os.environ.get('POSTGRES_TEST_DATABASE', 'dbt'),
                    'schema': unique_schema+'_alt',
                }
            },
            'target': 'default'
        }
    }


def snowflake_profile_data(unique_schema):
    return {
        'config': {
            'send_anonymous_usage_stats': False
        },
        'test': {
            'outputs': {
                'default': {
                    'type': 'snowflake',
                    'threads': 4,
                    'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
                    'user': os.getenv('SNOWFLAKE_TEST_USER'),
                    'password': os.getenv('SNOWFLAKE_TEST_PASSWORD'),
                    'database': os.getenv('SNOWFLAKE_TEST_DATABASE'),
                    'schema': unique_schema,
                    'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
                },
                'keepalives': {
                    'type': 'snowflake',
                    'threads': 4,
                    'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
                    'user': os.getenv('SNOWFLAKE_TEST_USER'),
                    'password': os.getenv('SNOWFLAKE_TEST_PASSWORD'),
                    'database': os.getenv('SNOWFLAKE_TEST_DATABASE'),
                    'schema': unique_schema,
                    'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
                    'client_session_keep_alive': True,
                },
            },
            'target': 'default',
        },
    }


@pytest.fixture
def dbt_profile_data(unique_schema, pytestconfig):
    profile_name = pytestconfig.getoption('profile')
    if profile_name == 'postgres':
        return postgres_profile_data(unique_schema)
    elif profile_name == 'snowflake':
        return snowflake_profile_data(unique_schema)
    else:
        print(f'Bad profile name {profile_name}!')
        return {}


@pytest.fixture
def dbt_profile(profiles_root, dbt_profile_data) -> Dict[str, Any]:
    path = os.path.join(profiles_root, 'profiles.yml')
    with open(path, 'w') as fp:
        fp.write(yaml.safe_dump(dbt_profile_data))
    return dbt_profile_data
