import os
import pytest
import random
import time
from typing import Dict, Any, Set

import yaml


def pytest_addoption(parser):
    parser.addoption(
        '--profile', default='postgres', help='Use the postgres profile',
    )


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
    config.addinivalue_line('markers', 'supported(: Marks postgres-only tests')
    config.addinivalue_line(
        'markers', 'snowflake: Mark snowflake-only tests'
    )
    config.addinivalue_line(
        'markers', 'any: Mark '
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
                    'host': 'database',
                    'port': 5432,
                    'user': 'root',
                    'pass': 'password',
                    'dbname': 'dbt',
                    'schema': unique_schema,
                },
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
