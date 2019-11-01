import os
import pytest
import random
import time
from typing import Dict, Any

import yaml


@pytest.fixture
def unique_schema() -> str:
    return "test{}{:04}".format(int(time.time()), random.randint(0, 9999))


@pytest.fixture
def profiles_root(tmpdir):
    return tmpdir.mkdir('profile')


@pytest.fixture
def project_root(tmpdir):
    return tmpdir.mkdir('project')


@pytest.fixture
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


@pytest.fixture
def postgres_profile(profiles_root, postgres_profile_data) -> Dict[str, Any]:
    path = os.path.join(profiles_root, 'profiles.yml')
    with open(path, 'w') as fp:
        fp.write(yaml.safe_dump(postgres_profile_data))
    return postgres_profile_data
