import pytest

from dbt.tests.util import run_dbt_and_capture
from tests.functional.simple_copy.test_simple_copy import models, seeds


@pytest.fixture
def project_config_update():
    return {
        'seeds': {'quote_columns': False},
        'quoting': {'database': True},
    }

def test_quoted_database(project):

   # run seed command
   results, log_output = run_dbt_and_capture(['--debug', '--single-threaded', 'seed'])

   assert f'create table "dbt"."{project.test_schema}"."seed"' in log_output
   # I'm not sure why we were testing for this string
   assert 'create schema if not exists' not in log_output
