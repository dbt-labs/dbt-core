import json
import os
from datetime import datetime, timedelta

import yaml

from dbt.exceptions import CompilationException
import dbt.tracking
import dbt.version
from tests.integration.base import DBTIntegrationTest, use_profile, AnyFloat, \
    AnyStringWith


class BaseSourcesTest(DBTIntegrationTest):
    @property
    def schema(self):
        return "sources"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'quoting': {'database': True, 'schema': True, 'identifier': True},
            'seeds': {
                'quote_columns': True,
            },
        }

    def setUp(self):
        super().setUp()
        os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE'] = 'test_run_schema'

    def tearDown(self):
        del os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE']
        super().tearDown()

    def run_dbt_with_vars(self, cmd, *args, **kwargs):
        vars_dict = {
            'test_run_schema': self.unique_schema(),
            'test_loaded_at': self.adapter.quote('updated_at'),
        }
        cmd.extend(['--vars', yaml.safe_dump(vars_dict)])
        return self.run_dbt(cmd, *args, **kwargs)


class SuccessfulSourcesTest(BaseSourcesTest):
    def setUp(self):
        super().setUp()
        self.run_dbt_with_vars(['seed'])
        self.maxDiff = None
        self._id = 101
        # this is the db initial value
        self.last_inserted_time = "2016-09-19T14:45:51+00:00"
        os.environ['DBT_ENV_CUSTOM_ENV_key'] = 'value'

    def tearDown(self):
        super().tearDown()
        del os.environ['DBT_ENV_CUSTOM_ENV_key']

    def _set_updated_at_to(self, delta):
        insert_time = datetime.utcnow() + delta
        timestr = insert_time.strftime("%Y-%m-%d %H:%M:%S")
        # favorite_color,id,first_name,email,ip_address,updated_at
        insert_id = self._id
        self._id += 1
        raw_sql = """INSERT INTO {schema}.{source}
            ({quoted_columns})
        VALUES (
            'blue',{id},'Jake','abc@example.com','192.168.1.1','{time}'
        )"""
        quoted_columns = ','.join(
            c for c in
            ('favorite_color', 'id', 'first_name',
             'email', 'ip_address', 'updated_at')
        )
        self.run_sql(
            raw_sql,
            kwargs={
                'schema': self.unique_schema(),
                'time': timestr,
                'id': insert_id,
                'source': self.adapter.quote('source'),
                'quoted_columns': quoted_columns,
            }
        )
        self.last_inserted_time = insert_time.strftime(
            "%Y-%m-%dT%H:%M:%S+00:00")


class TestSourceFreshness(SuccessfulSourcesTest):

    def _assert_freshness_results(self, path, state):
        self.assertTrue(os.path.exists(path))
        with open(path) as fp:
            data = json.load(fp)

        assert set(data) == {'metadata', 'results', 'elapsed_time'}
        assert 'generated_at' in data['metadata']
        assert isinstance(data['elapsed_time'], float)
        self.assertBetween(data['metadata']['generated_at'],
                           self.freshness_start_time)
        assert data['metadata']['dbt_schema_version'] == 'https://schemas.getdbt.com/dbt/sources/v2.json'
        assert data['metadata']['dbt_version'] == dbt.version.__version__
        assert data['metadata']['invocation_id'] == dbt.tracking.active_user.invocation_id
        key = 'key'
        if os.name == 'nt':
            key = key.upper()
        assert data['metadata']['env'] == {key: 'value'}

        last_inserted_time = self.last_inserted_time

        self.assertEqual(len(data['results']), 1)

        self.assertEqual(data['results'], [
            {
                'unique_id': 'source.test.test_source.test_table',
                'max_loaded_at': last_inserted_time,
                'snapshotted_at': AnyStringWith(),
                'max_loaded_at_time_ago_in_s': AnyFloat(),
                'status': state,
                'criteria': {
                    'filter': None,
                    'warn_after': {'count': 10, 'period': 'hour'},
                    'error_after': {'count': 18, 'period': 'hour'},
                },
                'adapter_response': {},
                'thread_id': AnyStringWith('Thread-'),
                'execution_time': AnyFloat(),
                'timing': [
                    {
                        'name': 'compile',
                        'started_at': AnyStringWith(),
                        'completed_at': AnyStringWith(),
                    },
                    {
                        'name': 'execute',
                        'started_at': AnyStringWith(),
                        'completed_at': AnyStringWith(),
                    }
                ]
            }
        ])

    def _run_source_freshness(self):
        # test_source.test_table should have a loaded_at field of `updated_at`
        # and a freshness of warn_after: 10 hours, error_after: 18 hours
        # by default, our data set is way out of date!
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/error_source.json'],
            expect_pass=False
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'error')
        self._assert_freshness_results('target/error_source.json', 'error')

        self._set_updated_at_to(timedelta(hours=-12))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/warn_source.json'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'warn')
        self._assert_freshness_results('target/warn_source.json', 'warn')

        self._set_updated_at_to(timedelta(hours=-2))
        self.freshness_start_time = datetime.utcnow()
        results = self.run_dbt_with_vars(
            ['source', 'freshness', '-o', 'target/pass_source.json'],
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'pass')
        self._assert_freshness_results('target/pass_source.json', 'pass')

    @use_profile('bigquery')
    def test_bigquery_source_freshness(self):
        self._run_source_freshness()
