from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

import os


class TestContextVars(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        os.environ["DBT_TEST_013_ENV_VAR"] = "1"

        os.environ["DBT_TEST_013_USER"] = "root"
        os.environ["DBT_TEST_013_PASS"] = "password"

        self.fields = [
            'this',
            'this.name',
            'this.schema',
            'this.table',
            'target.dbname',
            'target.host',
            'target.name',
            'target.port',
            'target.schema',
            'target.threads',
            'target.type',
            'target.user',
            'target.pass',
            'run_started_at',
            'invocation_id',
            'env_var'
        ]

    @property
    def schema(self):
        return "context_vars_013"

    @property
    def models(self):
        return "test/integration/013_context_var_tests/models"

    @property
    def profile_config(self):
        return {
            'test': {
                'outputs': {
                    # don't use env_var's here so the integration tests can run
                    # seed sql statements and the like. default target is used
                    'dev': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': 'database',
                        'port': 5432,
                        'user': "root",
                        'pass': "password",
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                    'prod': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': 'database',
                        'port': 5432,
                        # root/password
                        'user': "{{ env_var('DBT_TEST_013_USER') }}",
                        'pass': "{{ env_var('DBT_TEST_013_PASS') }}",
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    }
                },
                'target': 'dev'
            }
        }

    def get_ctx_vars(self):
        field_list = ", ".join(['"{}"'.format(f) for f in self.fields])
        query = 'select {field_list} from {schema}.context'.format(
            field_list=field_list,
            schema=self.unique_schema())

        vals = self.run_sql(query, fetch='all')
        ctx = dict([(k, v) for (k, v) in zip(self.fields, vals[0])])

        return ctx

    @attr(type='postgres')
    def test_env_vars_dev(self):
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 1)
        ctx = self.get_ctx_vars()

        this = '"{}"."{}"."context"'.format(self.default_database,
                                            self.unique_schema())
        self.assertEqual(ctx['this'], this)

        self.assertEqual(ctx['this.name'], 'context')
        self.assertEqual(ctx['this.schema'], self.unique_schema())
        self.assertEqual(ctx['this.table'], 'context')

        self.assertEqual(ctx['target.dbname'], 'dbt')
        self.assertEqual(ctx['target.host'], 'database')
        self.assertEqual(ctx['target.name'], 'dev')
        self.assertEqual(ctx['target.port'], 5432)
        self.assertEqual(ctx['target.schema'], self.unique_schema())
        self.assertEqual(ctx['target.threads'], 1)
        self.assertEqual(ctx['target.type'], 'postgres')
        self.assertEqual(ctx['target.user'], 'root')
        self.assertEqual(ctx['target.pass'], '')

        self.assertEqual(ctx['env_var'], '1')

    @attr(type='postgres')
    def test_env_vars_prod(self):
        results = self.run_dbt(['run', '--target', 'prod'])
        self.assertEqual(len(results), 1)
        ctx = self.get_ctx_vars()

        this = '"{}"."{}"."context"'.format(self.default_database,
                                            self.unique_schema())
        self.assertEqual(ctx['this'], this)

        self.assertEqual(ctx['this.name'], 'context')
        self.assertEqual(ctx['this.schema'], self.unique_schema())
        self.assertEqual(ctx['this.table'], 'context')

        self.assertEqual(ctx['target.dbname'], 'dbt')
        self.assertEqual(ctx['target.host'], 'database')
        self.assertEqual(ctx['target.name'], 'prod')
        self.assertEqual(ctx['target.port'], 5432)
        self.assertEqual(ctx['target.schema'], self.unique_schema())
        self.assertEqual(ctx['target.threads'], 1)
        self.assertEqual(ctx['target.type'], 'postgres')
        self.assertEqual(ctx['target.user'], 'root')
        self.assertEqual(ctx['target.pass'], '')
        self.assertEqual(ctx['env_var'], '1')
