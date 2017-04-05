from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

import dbt.flags

class TestContextVars(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

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
            'invocation_id'
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
                    'dev': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.schema
                    },
                    'prod': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.schema
                    }
                },
                'target': 'dev'
            }
        }

    def get_ctx_vars(self):
        field_list = ", ".join(['"{}"'.format(f) for f in self.fields])
        query = 'select {field_list} from {schema}.context'.format(
            field_list=field_list,
            schema=self.schema)

        vals = self.run_sql(query, fetch='all')
        ctx = dict([(k, v) for (k, v) in zip(self.fields, vals[0])])

        return ctx

    @attr(type='postgres')
    def test_env_vars_dev(self):
        self.run_dbt(['run'])
        ctx = self.get_ctx_vars()

        self.assertEqual(ctx['this'], '"context_vars_013"."context"')
        self.assertEqual(ctx['this.name'], 'context')
        self.assertEqual(ctx['this.schema'], 'context_vars_013')
        self.assertEqual(ctx['this.table'], 'context')

        self.assertEqual(ctx['target.dbname'], 'dbt')
        self.assertEqual(ctx['target.host'], 'database')
        self.assertEqual(ctx['target.name'], 'dev')
        self.assertEqual(ctx['target.port'], 5432)
        self.assertEqual(ctx['target.schema'], 'context_vars_013')
        self.assertEqual(ctx['target.threads'], 1)
        self.assertEqual(ctx['target.type'], 'postgres')
        self.assertEqual(ctx['target.user'], 'root')
        self.assertEqual(ctx['target.pass'], '')

    @attr(type='postgres')
    def test_env_vars_prod(self):
        self.run_dbt(['run', '--target', 'prod'])
        ctx = self.get_ctx_vars()

        self.assertEqual(ctx['this'], '"context_vars_013"."context"')
        self.assertEqual(ctx['this.name'], 'context')
        self.assertEqual(ctx['this.schema'], 'context_vars_013')
        self.assertEqual(ctx['this.table'], 'context')

        self.assertEqual(ctx['target.dbname'], 'dbt')
        self.assertEqual(ctx['target.host'], 'database')
        self.assertEqual(ctx['target.name'], 'prod')
        self.assertEqual(ctx['target.port'], 5432)
        self.assertEqual(ctx['target.schema'], 'context_vars_013')
        self.assertEqual(ctx['target.threads'], 1)
        self.assertEqual(ctx['target.type'], 'postgres')
        self.assertEqual(ctx['target.user'], 'root')
        self.assertEqual(ctx['target.pass'], '')
