from test.integration.base import DBTIntegrationTest, use_profile
import os


class TestPrePostRunHooks(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("seed_run.sql")

        self.fields = [
            'state',
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
        os.environ['TERM_TEST'] = 'TESTING'

    @property
    def schema(self):
        return "run_hooks_014"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['macros'],
            'seed-paths': ['seeds'],

            # The create and drop table statements here validate that these hooks run
            # in the same order that they are defined. Drop before create is an error.
            # Also check that the table does not exist below.
            "on-run-start": [
                "{{ custom_run_hook('start', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.start_hook_order_test ( id int )",
                "drop table {{ target.schema }}.start_hook_order_test",
                "{{ log(env_var('TERM_TEST'), info=True) }}",
            ],
            "on-run-end": [
                "{{ custom_run_hook('end', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.end_hook_order_test ( id int )",
                "drop table {{ target.schema }}.end_hook_order_test",
                "create table {{ target.schema }}.schemas ( schema text )",
                "insert into {{ target.schema }}.schemas (schema) values {% for schema in schemas %}( '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
                "create table {{ target.schema }}.db_schemas ( db text, schema text )",
                "insert into {{ target.schema }}.db_schemas (db, schema) values {% for db, schema in database_schemas %}('{{ db }}', '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
            ],
            'seeds': {
                'quote_columns': False,
            },
        }

    @property
    def models(self):
        return "models"

    def get_ctx_vars(self, state):
        field_list = ", ".join(['"{}"'.format(f) for f in self.fields])
        query = "select {field_list} from {schema}.on_run_hook where state = '{state}'".format(field_list=field_list, schema=self.unique_schema(), state=state)

        vals = self.run_sql(query, fetch='all')
        self.assertFalse(len(vals) == 0, 'nothing inserted into on_run_hook table')
        self.assertFalse(len(vals) > 1, 'too many rows in hooks table')
        ctx = dict([(k,v) for (k,v) in zip(self.fields, vals[0])])

        return ctx

    def assert_used_schemas(self):
        schemas_query = 'select * from {}.schemas'.format(self.unique_schema())
        results = self.run_sql(schemas_query, fetch='all')
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], self.unique_schema())

        db_schemas_query = 'select * from {}.db_schemas'.format(self.unique_schema())
        results = self.run_sql(db_schemas_query, fetch='all')
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], self.default_database)
        self.assertEqual(results[0][1], self.unique_schema())

    def check_hooks(self, state):
        ctx = self.get_ctx_vars(state)

        self.assertEqual(ctx['state'], state)
        self.assertEqual(ctx['target.dbname'], 'dbt')
        self.assertEqual(ctx['target.host'], self.database_host)
        self.assertEqual(ctx['target.name'], 'default2')
        self.assertEqual(ctx['target.port'], 5432)
        self.assertEqual(ctx['target.schema'], self.unique_schema())
        self.assertEqual(ctx['target.threads'], 4)
        self.assertEqual(ctx['target.type'], 'postgres')
        self.assertEqual(ctx['target.user'], 'root')
        self.assertEqual(ctx['target.pass'], '')

        self.assertTrue(ctx['run_started_at'] is not None and len(ctx['run_started_at']) > 0, 'run_started_at was not set')
        self.assertTrue(ctx['invocation_id'] is not None and len(ctx['invocation_id']) > 0, 'invocation_id was not set')

    @use_profile('postgres')
    def test__postgres__pre_and_post_run_hooks(self):
        self.run_dbt(['run'])

        self.check_hooks('start')
        self.check_hooks('end')

        self.assertTableDoesNotExist("start_hook_order_test")
        self.assertTableDoesNotExist("end_hook_order_test")
        self.assert_used_schemas()

    @use_profile('postgres')
    def test__postgres__pre_and_post_seed_hooks(self):
        self.run_dbt(['seed'])

        self.check_hooks('start')
        self.check_hooks('end')

        self.assertTableDoesNotExist("start_hook_order_test")
        self.assertTableDoesNotExist("end_hook_order_test")
        self.assert_used_schemas()
