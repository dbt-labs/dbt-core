from test.integration.base import DBTIntegrationTest, use_profile

class TestBigqueryPrePostRunHooks(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.use_profile('bigquery')
        self.use_default_project()
        self.run_sql_file("test/integration/014_hook_tests/seed_run_bigquery.sql")

        self.fields = [
            'state',
            'target_name',
            'target_schema',
            'target_threads',
            'target_type',
            'run_started_at',
            'invocation_id'
        ]

    @property
    def schema(self):
        return "run_hooks_014"

    @property
    def profile_config(self):
        profile = self.bigquery_profile()
        profile['test']['outputs']['default2']['threads'] = 3
        return profile


    @property
    def project_config(self):
        return {
            'macro-paths': ['test/integration/014_hook_tests/macros'],
            'data-paths': ['test/integration/014_hook_tests/data'],

            # The create and drop table statements here validate that these hooks run
            # in the same order that they are defined. Drop before create is an error.
            # Also check that the table does not exist below.
            "on-run-start": [
                "{{ custom_run_hook_bq('start', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.start_hook_order_test ( id INT64 )",
                "drop table {{ target.schema }}.start_hook_order_test",
            ],
            "on-run-end": [
                "{{ custom_run_hook_bq('end', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.end_hook_order_test ( id INT64 )",
                "drop table {{ target.schema }}.end_hook_order_test",
            ]
        }

    @property
    def models(self):
        return "test/integration/014_hook_tests/models"

    def get_ctx_vars(self, state):
        field_list = ", ".join(self.fields)
        query = "select {field_list} from `{schema}.on_run_hook` where state = '{state}'".format(field_list=field_list, schema=self.unique_schema(), state=state)

        vals = self.run_sql(query, fetch='all')
        self.assertFalse(len(vals) == 0, 'nothing inserted into on_run_hook table')
        self.assertFalse(len(vals) > 1, 'too many rows in hooks table')
        ctx = dict(zip(self.fields, vals[0]))

        return ctx

    def check_hooks(self, state):
        ctx = self.get_ctx_vars(state)

        self.assertEqual(ctx['state'], state)
        self.assertEqual(ctx['target_name'], 'default2')
        self.assertEqual(ctx['target_schema'], self.unique_schema())
        self.assertEqual(ctx['target_threads'], 3)
        self.assertEqual(ctx['target_type'], 'bigquery')

        self.assertTrue(ctx['run_started_at'] is not None and len(ctx['run_started_at']) > 0, 'run_started_at was not set')
        self.assertTrue(ctx['invocation_id'] is not None and len(ctx['invocation_id']) > 0, 'invocation_id was not set')

    @use_profile('bigquery')
    def test_bigquery_pre_and_post_run_hooks(self):
        self.run_dbt(['run'])

        self.check_hooks('start')
        self.check_hooks('end')

        self.assertTableDoesNotExist("start_hook_order_test")
        self.assertTableDoesNotExist("end_hook_order_test")

    @use_profile('bigquery')
    def test_bigquery_pre_and_post_seed_hooks(self):
        self.run_dbt(['seed'])

        self.check_hooks('start')
        self.check_hooks('end')

        self.assertTableDoesNotExist("start_hook_order_test")
        self.assertTableDoesNotExist("end_hook_order_test")
