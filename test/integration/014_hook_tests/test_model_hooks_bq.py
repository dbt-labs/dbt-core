from test.integration.base import DBTIntegrationTest, use_profile

MODEL_PRE_HOOK = """
   insert into {{this.schema}}.on_model_hook (
        state,
        target_name,
        target_schema,
        target_type,
        target_threads,
        run_started_at,
        invocation_id
   ) VALUES (
    'start',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )
"""


MODEL_POST_HOOK = """
   insert into {{this.schema}}.on_model_hook (
        state,
        target_name,
        target_schema,
        target_type,
        target_threads,
        run_started_at,
        invocation_id
   ) VALUES (
    'end',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )
"""

class TestBigqueryPrePostModelHooks(DBTIntegrationTest):
    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/014_hook_tests/seed_model_bigquery.sql")

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
        return "model_hooks_014"

    @property
    def profile_config(self):
        profile = self.bigquery_profile()
        profile['test']['outputs']['default2']['threads'] = 3
        return profile

    @property
    def project_config(self):
        return {
            'macro-paths': ['test/integration/014_hook_tests/macros'],
            'models': {
                'test': {
                    'pre-hook': [MODEL_PRE_HOOK],

                    'post-hook':[MODEL_POST_HOOK]
                }
            }
        }

    @property
    def models(self):
        return "test/integration/014_hook_tests/models"

    def get_ctx_vars(self, state):
        field_list = ", ".join(self.fields)
        query = "select {field_list} from `{schema}.on_model_hook` where state = '{state}'".format(field_list=field_list, schema=self.unique_schema(), state=state)

        vals = self.run_sql(query, fetch='all')
        self.assertFalse(len(vals) == 0, 'nothing inserted into hooks table')
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
    def test_pre_and_post_model_hooks_bigquery(self):
        self.run_dbt(['run'])

        self.check_hooks('start')
        self.check_hooks('end')


class TestBigqueryPrePostModelHooksOnSeeds(DBTIntegrationTest):
    @property
    def schema(self):
        return "model_hooks_014"

    @property
    def models(self):
        return "test/integration/014_hook_tests/seed-models-bq"

    @property
    def project_config(self):
        return {
            'data-paths': ['test/integration/014_hook_tests/data'],
            'models': {},
            'seeds': {
                'post-hook': [
                    'insert into {{ this }} (a, b, c) VALUES (10, 11, 12)',
                ]
            }
        }

    @use_profile('bigquery')
    def test_hooks_on_seeds_bigquery(self):
        res = self.run_dbt(['seed'])
        self.assertEqual(len(res), 1, 'Expected exactly one item')
        res = self.run_dbt(['test'])
        self.assertEqual(len(res), 1, 'Expected exactly one item')
        result = self.run_sql(
            'select a, b, c from `{schema}`.`example_seed` where a = 10',
            fetch='all'
        )
        self.assertFalse(len(result) == 0, 'nothing inserted into table by hook')
        self.assertFalse(len(result) > 1, 'too many rows in table')
