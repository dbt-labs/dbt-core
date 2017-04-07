from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


MODEL_PRE_HOOK = """
   insert into model_hooks_014.on_model_hook (
        "state",
        "target.dbname",
        "target.host",
        "target.name",
        "target.schema",
        "target.type",
        "target.user",
        "target.pass",
        "target.port",
        "target.threads",
        "run_started_at",
        "invocation_id"
   ) VALUES (
    'start',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.pass }}',
    {{ target.port }},
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )
"""

MODEL_POST_HOOK = """
   insert into model_hooks_014.on_model_hook (
        "state",
        "target.dbname",
        "target.host",
        "target.name",
        "target.schema",
        "target.type",
        "target.user",
        "target.pass",
        "target.port",
        "target.threads",
        "run_started_at",
        "invocation_id"
   ) VALUES (
    'end',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.pass }}',
    {{ target.port }},
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )
"""


class TestPrePostModelHooks(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/014_hook_tests/seed_model.sql")

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

    @property
    def schema(self):
        return "model_hooks_014"

    @property
    def project_config(self):
        return {
            'macro-paths': ['test/integration/014_hook_tests/macros'],
            'models': {
                'test': {
                    'pre-hook': MODEL_PRE_HOOK,
                    'post-hook': MODEL_POST_HOOK,
                }
            }
        }

    @property
    def models(self):
        return "test/integration/014_hook_tests/models"

    def get_ctx_vars(self, state):
        field_list = ", ".join(['"{}"'.format(f) for f in self.fields])
        query = "select {field_list} from {schema}.on_model_hook where state = '{state}'".format(field_list=field_list, schema=self.schema, state=state)

        vals = self.run_sql(query, fetch='all')
        self.assertFalse(len(vals) == 0, 'nothing inserted into hooks table')
        self.assertFalse(len(vals) > 1, 'too many rows in hooks table')
        ctx = dict([(k,v) for (k,v) in zip(self.fields, vals[0])])

        return ctx

    def check_hooks(self, state):
        ctx = self.get_ctx_vars(state)

        self.assertEqual(ctx['state'], state)
        self.assertEqual(ctx['target.dbname'], 'dbt')
        self.assertEqual(ctx['target.host'], 'database')
        self.assertEqual(ctx['target.name'], 'default2')
        self.assertEqual(ctx['target.port'], 5432)
        self.assertEqual(ctx['target.schema'], self.schema)
        self.assertEqual(ctx['target.threads'], 4)
        self.assertEqual(ctx['target.type'], 'postgres')
        self.assertEqual(ctx['target.user'], 'root')
        self.assertEqual(ctx['target.pass'], '')

        self.assertTrue(ctx['run_started_at'] is not None and len(ctx['run_started_at']) > 0, 'run_started_at was not set')
        self.assertTrue(ctx['invocation_id'] is not None and len(ctx['invocation_id']) > 0, 'invocation_id was not set')

    @attr(type='postgres')
    def test_pre_and_post_model_hooks(self):
        self.run_dbt(['run'])

        self.check_hooks('start')
        self.check_hooks('end')
