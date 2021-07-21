from test.integration.base import DBTIntegrationTest, use_profile
from dbt.exceptions import CompilationException


MODEL_PRE_HOOK = """
   insert into {{this.schema}}.on_model_hook (
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
    '{{ target.get("pass", "") }}',
    {{ target.port }},
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )
"""

MODEL_POST_HOOK = """
   insert into {{this.schema}}.on_model_hook (
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
    '{{ target.get("pass", "") }}',
    {{ target.port }},
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )
"""


class BaseTestPrePost(DBTIntegrationTest):
    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("seed_model.sql")

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

    def get_ctx_vars(self, state, count):
        field_list = ", ".join(['"{}"'.format(f) for f in self.fields])
        query = "select {field_list} from {schema}.on_model_hook where state = '{state}'".format(field_list=field_list, schema=self.unique_schema(), state=state)

        vals = self.run_sql(query, fetch='all')
        self.assertFalse(len(vals) == 0, 'nothing inserted into hooks table')
        self.assertFalse(len(vals) < count, 'too few rows in hooks table')
        self.assertFalse(len(vals) > count, 'too many rows in hooks table')
        return [{k: v for k, v in zip(self.fields, val)} for val in vals]

    def check_hooks(self, state, count=1):
        ctxs = self.get_ctx_vars(state, count=count)
        for ctx in ctxs:
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


class TestPrePostModelHooks(BaseTestPrePost):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['macros'],
            'models': {
                'test': {
                    'pre-hook': [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,

                        # outside transaction (runs first)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                    ],
                    'post-hook':[
                        # outside transaction (runs second)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},

                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_pre_and_post_model_hooks(self):
        self.run_dbt(['run'])

        self.check_hooks('start')
        self.check_hooks('end')


class TestHookRefs(BaseTestPrePost):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    'hooked': {
                        'post-hook': ['''
                        insert into {{this.schema}}.on_model_hook select
                        state,
                        '{{ target.dbname }}' as "target.dbname",
                        '{{ target.host }}' as "target.host",
                        '{{ target.name }}' as "target.name",
                        '{{ target.schema }}' as "target.schema",
                        '{{ target.type }}' as "target.type",
                        '{{ target.user }}' as "target.user",
                        '{{ target.get("pass", "") }}' as "target.pass",
                        {{ target.port }} as "target.port",
                        {{ target.threads }} as "target.threads",
                        '{{ run_started_at }}' as "run_started_at",
                        '{{ invocation_id }}' as "invocation_id"
                    from {{ ref('post') }}'''.strip()],
                    }
                },
            }
        }

    @property
    def models(self):
        return 'ref-hook-models'

    @use_profile('postgres')
    def test_postgres_pre_post_model_hooks_refed(self):
        self.run_dbt(['run'])

        self.check_hooks('start', count=1)
        self.check_hooks('end', count=1)


class TestPrePostModelHooksOnSeeds(DBTIntegrationTest):
    @property
    def schema(self):
        return "model_hooks_014"

    @property
    def models(self):
        return "seed-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'models': {},
            'seeds': {
                'post-hook': [
                    'alter table {{ this }} add column new_col int',
                    'update {{ this }} set new_col = 1'
                ],
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_hooks_on_seeds(self):
        res = self.run_dbt(['seed'])
        self.assertEqual(len(res), 1, 'Expected exactly one item')
        res = self.run_dbt(['test'])
        self.assertEqual(len(res), 1, 'Expected exactly one item')


class TestPrePostModelHooksOnSeedsPlusPrefixed(TestPrePostModelHooksOnSeeds):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'models': {},
            'seeds': {
                '+post-hook': [
                    'alter table {{ this }} add column new_col int',
                    'update {{ this }} set new_col = 1'
                ],
                'quote_columns': False,
            },
        }


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace(TestPrePostModelHooksOnSeeds):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'models': {},
            'seeds': {
                '+ post-hook': [
                    'alter table {{ this }} add column new_col int',
                    'update {{ this }} set new_col = 1'
                ],
                'quote_columns': False,
            },
        }


class TestPrePostModelHooksOnSnapshots(DBTIntegrationTest):
    @property
    def schema(self):
        return "model_hooks_014"

    @property
    def models(self):
        return "test-snapshot-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'snapshot-paths': ['test-snapshots'],
            'models': {},
            'snapshots': {
                'post-hook': [
                    'alter table {{ this }} add column new_col int',
                    'update {{ this }} set new_col = 1'
                ]
            },
            'seeds': {
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_hooks_on_snapshots(self):
        res = self.run_dbt(['seed'])
        self.assertEqual(len(res), 1, 'Expected exactly one item')
        res = self.run_dbt(['snapshot'])
        self.assertEqual(len(res), 1, 'Expected exactly one item')
        res = self.run_dbt(['test'])
        self.assertEqual(len(res), 1, 'Expected exactly one item')


class TestPrePostModelHooksInConfig(BaseTestPrePost):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['macros'],
        }

    @property
    def models(self):
        return "configured-models"

    @use_profile('postgres')
    def test_postgres_pre_and_post_model_hooks_model(self):
        self.run_dbt(['run'])

        self.check_hooks('start')
        self.check_hooks('end')

    @use_profile('postgres')
    def test_postgres_pre_and_post_model_hooks_model_and_project(self):
        self.use_default_project({
            'config-version': 2,
            'models': {
                'test': {
                    'pre-hook': [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                    ],
                    'post-hook':[
                        # outside transaction (runs second)
                        {"sql": "vacuum {{ this.schema }}.on_model_hook", "transaction": False},
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        })
        self.run_dbt(['run'])

        self.check_hooks('start', count=2)
        self.check_hooks('end', count=2)


class TestPrePostModelHooksInConfigKwargs(TestPrePostModelHooksInConfig):
    @property
    def models(self):
        return "kwargs-models"


class TestPrePostSnapshotHooksInConfigKwargs(TestPrePostModelHooksOnSnapshots):
    @property
    def models(self):
        return "test-snapshot-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'snapshot-paths': ['test-kwargs-snapshots'],
            'models': {},
            'seeds': {
                'quote_columns': False,
            },
        }


class TestDuplicateHooksInConfigs(DBTIntegrationTest):
    @property
    def schema(self):
        return "model_hooks_014"

    @property
    def models(self):
        return "error-models"

    @use_profile('postgres')
    def test_postgres_run_duplicate_hook_defs(self):
        with self.assertRaises(CompilationException) as exc:
            self.run_dbt(['run'])

        self.assertIn('pre_hook', str(exc.exception))
        self.assertIn('pre-hook', str(exc.exception))
