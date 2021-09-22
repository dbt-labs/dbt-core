import os
import shutil

from test.integration.base import DBTIntegrationTest, use_profile
from dbt.exceptions import CompilationException


class TestConfigs(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_039"

    def unique_schema(self):
        return super().unique_schema().upper()

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'models': {
                'test': {
                    'tagged': {
                        # the model configs will override this
                        'materialized': 'invalid',
                        # the model configs will append to these
                        'tags': ['tag_one'],
                    }
                },
            },
            'seeds': {
                'quote_columns': False,
            },
        }

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_config_layering(self):
        self.assertEqual(len(self.run_dbt(['seed'])), 1)
        # test the project-level tag, and both config() call tags
        self.assertEqual(len(self.run_dbt(['run', '--model', 'tag:tag_one'])), 1)
        self.assertEqual(len(self.run_dbt(['run', '--model', 'tag:tag_two'])), 1)
        self.assertEqual(len(self.run_dbt(['run', '--model', 'tag:tag_three'])), 1)
        self.assertTablesEqual('seed', 'model')
        # make sure we overwrote the materialization properly
        models = self.get_models_in_schema()
        self.assertEqual(models['model'], 'table')


class TestTargetConfigs(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_039"

    def unique_schema(self):
        return super().unique_schema().upper()

    @property
    def models(self):
        return "models"

    def setUp(self):
        super().setUp()
        self.init_targets = [d for d in os.listdir('.') if os.path.isdir(d) and d.startswith('target_')]

    def tearDown(self):
        super().tearDown()
        for d in self.new_dirs():
            shutil.rmtree(d)

    def new_dirs(self):
        for d in os.listdir('.'):
            if os.path.isdir(d) and d not in self.init_targets and d.startswith('target_'):
                yield d

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'target-path': "target_{{ modules.datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S') }}",
            'seeds': {
                'quote_columns': False,
            },
        }

    @use_profile('postgres')
    def test_postgres_alternative_target_paths(self):
        self.run_dbt(['seed'])
        dirs = list(self.new_dirs())
        self.assertEqual(len(dirs), 1)
        self.assertTrue(os.path.exists(os.path.join(dirs[0], 'manifest.json')))


class TestDisabledConfigs(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_039"

    def postgres_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'postgres',
                        # make sure you can do this and get an int out
                        'threads': "{{ (1 + 3) | as_number }}",
                        'host': self.database_host,
                        'port': "{{ (5400 + 32) | as_number }}",
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                    'disabled': {
                        'type': 'postgres',
                        # make sure you can do this and get an int out
                        'threads': "{{ (1 + 3) | as_number }}",
                        'host': self.database_host,
                        'port': "{{ (5400 + 32) | as_number }}",
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                },
                'target': 'default2'
            }
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'test-paths': ['tests'],
            'models': {
                'test': {
                    'enabled': "{{ (target.name == 'default2' | as_bool) }}",
                },
            },
            # set the `var` result in schema.yml to be 'seed', so that the
            # `source` call can suceed.
            'vars': {
                'test': {
                    'seed_name': 'seed',
                }
            },
            'seeds': {
                'quote_columns': False,
                'test': {
                    'seed': {
                        'enabled': "{{ (target.name == 'default2') | as_bool }}",
                    },
                },
            },
            'tests': {
                'test': {
                    'enabled': "{{ (target.name == 'default2') | as_bool }}",
                    'severity': 'WARN'
                },
            },
        }

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_disable_seed_partial_parse(self):
        self.run_dbt(['--partial-parse', 'seed', '--target', 'disabled'])
        self.run_dbt(['--partial-parse', 'seed', '--target', 'disabled'])

    @use_profile('postgres')
    def test_postgres_conditional_model(self):
        # no seeds/models - enabled should eval to False because of the target
        results = self.run_dbt(['seed', '--target', 'disabled'])
        self.assertEqual(len(results), 0)
        results = self.run_dbt(['run', '--target', 'disabled'])
        self.assertEqual(len(results), 0)
        results = self.run_dbt(['test', '--target', 'disabled'])
        self.assertEqual(len(results), 0)

        # has seeds/models - enabled should eval to True because of the target
        results = self.run_dbt(['seed'])
        self.assertEqual(len(results), 1)
        results = self.run_dbt(['run'])
        self.assertEqual(len(results), 2)
        results = self.run_dbt(['test'])
        self.assertEqual(len(results), 5)


class TestUnusedModelConfigs(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_039"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'test-paths': ['does-not-exist'],
            'models': {
                'test': {
                    'enabled': True,
                }
            },
            'seeds': {
                'quote_columns': False,
            },
            'sources': {
                'test': {
                    'enabled': True,
                }
            },
            'tests': {
                'test': {
                    'enabled': True,
                }
            },
        }

    @property
    def models(self):
        return "empty-models"

    @use_profile('postgres')
    def test_postgres_warn_unused_configuration_paths(self):
        with self.assertRaises(CompilationException) as exc:
            self.run_dbt(['--warn-error', 'seed'])

        self.assertIn('Configuration paths exist', str(exc.exception))
        self.assertIn('- sources.test', str(exc.exception))
        self.assertIn('- models.test', str(exc.exception))
        self.assertIn('- models.test', str(exc.exception))

        self.run_dbt(['seed'])

class TestConfigIndivTests(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_039"

    @property
    def models(self):
        return "models"
    
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'test-paths': ['tests'],
            'seeds': {
                'quote_columns': False,
            },
            'vars': {
                'test': {
                    'seed_name': 'seed',
                }
            },
            'tests': {
                'test': {
                    'enabled': True,
                    'severity': 'WARN'
                }
            }
        }

    @use_profile('postgres')
    def test_postgres_configuring_individual_tests(self):
        self.assertEqual(len(self.run_dbt(['seed'])), 1)
        self.assertEqual(len(self.run_dbt(['run'])), 2)
        
        # all tests on (minus sleeper_agent) + WARN
        self.assertEqual(len(self.run_dbt(['test'])), 5)
        
        # turn off two of them directly
        self.assertEqual(len(self.run_dbt(['test', '--vars', '{"enabled_direct": False}'])), 3)
        
        # turn on sleeper_agent data test directly
        self.assertEqual(len(self.run_dbt(['test', '--models', 'sleeper_agent', 
            '--vars', '{"enabled_direct": True}'])), 1)
        
        # set three to ERROR directly
        results = self.run_dbt(['test', '--models', 'config.severity:error',
            '--vars', '{"enabled_direct": True, "severity_direct": "ERROR"}'
            ], expect_pass = False)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status, 'fail')
        self.assertEqual(results[1].status, 'fail')
