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
                    # the model configs will override this
                    'materialized': 'invalid',
                    # the model configs will append to these
                    'tags': ['tag_one'],
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

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'seeds': {
                'quote_columns': False,
                'test': {
                    'seed': {
                        'enabled': False,
                    },
                },
            },
        }

    @property
    def models(self):
        return "empty-models"

    @use_profile('postgres')
    def test_postgres_disable_seed_partial_parse(self):
        self.run_dbt(['--partial-parse', 'seed'])
        self.run_dbt(['--partial-parse', 'seed'])


class TestConfigWithVars(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_039"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
            'seeds': {
                'quote_columns': False,
                'test': {
                    'vars': {
                        'something': 100,
                    },
                },
            },
        }

    @property
    def models(self):
        return "empty-models"

    @use_profile('postgres')
    def test_postgres_embedded_config_with_vars(self):
        with self.assertRaises(CompilationException) as exc:
            self.run_dbt(['seed'])

        self.assertIn('Found a "vars" dictionary in a config block', str(exc.exception))
        self.run_dbt(['seed'], strict=False)


class TestUnusedModelConfigs(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_039"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'data-paths': ['data'],
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
            }
        }

    @property
    def models(self):
        return "empty-models"

    @use_profile('postgres')
    def test_postgres_warn_unused_configuration_paths(self):
        with self.assertRaises(CompilationException) as exc:
            self.run_dbt(['seed'])

        self.assertIn('Configuration paths exist', str(exc.exception))
        self.assertIn('- sources.test', str(exc.exception))
        self.assertIn('- models.test', str(exc.exception))

        self.run_dbt(['seed'], strict=False)
