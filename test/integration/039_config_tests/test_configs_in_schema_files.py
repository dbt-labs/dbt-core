import os
import shutil

from test.integration.base import DBTIntegrationTest, use_profile, get_manifest, normalize
from dbt.exceptions import CompilationException, ParsingException


class TestSchemaFileConfigs(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_039-alt"

    def unique_schema(self):
        return super().unique_schema().upper()

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds-alt'],
            'models': {
                '+meta': {
                    'company': 'NuMade',
                },
                'test': {
                    '+meta': {
                        'project': 'test',
                    },
                    'tagged': {
                        '+meta': {
                            'team': 'Core Team',
                        },
                        'tags': ['tag_in_project'],
                        'model': {
                            'materialized': 'table',
                            '+meta': {
                                'owner': 'Julie Dent',
                            },
                        }
                    }
                },
            },
            'vars': {
                'test': {
                    'my_var': 'TESTING',
                }
            },
            'seeds': {
                'quote_columns': False,
            },
        }

    @property
    def models(self):
        return "models-alt"

    @use_profile('postgres')
    def test_postgres_config_layering(self):
        self.assertEqual(len(self.run_dbt(['seed'])), 1)
        # test the project-level tag, and both config() call tags
        self.assertEqual(len(self.run_dbt(['run', '--model', 'tag:tag_in_project'])), 1)
        self.assertEqual(len(self.run_dbt(['run', '--model', 'tag:tag_1_in_model'])), 1)
        self.assertEqual(len(self.run_dbt(['run', '--model', 'tag:tag_2_in_model'])), 1)
        self.assertEqual(len(self.run_dbt(['run', '--model', 'tag:tag_in_schema'])), 1)
        manifest = get_manifest()
        model_id = 'model.test.model'
        model_node = manifest.nodes[model_id]
        meta_expected = {'company': 'NuMade', 'project': 'test', 'team': 'Core Team', 'owner': 'Julie Smith', 'my_attr': 'TESTING'}
        self.assertEqual(model_node.meta, meta_expected)
        self.assertEqual(model_node.config.meta, meta_expected)
        model_tags = ['tag_1_in_model', 'tag_2_in_model', 'tag_in_project', 'tag_in_schema']
        model_node_tags = model_node.tags.copy()
        model_node_tags.sort()
        self.assertEqual(model_node_tags, model_tags)
        model_node_config_tags = model_node.config.tags.copy()
        model_node_config_tags.sort()
        self.assertEqual(model_node_config_tags, model_tags)
        model_meta = {
            'company': 'NuMade',
            'project': 'test',
            'team': 'Core Team',
            'owner': 'Julie Smith',
            'my_attr': "TESTING",
        }
        self.assertEqual(model_node.config.meta, model_meta)
        # make sure we overwrote the materialization properly
        models = self.get_models_in_schema()
        self.assertEqual(models['model'], 'table')
        self.assertTablesEqual('some_seed', 'model')
        # look for test meta
        schema_file_id = model_node.patch_path
        schema_file = manifest.files[schema_file_id]
        tests = schema_file.get_tests('models', 'model')
        self.assertIn(tests[0], manifest.nodes)
        test = manifest.nodes[tests[0]]
        expected_meta = {'owner': 'Simple Simon'}
        self.assertEqual(test.config.meta, expected_meta)
        test = manifest.nodes[tests[1]]
        expected_meta = {'owner': 'John Doe'}
        self.assertEqual(test.config.meta, expected_meta)

        # copy a schema file with multiple metas
        shutil.copyfile('extra-alt/untagged.yml', 'models-alt/untagged.yml')
        with self.assertRaises(ParsingException):
            results = self.run_dbt(["run"])

        # copy a schema file with config key in top-level of test and in config dict
        shutil.copyfile('extra-alt/untagged2.yml', 'models-alt/untagged.yml')
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["run"])

    def tearDown(self):
        if os.path.exists(normalize('models-alt/untagged.yml')):
            os.remove(normalize('models-alt/untagged.yml'))

