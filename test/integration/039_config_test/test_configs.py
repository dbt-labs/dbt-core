
from test.integration.base import DBTIntegrationTest, use_profile


class TestConfigs(DBTIntegrationTest):
    @property
    def schema(self):
        return "config_039"

    def unique_schema(self):
        return super(TestConfigs, self).unique_schema().upper()

    @property
    def project_config(self):
        return {
            'data-paths': ['test/integration/039_config_test/data'],
            'models': {
                'test': {
                    # the model configs will override this
                    'materialized': 'invalid',
                    # the model configs will append to these
                    'tags': ['tag_one'],
                },
            },
        }

    @property
    def models(self):
        return "test/integration/039_config_test/models"

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
