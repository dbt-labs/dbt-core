import yaml
from test.integration.base import DBTIntegrationTest, use_profile


class TestDefaultSelectors(DBTIntegrationTest):
    """Test the selectors' default argument"""
    @property
    def schema(self):
        return 'test_default_selectors_101'

    @property
    def models(self):
        return 'models'

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'model-paths': ['models'],
            'seed-paths': ['seeds'],
            'seeds': {
                'quote_columns': False,
            },
        }

    @property
    def selectors_config(self):
        return yaml.safe_load('''
            selectors:
            - name: default_selector
              description: test default selector
              definition:
                union:
                  - method: source
                    value: "test.src.source_a"
                  - method: fqn
                    value: "model_a"
              default: true
        ''')

    def list_and_assert(self, expected):
        '''list resources in the project with the selectors default'''
        listed = self.run_dbt(['ls', '--resource-type', 'model'])

        assert len(listed) == len(expected)

    def compile_and_assert(self, expected):
        '''Compile project with the selectors default'''
        compiled = self.run_dbt(['compile'])

        assert len(compiled.results) == len(expected)
        assert compiled.results[0].node.name == expected[0]

    def run_and_assert(self, expected):
        run = self.run_dbt(['run'])

        assert len(run.results) == len(expected)
        assert run.results[0].node.name == expected[0]

    def freshness_and_assert(self, expected):
        self.run_dbt(['seed', '-s', 'test.model_c'])
        freshness = self.run_dbt(['source', 'freshness'])

        assert len(freshness.results) == len(expected)
        assert freshness.results[0].node.name == expected[0]

    @use_profile('postgres')
    def test__postgres__model_a_only(self):
        expected_model = ['model_a']

        self.list_and_assert(expected_model)
        self.compile_and_assert(expected_model)

    def test__postgres__source_a_only(self):
        expected_source = ['source_a']

        self.freshness_and_assert(expected_source)