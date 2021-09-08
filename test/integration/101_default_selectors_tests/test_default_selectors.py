import yaml

from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile

from dbt.task.list import ListTask


class TestDefaultSelectors(DBTIntegrationTest):
    """Test the selectors default argument"""
    @property
    def schema(self):
        return "test_default_selectors_101"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "source-paths": ["models"]
        }

    @property
    def selectors_config(self):
        return yaml.safe_load('''
            selectors:
            - name: default_selector
              description: test default selector
              definition:
                method: tag
                value: marketing
              default: true
        ''')

    def list_and_assert(self, expected):
        """list resources in the project with the selectors default"""
        listed = self.run_dbt(['ls', '--resource-type', 'test'])

        assert len(listed) == len(expected)


    @use_profile('postgres')
    def test__postgres__model_a_only(self):
        expected = ['model_a']

        self.list_and_assert(expected)

