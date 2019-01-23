from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, use_profile


class TestSources(DBTIntegrationTest):
    @property
    def schema(self):
        return "sources_042"

    @property
    def models(self):
        return "test/integration/042_sources_test/models"

    @use_profile('postgres')
    def test_basic_source_def(self):
        os.environ['DBT_TEST_SCHEMA_NAME_VARIABLE'] = 'test_run_schema'
        self.run_sql_file('test/integration/042_sources_test/source.sql',
                          kwargs={'schema':self.unique_schema()})
        self.run_dbt([
            'run',
            '--vars',
            '{{test_run_schema: {}}}'.format(self.unique_schema())
        ])
        self.assertTablesEqual('source', 'descendant_model')
        results = self.run_dbt([
            'test',
            '--vars',
            '{{test_run_schema: {}}}'.format(self.unique_schema())
        ])
        self.assertEqual(len(results), 2)
