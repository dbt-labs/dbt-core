import json
import os

from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestCatalogGenerate(DBTIntegrationTest):

    def setUp(self):
        super(TestCatalogGenerate, self).setUp()
        self.run_sql_file("test/integration/029_catalog_generate_tests/seed.sql")

    @property
    def schema(self):
        return "simple_dependency_029"

    @property
    def models(self):
        return "test/integration/029_catalog_generate_tests/models"

    @property
    def project_config(self):
        return {
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project'
            ]
        }

    @attr(type='postgres')
    @attr(type='catalog')
    def test_simple_generate(self):
        self.run_dbt(["catalog", "generate"])
        self.assertTrue(os.path.exists('./target/catalog.json'))

        with open('./target/catalog.json') as fp:
            data = json.load(fp)

        my_schema_name = self.unique_schema()
        self.assertIn(my_schema_name, data)
        my_schema = data[my_schema_name]
        expected_tables = {
            'seed', 'seed_config_expected_1', 'seed_config_expected_2',
            'seed_config_expected_3', 'seed_summary'
        }
        self.assertEqual(set(my_schema), expected_tables)
        expected_summary = {
            'metadata': {
                'schema': my_schema_name,
                'name': 'seed_summary',
                'type': 'BASE TABLE',
                'comment': None,
            },
            'columns': [
                {
                    'name': 'year',
                    'index': 1,
                    'type': 'timestamp without time zone',
                    'comment': None,
                },
                {
                    'name': 'count',
                    'index': 2,
                    'type': 'bigint',
                    'comment': None
                },
            ],
        }
        self.assertEqual(expected_summary, my_schema_name)





