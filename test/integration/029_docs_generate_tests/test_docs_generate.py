import json
import os

from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


class TestDocsGenerate(DBTIntegrationTest):

    def setUp(self):
        super(TestDocsGenerate, self).setUp()
        self.run_sql_file("test/integration/029_docs_generate_tests/seed.sql")

    @property
    def schema(self):
        return "simple_dependency_029"

    @property
    def models(self):
        return "test/integration/029_docs_generate_tests/models"

    @property
    def project_config(self):
        return {
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project'
            ]
        }

    @attr(type='postgres')
    def test_simple_generate(self):
        self.run_dbt(["deps"])
        self.run_dbt(["docs", "generate"])
        self.assertTrue(os.path.exists('./target/catalog.json'))

        with open('./target/catalog.json') as fp:
            data = json.load(fp)

        my_schema_name = self.unique_schema()
        self.assertIn(my_schema_name, data)
        my_schema = data[my_schema_name]
        expected = {
            'seed': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'seed',
                    'type': 'BASE TABLE',
                    'comment': None
                },
                'columns': [
                    {
                        'name': 'id',
                        'index': 1,
                        'type': 'integer',
                        'comment': None
                    },
                    {
                        'name': 'first_name',
                        'index': 2,
                        'type': 'character varying',
                        'comment': None
                    },
                    {
                        'name': 'email', 'index': 3,
                        'type': 'character varying',
                        'comment': None,
                    },
                    {
                        'name': 'ip_address',
                        'index': 4,
                        'type': 'character varying',
                        'comment': None
                    },
                    {
                        'name': 'updated_at',
                        'index': 5,
                        'type': 'timestamp without time zone',
                        'comment': None
                    },
                ],
            },
            'seed_config_expected_1':
                {
                    'metadata': {
                    'schema': my_schema_name,
                    'name': 'seed_config_expected_1',
                    'type': 'BASE TABLE',
                    'comment': None,
                },
               'columns': [
                    {
                        'name': 'id',
                        'index': 1,
                        'type': 'integer',
                        'comment': None,
                    },
                    {
                        'name': 'first_name',
                        'index': 2,
                        'type': 'character varying',
                        'comment': None,
                    },
                    {
                        'name': 'email',
                        'index': 3,
                        'type': 'character varying',
                        'comment': None,
                    },
                    {
                        'name': 'ip_address',
                        'index': 4,
                        'type': 'character varying',
                        'comment': None,
                    },
                    {
                        'name': 'updated_at',
                        'index': 5,
                        'type': 'timestamp without time zone',
                        'comment': None,
                    },
                    {
                        'name': 'c1',
                        'index': 6,
                        'type': 'text',
                        'comment': None,
                    },
                    {
                        'name': 'c2',
                        'index': 7,
                        'type': 'text',
                        'comment': None,
                    },
                    {
                        'name': 'some_bool',
                        'index': 8,
                        'type': 'text',
                        'comment': None,
                    },
                ],
            },
            'seed_summary': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'seed_summary',
                    'type': 'BASE TABLE',
                    'comment': None
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
                        'comment': None,
                    },
                ]
            }
        }

        self.assertEqual(expected, my_schema)
