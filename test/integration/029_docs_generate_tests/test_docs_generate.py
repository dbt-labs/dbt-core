import json
import os

from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


class TestDocsGenerate(DBTIntegrationTest):

    @property
    def schema(self):
        return 'simple_dependency_029'

    @staticmethod
    def dir(path):
        return "test/integration/029_docs_generate_tests/" + path.lstrip("/")

    @property
    def models(self):
        return self.dir("models")

    @property
    def project_config(self):
        return {
            'repositories': [
                'https://github.com/fishtown-analytics/dbt-integration-project'
            ],
            'quoting': {
                'identifier': False
            }
        }

    @attr(type='postgres')
    def test__postgres__simple_generate(self):
        self.use_profile('postgres')
        self.use_default_project({"data-paths": [self.dir("seed")]})

        self.assertEqual(len(self.run_dbt(["seed"])), 1)
        self.assertEqual(len(self.run_dbt()), 1)
        self.run_dbt(['docs', 'generate'])
        self.assertTrue(os.path.exists('./target/catalog.json'))

        with open('./target/catalog.json') as fp:
            data = json.load(fp)

        my_schema_name = self.unique_schema()
        self.assertIn(my_schema_name, data)
        my_schema = data[my_schema_name]
        expected = {
            'model': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'model',
                    'type': 'VIEW',
                    'comment': None,
                },
                'columns': [
                    {
                        'name': 'id',
                        'index': 1,
                        'type': 'integer',
                        'comment': None,
                    },
                ],
            },
            'seed': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'seed',
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
                        'type': 'text',
                        'comment': None,
                    },
                    {
                        'name': 'email',
                        'index': 3,
                        'type': 'text',
                        'comment': None,
                    },
                    {
                        'name': 'ip_address',
                        'index': 4,
                        'type': 'text',
                        'comment': None,
                    },
                    {
                        'name': 'updated_at',
                        'index': 5,
                        'type': 'timestamp without time zone',
                        'comment': None,
                    },
                ],
            },
        }

        self.assertEqual(expected, my_schema)

    @attr(type='snowflake')
    def test__snowflake__simple_generate(self):
        self.use_profile('snowflake')
        self.use_default_project({"data-paths": [self.dir("seed")]})

        self.assertEqual(len(self.run_dbt(["seed"])), 1)
        self.assertEqual(len(self.run_dbt()), 1)
        self.run_dbt(['docs', 'generate'])
        self.assertTrue(os.path.exists('./target/catalog.json'))

        with open('./target/catalog.json') as fp:
            data = json.load(fp)

        my_schema_name = self.unique_schema()
        self.assertIn(my_schema_name, data)
        my_schema = data[my_schema_name]
        expected = {
            'MODEL': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'MODEL',
                    'type': 'VIEW',
                    'comment': None,
                },
                'columns': [
                    {
                        'name': 'ID',
                        'index': 1,
                        'type': 'NUMBER',
                        'comment': None,
                    },
                ],
            },
            'SEED': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'SEED',
                    'type': 'BASE TABLE',
                    'comment': None,
                },
                'columns': [
                    {
                        'name': 'ID',
                        'index': 1,
                        'type': 'NUMBER',
                        'comment': None,
                    },
                    {
                        'name': 'FIRST_NAME',
                        'index': 2,
                        'type': 'TEXT',
                        'comment': None,
                    },
                    {
                        'name': 'EMAIL',
                        'index': 3,
                        'type': 'TEXT',
                        'comment': None,
                    },
                    {
                        'name': 'IP_ADDRESS',
                        'index': 4,
                        'type': 'TEXT',
                        'comment': None,
                    },
                    {
                        'name': 'UPDATED_AT',
                        'index': 5,
                        'type': 'TIMESTAMP_NTZ',
                        'comment': None,
                    },
                ],
            },
        }

        self.assertEqual(expected, my_schema)
