import json
import os

from test.integration.base import DBTIntegrationTest, use_profile


class TestDocsGenerate(DBTIntegrationTest):
    def setUp(self):
        super(TestDocsGenerate,self).setUp()
        self.maxDiff = None

    @property
    def schema(self):
        return 'docs_generate_029'

    @staticmethod
    def dir(path):
        return os.path.normpath(
            os.path.join('test/integration/029_docs_generate_tests', path))

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

    def run_and_generate(self):
        self.use_default_project({"data-paths": [self.dir("seed")]})

        self.assertEqual(len(self.run_dbt(["seed"])), 1)
        self.assertEqual(len(self.run_dbt()), 1)
        self.run_dbt(['docs', 'generate'])

    def verify_catalog(self, expected):
        self.assertTrue(os.path.exists('./target/catalog.json'))

        with open('./target/catalog.json') as fp:
            catalog = json.load(fp)

        my_schema_name = self.unique_schema()
        self.assertIn(my_schema_name, catalog)
        my_schema = catalog[my_schema_name]
        self.assertEqual(expected, my_schema)

    def verify_manifest_macros(self, manifest):
        # just test a known global macro to avoid having to update this every
        # time they change.
        self.assertIn('macro.dbt.column_list', manifest['macros'])
        macro = manifest['macros']['macro.dbt.column_list']
        self.assertEqual(
            set(macro),
            {
                'path', 'original_file_path', 'package_name', 'raw_sql',
                'root_path', 'name', 'unique_id', 'tags', 'resource_type',
                'depends_on'
            }
        )
        # Don't compare the sql, just make sure it exists
        self.assertTrue(len(macro['raw_sql']) > 10)
        without_sql = {k: v for k, v in macro.items() if k != 'raw_sql'}
        # Windows means we can't hard-code this.
        helpers_path = os.path.join('materializations', 'helpers.sql')
        self.assertEqual(
            without_sql,
            {
                'path': helpers_path,
                'original_file_path': helpers_path,
                'package_name': 'dbt',
                'root_path': os.path.join(os.getcwd(), 'dbt','include',
                                          'global_project'),
                'name': 'column_list',
                'unique_id': 'macro.dbt.column_list',
                'tags': [],
                'resource_type': 'macro',
                'depends_on': {'macros': []}
            }
        )

    def expected_seeded_manifest(self):
        # the manifest should be consistent across DBs for this test
        model_sql_path = self.dir('models/model.sql')
        my_schema_name = self.unique_schema()
        return {
            'nodes': {
                'model.test.model': {
                    'name': 'model',
                    'root_path': os.getcwd(),
                    'resource_type': 'model',
                    'path': 'model.sql',
                    'original_file_path': model_sql_path,
                    'package_name': 'test',
                    'raw_sql': open(model_sql_path).read().rstrip('\n'),
                    'refs': [['seed']],
                    'depends_on': {'nodes': ['seed.test.seed'], 'macros': []},
                    'unique_id': 'model.test.model',
                    'empty': False,
                    'fqn': ['test', 'model'],
                    'tags': [],
                    'config': {'enabled': True,
                    'materialized': 'view',
                    'pre-hook': [],
                    'post-hook': [],
                    'vars': {},
                    'column_types': {},
                    'quoting': {}},
                    'schema': my_schema_name,
                    'alias': 'model'
                },
                'seed.test.seed': {
                    'path': 'seed.csv',
                    'name': 'seed',
                    'root_path': os.getcwd(),
                    'resource_type': 'seed',
                    'raw_sql': '-- csv --',
                    'package_name': 'test',
                    'original_file_path': self.dir('seed/seed.csv'),
                    'refs': [],
                    'depends_on': {'nodes': [], 'macros': []},
                    'unique_id': 'seed.test.seed',
                    'empty': False,
                    'fqn': ['test', 'seed'],
                    'tags': [],
                    'config': {'enabled': True,
                    'materialized': 'seed',
                    'pre-hook': [],
                    'post-hook': [],
                    'vars': {},
                    'column_types': {},
                    'quoting': {}},
                    'schema': my_schema_name,
                    'alias': 'seed'
                },
            },
            'parent_map': {
                'model.test.model': ['seed.test.seed'],
                'seed.test.seed': [],
            },
            'child_map': {
                'model.test.model': [],
                'seed.test.seed': ['model.test.model'],
            },
        }

    def verify_manifest(self, expected_manifest):
        self.assertTrue(os.path.exists('./target/manifest.json'))

        with open('./target/manifest.json') as fp:
            manifest = json.load(fp)

        self.assertEqual(
            set(manifest),
            {'nodes', 'macros', 'parent_map', 'child_map'}
        )

        self.verify_manifest_macros(manifest)
        manifest_without_macros = {
            k: v for k, v in manifest.items() if k != 'macros'
        }
        self.assertEqual(manifest_without_macros, expected_manifest)

    @use_profile('postgres')
    def test__postgres__run_and_generate(self):
        self.run_and_generate()
        my_schema_name = self.unique_schema()
        expected_cols = [
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
        ]
        expected_catalog = {
            'model': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'model',
                    'type': 'VIEW',
                    'comment': None,
                },
                'columns': expected_cols,
            },
            'seed': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'seed',
                    'type': 'BASE TABLE',
                    'comment': None,
                },
                'columns': expected_cols,
            },
        }
        self.verify_catalog(expected_catalog)
        self.verify_manifest(self.expected_seeded_manifest())

    @use_profile('snowflake')
    def test__snowflake__run_and_generate(self):
        self.run_and_generate()
        my_schema_name = self.unique_schema()
        expected_cols = [
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
        ]
        expected_catalog = {
            'MODEL': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'MODEL',
                    'type': 'VIEW',
                    'comment': None,
                },
                'columns': expected_cols,
            },
            'SEED': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'SEED',
                    'type': 'BASE TABLE',
                    'comment': None,
                },
                'columns': expected_cols,
            },
        }

        self.verify_catalog(expected_catalog)
        self.verify_manifest(self.expected_seeded_manifest())

    @use_profile('bigquery')
    def test__bigquery__run_and_generate(self):
        self.run_and_generate()
        my_schema_name = self.unique_schema()
        expected_cols = [
            {
                'name': 'id',
                'index': 1,
                'type': 'INTEGER',
                'comment': None,
            },
            {
                'name': 'first_name',
                'index': 2,
                'type': 'STRING',
                'comment': None,
            },
            {
                'name': 'email',
                'index': 3,
                'type': 'STRING',
                'comment': None,
            },
            {
                'name': 'ip_address',
                'index': 4,
                'type': 'STRING',
                'comment': None,
            },
            {
                'name': 'updated_at',
                'index': 5,
                'type': 'DATETIME',
                'comment': None,
            },
        ]
        expected_catalog = {
            'model': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'model',
                    'type': 'view',
                    'comment': None,
                },
                'columns': expected_cols,
            },
            'seed': {
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'seed',
                    'type': 'table',
                    'comment': None,
                },
                'columns': expected_cols,
            },
        }
        self.verify_catalog(expected_catalog)
        self.verify_manifest(self.expected_seeded_manifest())

    @use_profile('bigquery')
    def test__bigquery__nested_models(self):
        self.use_default_project({'source-paths': [self.dir('bq_models')]})

        self.assertEqual(len(self.run_dbt()), 2)
        self.run_dbt(['docs', 'generate'])

        my_schema_name = self.unique_schema()
        expected_cols = [
            {
                "name": "field_1",
                "index": 1,
                "type": "INTEGER",
                "comment": None
            },
            {
                "name": "field_2",
                "index": 2,
                "type": "INTEGER",
                "comment": None
            },
            {
                "name": "field_3",
                "index": 3,
                "type": "INTEGER",
                "comment": None
            },
            {
                "name": "nested_field.field_4",
                "index": 4,
                "type": "INTEGER",
                "comment": None
            },
            {
                "name": "nested_field.field_5",
                "index": 5,
                "type": "INTEGER",
                "comment": None
            }
        ]
        catalog = {
            "model": {
                "metadata": {
                    "schema": my_schema_name,
                    "name": "model",
                    "type": "view",
                    "comment": None
                },
                "columns": expected_cols
            },
            "seed": {
                "metadata": {
                "schema": my_schema_name,
                "name": "seed",
                "type": "view",
                "comment": None
                },
                "columns": expected_cols
            }
        }
        self.verify_catalog(catalog)
        model_sql_path = self.dir('bq_models/model.sql')
        seed_sql_path = self.dir('bq_models/seed.sql')
        expected_manifest = {
            'nodes': {
               'model.test.model': {
                    'alias': 'model',
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {}
                    },
                    'depends_on': {
                        'macros': [],
                        'nodes': ['model.test.seed']
                    },
                    'empty': False,
                    'fqn': ['test', 'model'],
                    'name': 'model',
                    'original_file_path': model_sql_path,
                    'package_name': 'test',
                    'path': 'model.sql',
                    'raw_sql': open(model_sql_path).read().rstrip('\n'),
                    'refs': [['seed']],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'tags': [],
                    'unique_id': 'model.test.model'
                },
                'model.test.seed': {
                    'alias': 'seed',
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {}
                    },
                    'depends_on': {
                        'macros': [],
                        'nodes': []
                    },
                    'empty': False,
                    'fqn': ['test', 'seed'],
                    'name': 'seed',
                    'original_file_path': seed_sql_path,
                    'package_name': 'test',
                    'path': 'seed.sql',
                    'raw_sql': open(seed_sql_path).read().rstrip('\n'),
                    'refs': [],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'tags': [],
                    'unique_id': 'model.test.seed'
                }
            },
            'child_map': {
               'model.test.model': [],
                'model.test.seed': ['model.test.model']
            },
            'parent_map': {
                'model.test.model': ['model.test.seed'],
                'model.test.seed': []
            },
        }
        self.verify_manifest(expected_manifest)
