import json
import os
from datetime import datetime, timedelta

from test.integration.base import DBTIntegrationTest, use_profile

DATEFMT = '%Y-%m-%dT%H:%M:%S.%fZ'

class AnyFloat(object):
    """Any float. Use this in assertEqual() calls to assert that it is a float.
    """
    def __eq__(self, other):
        return isinstance(other, float)


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

    def run_and_generate(self, extra=None, seed_count=1, model_count=1):
        project = {"data-paths": [self.dir("seed")]}
        if extra:
            project.update(extra)
        self.use_default_project(project)

        self.assertEqual(len(self.run_dbt(["seed"])), seed_count)
        self.assertEqual(len(self.run_dbt()), model_count)
        os.remove(os.path.normpath('target/manifest.json'))
        os.remove(os.path.normpath('target/run_results.json'))
        self.generate_start_time = datetime.utcnow()
        self.run_dbt(['docs', 'generate'])

    def assertBetween(self, timestr, start, end=None):
        if end is None:
            end = datetime.utcnow()

        parsed = datetime.strptime(timestr, DATEFMT)

        self.assertLessEqual(start, parsed,
            'parsed date {} happened before {}'.format(
                parsed,
                start.strftime(DATEFMT))
        )
        self.assertGreaterEqual(end, parsed,
            'parsed date {} happened after {}'.format(
                parsed,
                end.strftime(DATEFMT))
        )

    def _expected_catalog(self, id_type, text_type, time_type, view_type,
                          table_type, case=None):
        if case is None:
            case = lambda x: x

        my_schema_name = self.unique_schema()
        expected_cols = {
            case('id'): {
                'name': case('id'),
                'index': 1,
                'type': id_type,
                'comment': None,
            },
            case('first_name'): {
                'name': case('first_name'),
                'index': 2,
                'type': text_type,
                'comment': None,
            },
            case('email'): {
                'name': case('email'),
                'index': 3,
                'type': text_type,
                'comment': None,
            },
            case('ip_address'): {
                'name': case('ip_address'),
                'index': 4,
                'type': text_type,
                'comment': None,
            },
            case('updated_at'): {
                'name': case('updated_at'),
                'index': 5,
                'type': time_type,
                'comment': None,
            },
        }
        return {
            'model.test.model': {
                'unique_id': 'model.test.model',
                'metadata': {
                    'schema': my_schema_name,
                    'name': case('model'),
                    'type': view_type,
                    'comment': None,
                },
                'columns': expected_cols,
            },
            'seed.test.seed': {
                'unique_id': 'seed.test.seed',
                'metadata': {
                    'schema': my_schema_name,
                    'name': case('seed'),
                    'type': table_type,
                    'comment': None,
                },
                'columns': expected_cols,
            },
        }


    def expected_postgres_catalog(self):
        return self._expected_catalog(
            id_type='integer',
            text_type='text',
            time_type='timestamp without time zone',
            view_type='VIEW',
            table_type='BASE TABLE'
        )

    def expected_postgres_references_catalog(self):
        my_schema_name = self.unique_schema()

        summary_columns = {
            'first_name': {
                'name': 'first_name',
                'index': 1,
                'type': 'text',
                'comment': None,
            },
            'ct': {
                'name': 'ct',
                'index': 2,
                'type': 'bigint',
                'comment': None,
            },
        }
        return {
            'seed.test.seed': {
                'unique_id': 'seed.test.seed',
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'seed',
                    'type': 'BASE TABLE',
                    'comment': None,
                },
                'columns': {
                    'id': {
                        'name': 'id',
                        'index': 1,
                        'type': 'integer',
                        'comment': None,
                    },
                    'first_name': {
                        'name': 'first_name',
                        'index': 2,
                        'type': 'text',
                        'comment': None,
                    },
                    'email': {
                        'name': 'email',
                        'index': 3,
                        'type': 'text',
                        'comment': None,
                    },
                    'ip_address': {
                        'name': 'ip_address',
                        'index': 4,
                        'type': 'text',
                        'comment': None,
                    },
                    'updated_at': {
                        'name': 'updated_at',
                        'index': 5,
                        'type': 'timestamp without time zone',
                        'comment': None,
                    },
                },
            },
            'model.test.ephemeral_summary': {
                'unique_id': 'model.test.ephemeral_summary',
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'ephemeral_summary',
                    'type': 'BASE TABLE',
                    'comment': None,
                },
                'columns': summary_columns,
            },
            'model.test.view_summary': {
                'unique_id': 'model.test.view_summary',
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'view_summary',
                    'type': 'VIEW',
                    'comment': None,
                },
                'columns': summary_columns,
            },
        }

    def expected_snowflake_catalog(self):
        return self._expected_catalog(
            id_type='NUMBER',
            text_type='TEXT',
            time_type='TIMESTAMP_NTZ',
            view_type='VIEW',
            table_type='BASE TABLE',
            case=lambda x: x.upper())

    def expected_bigquery_catalog(self):
        return self._expected_catalog(
            id_type='INT64',
            text_type='STRING',
            time_type='DATETIME',
            view_type='view',
            table_type='table'
        )

    def expected_bigquery_nested_catalog(self):
        my_schema_name = self.unique_schema()
        expected_cols = {
            'field_1': {
                "name": "field_1",
                "index": 1,
                "type": "INT64",
                "comment": None
            },
            'field_2': {
                "name": "field_2",
                "index": 2,
                "type": "INT64",
                "comment": None
            },
            'field_3': {
                "name": "field_3",
                "index": 3,
                "type": "INT64",
                "comment": None
            },
            'nested_field.field_4': {
                "name": "nested_field.field_4",
                "index": 4,
                "type": "INT64",
                "comment": None
            },
            'nested_field.field_5': {
                "name": "nested_field.field_5",
                "index": 5,
                "type": "INT64",
                "comment": None
            }
        }
        return {
            "model.test.model": {
                'unique_id': 'model.test.model',
                "metadata": {
                    "schema": my_schema_name,
                    "name": "model",
                    "type": "view",
                    "comment": None
                },
                "columns": expected_cols
            },
            "model.test.seed": {
                'unique_id': 'model.test.seed',
                "metadata": {
                "schema": my_schema_name,
                "name": "seed",
                "type": "view",
                "comment": None
                },
                "columns": expected_cols
            }
        }

    def expected_redshift_catalog(self):
        return self._expected_catalog(
            id_type='integer',
            text_type='character varying',
            time_type='timestamp without time zone',
            view_type='VIEW',
            table_type='BASE TABLE'
        )

    def expected_redshift_incremental_catalog(self):
        my_schema_name = self.unique_schema()
        return {
            'model.test.model': {
                'unique_id': 'model.test.model',
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'model',
                    'type': 'LATE BINDING VIEW',
                    'comment': None,
                },
                'columns': {
                    'id': {
                        'name': 'id',
                        'index': 1,
                        'type': 'integer',
                        'comment': None,
                    },
                    'first_name': {
                        'name': 'first_name',
                        'index': 2,
                        'type': 'character varying(5)',
                        'comment': None,
                    },
                    'email': {
                        'name': 'email',
                        'index': 3,
                        'type': 'character varying(23)',
                        'comment': None,
                    },
                    'ip_address': {
                        'name': 'ip_address',
                        'index': 4,
                        'type': 'character varying(14)',
                        'comment': None,
                    },
                    'updated_at': {
                        'name': 'updated_at',
                        'index': 5,
                        'type': 'timestamp without time zone',
                        'comment': None,
                    },
                },
            },
            'seed.test.seed': {
                'unique_id': 'seed.test.seed',
                'metadata': {
                    'schema': my_schema_name,
                    'name': 'seed',
                    'type': 'BASE TABLE',
                    'comment': None,
                },
                'columns': {
                    'id': {
                        'name': 'id',
                        'index': 1,
                        'type': 'integer',
                        'comment': None,
                    },
                    'first_name': {
                        'name': 'first_name',
                        'index': 2,
                        'type': 'character varying',
                        'comment': None,
                    },
                    'email': {
                        'name': 'email',
                        'index': 3,
                        'type': 'character varying',
                        'comment': None,
                    },
                    'ip_address': {
                        'name': 'ip_address',
                        'index': 4,
                        'type': 'character varying',
                        'comment': None,
                    },
                    'updated_at': {
                        'name': 'updated_at',
                        'index': 5,
                        'type': 'timestamp without time zone',
                        'comment': None,
                    },
                },
            },
        }

    def verify_catalog(self, expected):
        self.assertTrue(os.path.exists('./target/catalog.json'))

        with open('./target/catalog.json') as fp:
            catalog = json.load(fp)

        self.assertIn('generated_at', catalog)
        self.assertBetween(
            catalog.pop('generated_at'),
            start=self.generate_start_time,
        )
        actual = catalog['nodes']
        self.assertEqual(expected, actual)

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
                    'config': {
                        'enabled': True,
                        'materialized': 'view',
                        'pre-hook': [],
                        'post-hook': [],
                        'vars': {},
                        'column_types': {},
                        'quoting': {}
                    },
                    'schema': my_schema_name,
                    'alias': 'model',
                    'description': 'The test model',
                    'columns': {
                        'id': {
                            'name': 'id',
                            'description': 'The user ID number',
                        },
                        'first_name': {
                            'name': 'first_name',
                            'description': "The user's first name",
                        },
                        'email': {
                            'name': 'email',
                            'description': "The user's email",
                        },
                        'ip_address': {
                            'name': 'ip_address',
                            'description': "The user's IP address",
                        },
                        'updated_at': {
                            'name': 'updated_at',
                            'description': "The last time this user's email was updated",
                        },
                    },
                    'patch_path': self.dir('models/schema.yml'),
                    'docrefs': [],
                },
                'seed.test.seed': {
                    'path': 'seed.csv',
                    'name': 'seed',
                    'root_path': os.getcwd(),
                    'resource_type': 'seed',
                    'raw_sql': '-- csv --',
                    'package_name': 'test',
                    'original_file_path': self.dir(os.path.join('seed',
                                                                'seed.csv')),
                    'refs': [],
                    'depends_on': {'nodes': [], 'macros': []},
                    'unique_id': 'seed.test.seed',
                    'empty': False,
                    'fqn': ['test', 'seed'],
                    'tags': [],
                    'config': {
                        'enabled': True,
                        'materialized': 'seed',
                        'pre-hook': [],
                        'post-hook': [],
                        'vars': {},
                        'column_types': {},
                        'quoting': {}
                    },
                    'schema': my_schema_name,
                    'alias': 'seed',
                    'description': '',
                    'columns': {},
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
            'docs': {},
            'metadata': {
                'project_id': '098f6bcd4621d373cade4e832627b4f6',
                'user_id': None,
                'send_anonymous_usage_stats': False,
            },
        }

    def expected_postgres_references_manifest(self):
        my_schema_name = self.unique_schema()
        docs_path = self.dir('ref_models/docs.md')
        docs_file = open(docs_path).read().lstrip()
        return {
            'nodes': {
                'model.test.ephemeral_copy': {
                    'alias': 'ephemeral_copy',
                    'columns': {},
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'ephemeral',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {}
                    },
                    'depends_on': {'macros': [], 'nodes': []},
                    'description': '',
                    'empty': False,
                    'fqn': ['test', 'ephemeral_copy'],
                    'name': 'ephemeral_copy',
                    'original_file_path': self.dir('ref_models/ephemeral_copy.sql'),
                    'package_name': 'test',
                    'path': 'ephemeral_copy.sql',
                    'raw_sql': (
                        '{{\n  config(\n    materialized = "ephemeral"\n  )\n}}'
                        '\n\nselect * from {{ this.schema }}.seed'
                    ),
                    'refs': [],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'tags': [],
                    'unique_id': 'model.test.ephemeral_copy'
                },
                'model.test.ephemeral_summary': {
                    'alias': 'ephemeral_summary',
                    'columns': {
                        'first_name': {
                            'description': 'The first name being summarized',
                            'name': 'first_name'
                        },
                        'ct': {
                            'description': 'The number of instances of the first name',
                            'name': 'ct'
                        },
                    },
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'table',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {}
                    },
                    'depends_on': {
                        'macros': [],
                        'nodes': ['model.test.ephemeral_copy']
                    },
                    'description': 'A summmary table of the ephemeral copy of the seed data',
                    'docrefs': [
                        {
                            'column_name': 'first_name',
                            'documentation_name': 'summary_first_name',
                            'documentation_package': ''
                        },
                        {
                            'column_name': 'ct',
                            'documentation_name': 'summary_count',
                            'documentation_package': ''
                        },
                        {
                            'documentation_name': 'ephemeral_summary',
                            'documentation_package': ''
                        }
                    ],
                    'empty': False,
                    'fqn': ['test',
                    'ephemeral_summary'],
                    'name': 'ephemeral_summary',
                    'original_file_path': self.dir('ref_models/ephemeral_summary.sql'),
                    'package_name': 'test',
                    'patch_path': self.dir('ref_models/schema.yml'),
                    'path': 'ephemeral_summary.sql',
                    'raw_sql': (
                        '{{\n  config(\n    materialized = "table"\n  )\n}}\n\n'
                        'select first_name, count(*) as ct from '
                        "{{ref('ephemeral_copy')}}\ngroup by first_name\n"
                        'order by first_name asc'
                    ),
                    'refs': [['ephemeral_copy']],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'tags': [],
                    'unique_id': 'model.test.ephemeral_summary'},
                'model.test.view_summary': {
                    'alias': 'view_summary',
                    'columns': {
                        'first_name': {
                            'description': 'The first name being summarized',
                            'name': 'first_name'
                        },
                        'ct': {
                            'description': 'The number of instances of the first name',
                            'name': 'ct'
                        },
                    },
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
                        'nodes': ['model.test.ephemeral_summary']
                    },
                    'description': 'A view of the summary of the ephemeral copy of the seed data',
                    'docrefs': [
                        {
                            'column_name': 'first_name',
                            'documentation_name': 'summary_first_name',
                            'documentation_package': ''
                        },
                        {
                            'column_name': 'ct',
                            'documentation_name': 'summary_count',
                            'documentation_package': ''
                        },
                        {
                            'documentation_name': 'view_summary',
                            'documentation_package': ''
                        }
                    ],
                    'empty': False,
                    'fqn': ['test', 'view_summary'],
                    'name': 'view_summary',
                    'original_file_path': self.dir('ref_models/view_summary.sql'),
                    'package_name': 'test',
                    'patch_path': self.dir('ref_models/schema.yml'),
                    'path': 'view_summary.sql',
                    'raw_sql': (
                        '{{\n  config(\n    materialized = "view"\n  )\n}}\n\n'
                        'select first_name, ct from '
                        "{{ref('ephemeral_summary')}}\norder by ct asc"
                    ),
                    'refs': [['ephemeral_summary']],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'tags': [],
                    'unique_id': 'model.test.view_summary'
                },
                'seed.test.seed': {
                    'alias': 'seed',
                    'columns': {},
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'seed',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {}
                    },
                    'depends_on': {'macros': [], 'nodes': []},
                    'description': '',
                    'empty': False,
                    'fqn': ['test', 'seed'],
                    'name': 'seed',
                    'original_file_path': self.dir('seed/seed.csv'),
                    'package_name': 'test',
                    'path': 'seed.csv',
                    'raw_sql': '-- csv --',
                    'refs': [],
                    'resource_type': 'seed',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'tags': [],
                    'unique_id': 'seed.test.seed'
                }
            },
            'docs': {
                'test.ephemeral_summary': {
                    'block_contents': (
                        'A summmary table of the ephemeral copy of the seed data'
                    ),
                    'file_contents': docs_file,
                    'name': 'ephemeral_summary',
                    'original_file_path': docs_path,
                    'package_name': 'test',
                    'path': 'docs.md',
                    'resource_type': 'documentation',
                    'root_path': os.getcwd(),
                    'unique_id': 'test.ephemeral_summary'
                },
                'test.summary_count': {
                    'block_contents': 'The number of instances of the first name',
                    'file_contents': docs_file,
                    'name': 'summary_count',
                    'original_file_path': docs_path,
                    'package_name': 'test',
                    'path': 'docs.md',
                    'resource_type': 'documentation',
                    'root_path': os.getcwd(),
                    'unique_id': 'test.summary_count'
                },
                'test.summary_first_name': {
                    'block_contents': 'The first name being summarized',
                    'file_contents': docs_file,
                    'name': 'summary_first_name',
                    'original_file_path': docs_path,
                    'package_name': 'test',
                    'path': 'docs.md',
                    'resource_type': 'documentation',
                    'root_path': os.getcwd(),
                    'unique_id': 'test.summary_first_name'
                },
                'test.view_summary': {
                    'block_contents': (
                        'A view of the summary of the ephemeral copy of the '
                        'seed data'
                    ),
                    'file_contents': docs_file,
                    'name': 'view_summary',
                    'original_file_path': docs_path,
                    'package_name': 'test',
                    'path': 'docs.md',
                    'resource_type': 'documentation',
                    'root_path': os.getcwd(),
                    'unique_id': 'test.view_summary'
                },
            },
            'child_map': {
                'model.test.ephemeral_copy': ['model.test.ephemeral_summary'],
                'model.test.ephemeral_summary': ['model.test.view_summary'],
                'model.test.view_summary': [],
                'seed.test.seed': [],
            },
            'parent_map': {
                'model.test.ephemeral_copy': [],
                'model.test.ephemeral_summary': ['model.test.ephemeral_copy'],
                'model.test.view_summary': ['model.test.ephemeral_summary'],
                'seed.test.seed': [],
            },
            'metadata': {
                'project_id': '098f6bcd4621d373cade4e832627b4f6',
                'user_id': None,
                'send_anonymous_usage_stats': False,
            },
        }

    def expected_bigquery_nested_manifest(self):
        model_sql_path = self.dir('bq_models/model.sql')
        seed_sql_path = self.dir('bq_models/seed.sql')
        my_schema_name = self.unique_schema()
        return {
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
                    'unique_id': 'model.test.model',
                    'columns': {
                        'field_1': {
                            'name': 'field_1',
                            'description': 'The first field',
                        },
                        'field_2': {
                            'name': 'field_2',
                            'description': 'The second field',
                        },
                        'field_3': {
                            'name': 'field_3',
                            'description': 'The third field',
                        },
                        'nested_field.field_4': {
                            'name': 'nested_field.field_4',
                            'description': 'The first nested field',
                        },
                        'nested_field.field_5': {
                            'name': 'nested_field.field_5',
                            'description': 'The second nested field',
                        },
                    },
                    'description': 'The test model',
                    'patch_path': self.dir('bq_models/schema.yml'),
                    'docrefs': [],
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
                    'unique_id': 'model.test.seed',
                    'columns': {},
                    'description': '',
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
            'docs': {},
            'metadata': {
                'project_id': '098f6bcd4621d373cade4e832627b4f6',
                'user_id': None,
                'send_anonymous_usage_stats': False,
            },
        }

    def expected_redshift_incremental_view_manifest(self):
        model_sql_path = self.dir('rs_models/model.sql')
        my_schema_name = self.unique_schema()
        return {
            "nodes": {
                "model.test.model": {
                    "name": "model",
                    "root_path": os.getcwd(),
                    "resource_type": "model",
                    "path": "model.sql",
                    "original_file_path": model_sql_path,
                    "package_name": "test",
                    "raw_sql": open(model_sql_path).read().rstrip('\n'),
                    "refs": [["seed"]],
                    "depends_on": {
                        "nodes": ["seed.test.seed"],
                        "macros": [],
                    },
                    "unique_id": "model.test.model",
                    "empty": False,
                    "fqn": ["test", "model"],
                    "tags": [],
                    "config": {
                        "bind": False,
                        "enabled": True,
                        "materialized": "view",
                        "pre-hook": [],
                        "post-hook": [],
                        "vars": {},
                        "column_types": {},
                        "quoting": {},
                    },
                    "schema": my_schema_name,
                    "alias": "model",
                    'description': 'The test model',
                    'columns': {
                        'id': {
                            'name': 'id',
                            'description': 'The user ID number',
                        },
                        'first_name': {
                            'name': 'first_name',
                            'description': "The user's first name",
                        },
                        'email': {
                            'name': 'email',
                            'description': "The user's email",
                        },
                        'ip_address': {
                            'name': 'ip_address',
                            'description': "The user's IP address",
                        },
                        'updated_at': {
                            'name': 'updated_at',
                            'description': "The last time this user's email was updated",
                        },
                    },
                    'patch_path': self.dir('rs_models/schema.yml'),
                    'docrefs': [],
                },
                "seed.test.seed": {
                    "path": "seed.csv",
                    "name": "seed",
                    "root_path": os.getcwd(),
                    "resource_type": "seed",
                    "raw_sql": "-- csv --",
                    "package_name": "test",
                    "original_file_path": self.dir("seed/seed.csv"),
                    "refs": [],
                    "depends_on": {
                        "nodes": [],
                        "macros": [],
                    },
                    "unique_id": "seed.test.seed",
                    "empty": False,
                    "fqn": ["test", "seed"],
                    "tags": [],
                    "config": {
                        "enabled": True,
                        "materialized": "seed",
                        "pre-hook": [],
                        "post-hook": [],
                        "vars": {},
                        "column_types": {},
                        "quoting": {},
                    },
                    "schema": my_schema_name,
                    "alias": "seed",
                    'columns': {},
                    'description': '',
                },
            },
            "parent_map": {
                "model.test.model": ["seed.test.seed"],
                "seed.test.seed": []
            },
            "child_map": {
                "model.test.model": [],
                "seed.test.seed": ["model.test.model"]
            },
            'docs': {},
            'metadata': {
                'project_id': '098f6bcd4621d373cade4e832627b4f6',
                'user_id': None,
                'send_anonymous_usage_stats': False,
            },
        }

    def verify_manifest(self, expected_manifest):
        self.assertTrue(os.path.exists('./target/manifest.json'))

        with open('./target/manifest.json') as fp:
            manifest = json.load(fp)

        self.assertEqual(
            set(manifest),
            {'nodes', 'macros', 'parent_map', 'child_map', 'generated_at',
             'docs', 'metadata'}
        )

        self.verify_manifest_macros(manifest)
        manifest_without_extras = {
            k: v for k, v in manifest.items()
            if k not in {'macros', 'generated_at'}
        }
        self.assertBetween(
            manifest['generated_at'],
            start=self.generate_start_time
        )
        self.assertEqual(manifest_without_extras, expected_manifest)

    def expected_run_results(self):
        """
        The expected results of this run.
        """
        schema = self.unique_schema()
        compiled_sql = '\n\nselect * from "{}".seed'.format(schema)
        status = 'CREATE VIEW'

        if self.adapter_type == 'snowflake':
            status = 'SUCCESS 1'

        if self.adapter_type == 'bigquery':
            status = 'OK'
            compiled_sql = '\n\nselect * from `{}`.`{}`.seed'.format(
                self._profile['project'], schema
            )
        status = None

        return [
            {
                'error': None,
                'execution_time': AnyFloat(),
                'fail': None,
                'node': {
                    'alias': 'model',
                    'build_path': os.path.normpath(
                        'target/compiled/test/model.sql'
                    ),
                    'columns': {
                        'id': {'description': 'The user ID number', 'name': 'id'},
                        'first_name': {'description': "The user's first name", 'name': 'first_name'},
                        'email': {'description': "The user's email", 'name': 'email'},
                        'ip_address': {'description': "The user's IP address", 'name': 'ip_address'},
                        'updated_at': {'description': "The last time this user's email was updated", 'name': 'updated_at'}
                    },
                    'compiled': True,
                    'compiled_sql': compiled_sql,
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
                        'nodes': ['seed.test.seed']
                    },
                    'description': 'The test model',
                    'docrefs': [],
                    'empty': False,
                    'extra_ctes': [],
                    'extra_ctes_injected': True,
                    'fqn': ['test', 'model'],
                    'injected_sql': compiled_sql,
                    'name': 'model',
                    'original_file_path': self.dir('models/model.sql'),
                    'package_name': 'test',
                    'patch_path': self.dir('models/schema.yml'),
                    'path': 'model.sql',
                    'raw_sql': "{{\n    config(\n        materialized='view'\n    )\n}}\n\nselect * from {{ ref('seed') }}",
                    'refs': [['seed']],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': schema,
                    'tags': [],
                    'unique_id': 'model.test.model',
                    'wrapped_sql': 'None'
                },
                'skip': False,
                'status': status,
            },
            {
                'error': None,
                'execution_time': AnyFloat(),
                'fail': None,
                'node': {
                    'alias': 'seed',
                    'build_path': os.path.normpath(
                        'target/compiled/test/seed.csv'
                    ),
                    'columns': {},
                    'compiled': True,
                    'compiled_sql': '-- csv --',
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'seed',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {},
                    },
                    'depends_on': {'macros': [], 'nodes': []},
                    'description': '',
                    'empty': False,
                    'extra_ctes': [],
                    'extra_ctes_injected': True,
                    'fqn': ['test', 'seed'],
                    'injected_sql': '-- csv --',
                    'name': 'seed',
                    'original_file_path': self.dir('seed/seed.csv'),
                    'package_name': 'test',
                    'path': 'seed.csv',
                    'raw_sql': '-- csv --',
                    'refs': [],
                    'resource_type': 'seed',
                    'root_path': os.getcwd(),
                    'schema': schema,
                    'tags': [],
                    'unique_id': 'seed.test.seed',
                    'wrapped_sql': 'None'
                },
                'skip': False,
                'status': None,
            },
        ]

    def expected_postgres_references_run_results(self):
        my_schema_name = self.unique_schema()
        ephemeral_compiled_sql = (
            '\n\nselect first_name, count(*) as ct from '
            '__dbt__CTE__ephemeral_copy\ngroup by first_name\n'
            'order by first_name asc'
        )
        cte_sql = (
            ' __dbt__CTE__ephemeral_copy as (\n\n\nselect * from {}.seed\n)'
        ).format(my_schema_name)

        ephemeral_injected_sql = (
            '\n\nwith{}select first_name, count(*) as ct from '
            '__dbt__CTE__ephemeral_copy\ngroup by first_name\n'
            'order by first_name asc'
        ).format(cte_sql)

        view_compiled_sql = (
            '\n\nselect first_name, ct from "{}".ephemeral_summary\n'
            'order by ct asc'
        ).format(my_schema_name)

        return [
            {
                'error': None,
                'execution_time': AnyFloat(),
                'fail': None,
                'node': {
                    'alias': 'ephemeral_summary',
                    'build_path': os.path.normpath(
                        'target/compiled/test/ephemeral_summary.sql'
                    ),
                    'columns': {
                        'first_name': {
                            'description': 'The first name being summarized',
                            'name': 'first_name'
                        },
                        'ct': {
                            'description': 'The number of instances of the first name',
                            'name': 'ct'
                        },
                    },
                    'compiled': True,
                    'compiled_sql': ephemeral_compiled_sql,
                    'config': {
                        'enabled': True,
                        'materialized': 'table',
                        'pre-hook': [],
                        'post-hook': [],
                        'vars': {},
                        'column_types': {},
                        'quoting': {}
                    },
                    'depends_on': {
                        'nodes': ['model.test.ephemeral_copy'],
                        'macros': []
                    },
                    'description': (
                        'A summmary table of the ephemeral copy of the seed data'
                    ),
                    'docrefs': [
                        {
                            'column_name': 'first_name',
                            'documentation_name': 'summary_first_name',
                            'documentation_package': ''
                        },
                        {
                            'column_name': 'ct',
                            'documentation_name': 'summary_count',
                            'documentation_package': ''
                        },
                        {
                            'documentation_name': 'ephemeral_summary',
                            'documentation_package': ''
                        }
                    ],
                    'empty': False,
                    'extra_ctes': [
                        {'id': 'model.test.ephemeral_copy', 'sql': cte_sql},
                    ],
                    'extra_ctes_injected': True,
                    'fqn': ['test', 'ephemeral_summary'],
                    'injected_sql': ephemeral_injected_sql,
                    'name': 'ephemeral_summary',
                    'original_file_path': self.dir('ref_models/ephemeral_summary.sql'),
                    'package_name': 'test',
                    'patch_path': self.dir('ref_models/schema.yml'),
                    'path': 'ephemeral_summary.sql',
                    'raw_sql': (
                        '{{\n  config(\n    materialized = "table"\n  )\n}}\n'
                        '\nselect first_name, count(*) as ct from '
                        "{{ref('ephemeral_copy')}}\ngroup by first_name\n"
                        'order by first_name asc'
                    ),
                    'refs': [['ephemeral_copy']],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'tags': [],
                    'unique_id': 'model.test.ephemeral_summary',
                    'wrapped_sql': 'None',
                },
                'skip': False,
                'status': None,
            },
            {
                'error': None,
                'execution_time': AnyFloat(),
                'fail': None,
                'node': {
                    'alias': 'view_summary',
                    'build_path': os.path.normpath(
                        'target/compiled/test/view_summary.sql'
                    ),
                    'alias': 'view_summary',
                    'columns': {
                        'first_name': {
                            'description': 'The first name being summarized',
                            'name': 'first_name'
                        },
                        'ct': {
                            'description': 'The number of instances of the first name',
                            'name': 'ct'
                        },
                    },
                    'compiled': True,
                    'compiled_sql': view_compiled_sql,
                    'config': {
                        'enabled': True,
                        'materialized': 'view',
                        'pre-hook': [],
                        'post-hook': [],
                        'vars': {},
                        'column_types': {},
                        'quoting': {}
                    },
                    'depends_on': {
                        'nodes': ['model.test.ephemeral_summary'],
                        'macros': []
                    },
                    'description': (
                        'A view of the summary of the ephemeral copy of the '
                        'seed data'
                    ),
                    'docrefs': [
                        {
                            'column_name': 'first_name',
                            'documentation_name': 'summary_first_name',
                            'documentation_package': ''
                        },
                        {
                            'column_name': 'ct',
                            'documentation_name': 'summary_count',
                            'documentation_package': ''
                        },
                        {
                            'documentation_name': 'view_summary',
                            'documentation_package': ''
                        }
                    ],
                    'empty': False,
                    'extra_ctes': [],
                    'extra_ctes_injected': True,
                    'fqn': ['test', 'view_summary'],
                    'injected_sql': view_compiled_sql,
                    'name': 'view_summary',
                    'original_file_path': self.dir('ref_models/view_summary.sql'),
                    'package_name': 'test',
                    'patch_path': self.dir('ref_models/schema.yml'),
                    'path': 'view_summary.sql',
                    'raw_sql': (
                        '{{\n  config(\n    materialized = "view"\n  )\n}}\n\n'
                        'select first_name, ct from '
                        "{{ref('ephemeral_summary')}}\norder by ct asc"
                    ),
                    'refs': [['ephemeral_summary']],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'tags': [],
                    'unique_id': 'model.test.view_summary',
                    'wrapped_sql': 'None',
                },
                'skip': False,
                'status': None,
            },
            {
                'error': None,
                'execution_time': AnyFloat(),
                'fail': None,
                'node': {
                    'alias': 'seed',
                    'build_path': os.path.normpath(
                        'target/compiled/test/seed.csv'
                    ),
                    'columns': {},
                    'compiled': True,
                    'compiled_sql': '-- csv --',
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'seed',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': {},
                    },
                    'depends_on': {'macros': [], 'nodes': []},
                    'description': '',
                    'empty': False,
                    'extra_ctes': [],
                    'extra_ctes_injected': True,
                    'fqn': ['test', 'seed'],
                    'injected_sql': '-- csv --',
                    'name': 'seed',
                    'original_file_path': self.dir('seed/seed.csv'),
                    'package_name': 'test',
                    'path': 'seed.csv',
                    'raw_sql': '-- csv --',
                    'refs': [],
                    'resource_type': 'seed',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'tags': [],
                    'unique_id': 'seed.test.seed',
                    'wrapped_sql': 'None'
                },
                'skip': False,
                'status': None,
            },
        ]

    def verify_run_results(self, expected_run_results):
        with open('./target/run_results.json') as fp:
            run_result = json.load(fp)

        self.assertIn('generated_at', run_result)
        self.assertIn('elapsed_time', run_result)
        self.assertBetween(
            run_result['generated_at'],
            start=self.generate_start_time
        )
        self.assertGreater(run_result['elapsed_time'], 0)
        self.assertTrue(
            isinstance(run_result['elapsed_time'], float),
            "run_result['elapsed_time'] is of type {}, expected float".format(
                str(type(run_result['elapsed_time'])))
        )
        # sort the results so we can make reasonable assertions
        run_result['results'].sort(key=lambda r: r['node']['unique_id'])
        self.assertEqual(run_result['results'], expected_run_results)

    @use_profile('postgres')
    def test__postgres__run_and_generate(self):
        self.run_and_generate()
        self.verify_catalog(self.expected_postgres_catalog())
        self.verify_manifest(self.expected_seeded_manifest())
        self.verify_run_results(self.expected_run_results())

    @use_profile('postgres')
    def test__postgres_references(self):
        self.run_and_generate(
            {'source-paths': [self.dir('ref_models')]},
            model_count=2
        )

        self.verify_catalog(self.expected_postgres_references_catalog())
        self.verify_manifest(self.expected_postgres_references_manifest())
        self.verify_run_results(self.expected_postgres_references_run_results())

    @use_profile('snowflake')
    def test__snowflake__run_and_generate(self):
        self.run_and_generate()

        self.verify_catalog(self.expected_snowflake_catalog())
        self.verify_manifest(self.expected_seeded_manifest())
        self.verify_run_results(self.expected_run_results())

    @use_profile('bigquery')
    def test__bigquery__run_and_generate(self):
        self.run_and_generate()

        self.verify_catalog(self.expected_bigquery_catalog())
        self.verify_manifest(self.expected_seeded_manifest())
        self.verify_run_results(self.expected_run_results())

    @use_profile('bigquery')
    def test__bigquery__nested_models(self):
        self.use_default_project({'source-paths': [self.dir('bq_models')]})

        self.assertEqual(len(self.run_dbt()), 2)
        os.remove(os.path.normpath('target/manifest.json'))
        os.remove(os.path.normpath('target/run_results.json'))
        self.generate_start_time = datetime.utcnow()
        self.run_dbt(['docs', 'generate'])

        self.verify_catalog(self.expected_bigquery_nested_catalog())
        self.verify_manifest(self.expected_bigquery_nested_manifest())

    @use_profile('redshift')
    def test__redshift__run_and_generate(self):
        self.run_and_generate()
        self.verify_catalog(self.expected_redshift_catalog())
        self.verify_manifest(self.expected_seeded_manifest())
        self.verify_run_results(self.expected_run_results())

    @use_profile('redshift')
    def test__redshift__incremental_view(self):
        self.run_and_generate({'source-paths': [self.dir('rs_models')]})
        self.verify_catalog(self.expected_redshift_incremental_catalog())
        self.verify_manifest(self.expected_redshift_incremental_view_manifest())
