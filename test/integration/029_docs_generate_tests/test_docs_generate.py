from __future__ import unicode_literals
import json
from numbers import Integral
import os
from datetime import datetime, timedelta
from mock import ANY, patch

from test.integration.base import DBTIntegrationTest, use_profile
from dbt.compat import basestring

DATEFMT = '%Y-%m-%dT%H:%M:%S.%fZ'

class AnyFloat(object):
    """Any float. Use this in assertEqual() calls to assert that it is a float.
    """
    def __eq__(self, other):
        return isinstance(other, float)


class AnyStringWith(object):
    def __init__(self, contains):
        self.contains = contains

    def __eq__(self, other):
        return isinstance(other, basestring) and self.contains in other


def _read_file(path):
    with open(path) as fp:
        return fp.read()


def _normalize(path):
    """On windows, neither is enough on its own:

    >>> normcase('C:\\documents/ALL CAPS/subdir\\..')
    'c:\\documents\\all caps\\subdir\\..'
    >>> normpath('C:\\documents/ALL CAPS/subdir\\..')
    'C:\\documents\\ALL CAPS'
    >>> normpath(normcase('C:\\documents/ALL CAPS/subdir\\..'))
    'c:\\documents\\all caps'
    """
    return os.path.normcase(os.path.normpath(path))


class TestDocsGenerate(DBTIntegrationTest):
    setup_alternate_db = True
    def setUp(self):
        super(TestDocsGenerate,self).setUp()
        self.maxDiff = None

    @property
    def schema(self):
        return 'docs_generate_029'

    @staticmethod
    def dir(path):
        return _normalize(
            os.path.join('test/integration/029_docs_generate_tests', path)
        )

    @property
    def models(self):
        return self.dir("models")

    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    'git': 'https://github.com/fishtown-analytics/dbt-integration-project',
                },
            ],
        }

    @property
    def project_config(self):
        return {
            'quoting': {
                'identifier': False
            }
        }

    def run_and_generate(self, extra=None, seed_count=1, model_count=1, alternate_db=None):
        if alternate_db is None:
            alternate_db = self.alternative_database
        project = {
            "data-paths": [self.dir("seed")],
            'macro-paths': [self.dir('macros')],
            'models': {
                'vars': {'alternate_db': alternate_db},
            },
        }
        if extra:
            project.update(extra)
        self.use_default_project(project)

        self.assertEqual(len(self.run_dbt(["seed"])), seed_count)
        self.assertEqual(len(self.run_dbt()), model_count)
        os.remove(_normalize('target/manifest.json'))
        os.remove(_normalize('target/run_results.json'))
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

    def _no_stats(self):
        return {
            'has_stats': {
                'id': 'has_stats',
                'label': 'Has Stats?',
                'value': False,
                'description': 'Indicates whether there are statistics for this table',
                'include': False,
            },
        }

    def _redshift_stats(self):
        return {
            "has_stats": {
              "id": "has_stats",
              "label": "Has Stats?",
              "value": True,
              "description": "Indicates whether there are statistics for this table",
              "include": False
            },
            "encoded": {
              "id": "encoded",
              "label": "Encoded",
              "value": "Y",
              "description": "Indicates whether any column in the table has compression encoding defined.",
              "include": True
            },
            "diststyle": {
              "id": "diststyle",
              "label": "Dist Style",
              "value": "EVEN",
              "description": "Distribution style or distribution key column, if key distribution is defined.",
              "include": True
            },
            "max_varchar": {
              "id": "max_varchar",
              "label": "Max Varchar",
              "value": AnyFloat(),
              "description": "Size of the largest column that uses a VARCHAR data type.",
              "include": True
            },
            "size": {
              "id": "size",
              "label": "Approximate Size",
              "value": AnyFloat(),
              "description": "Approximate size of the table, calculated from a count of 1MB blocks",
              "include": True
            },
            "pct_used": {
              "id": "pct_used",
              "label": "Disk Utilization",
              "value": AnyFloat(),
              "description": "Percent of available space that is used by the table.",
              "include": True
            },
            "stats_off": {
              "id": "stats_off",
              "label": "Stats Off",
              "value": AnyFloat(),
              "description": "Number that indicates how stale the table statistics are; 0 is current, 100 is out of date.",
              "include": True
            },
            "rows": {
              "id": "rows",
              "label": "Approximate Row Count",
              "value": AnyFloat(),
              "description": "Approximate number of rows in the table. This value includes rows marked for deletion, but not yet vacuumed.",
              "include": True
            },
        }

    def _snowflake_stats(self):
        return {
            'has_stats': {
                'id': 'has_stats',
                'label': 'Has Stats?',
                'value': True,
                'description': 'Indicates whether there are statistics for this table',
                'include': False,
            },
            'bytes': {
                'id': 'bytes',
                'label': 'Approximate Size',
                'value': AnyFloat(),
                'description': 'Approximate size of the table as reported by Snowflake',
                'include': True,
            },
            'row_count': {
                'id': 'row_count',
                'label': 'Row Count',
                'value': 1.0,
                'description': 'An approximate count of rows in this table',
                'include': True,
            },
        }

    def _bigquery_stats(self, is_table, partition=None, cluster=None):
        stats = {
            'has_stats': {
                'id': 'has_stats',
                'label': 'Has Stats?',
                'value': True,
                'description': 'Indicates whether there are statistics for this table',
                'include': False,
            },
            'location': {
                'id': 'location',
                'label': 'Location',
                'value': 'US',
                'description':  'The geographic location of this table',
                'include': True,
            },
        }
        if is_table:
            stats.update({
                'num_bytes': {
                    'id': 'num_bytes',
                    'label': 'Number of bytes',
                    'value': AnyFloat(),
                    'description': 'The number of bytes this table consumes',
                    'include': True,
                },
                'num_rows': {
                    'id': 'num_rows',
                    'label': 'Number of rows',
                    'value': AnyFloat(),
                    'description': 'The number of rows in this table',
                    'include': True,
                },
                'partitioning_type': {
                    'id': 'partitioning_type',
                    'label': 'Partitioning Type',
                    'value': partition,
                    'description': 'The partitioning type used for this table',
                    'include': True,
                },
                'clustering_fields': {
                    'id': 'clustering_fields',
                    'label': 'Clustering Fields',
                    'value': cluster,
                    'description': 'The clustering fields for this table',
                    'include': True,
                },
            })
        return stats

    def _expected_catalog(self, id_type, text_type, time_type, view_type,
                          table_type, model_stats, seed_stats=None, case=None,
                          model_database=None):
        if case is None:
            case = lambda x: x
        if seed_stats is None:
            seed_stats = model_stats

        if model_database is None:
            model_database = self.default_database
        my_schema_name = self.unique_schema()
        role = self.get_role()
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
                    'database': model_database,
                    'name': case('model'),
                    'type': view_type,
                    'comment': None,
                    'owner': self.get_role(),
                },
                'stats': model_stats,
                'columns': expected_cols,
            },
            'seed.test.seed': {
                'unique_id': 'seed.test.seed',
                'metadata': {
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'name': case('seed'),
                    'type': table_type,
                    'comment': None,
                    'owner': self.get_role(),
                },
                'stats': seed_stats,
                'columns': expected_cols,
            },
        }

    def expected_postgres_catalog(self):
        return self._expected_catalog(
            id_type='integer',
            text_type='text',
            time_type='timestamp without time zone',
            view_type='VIEW',
            table_type='BASE TABLE',
            model_stats=self._no_stats()
        )

    def get_role(self):
        if self.adapter_type in {'postgres', 'redshift'}:
            profile = self.get_profile(self.adapter_type)
            target_name = profile['test']['target']
            return profile['test']['outputs'][target_name]['user']
        elif self.adapter_type == 'bigquery':
            return None
        else:  # snowflake
            return self.run_sql('select current_role()', fetch='one')[0]

    def expected_postgres_references_catalog(self):
        model_database = self.default_database
        my_schema_name = self.unique_schema()
        role = self.get_role()
        stats = self._no_stats()
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
                    'database': self.default_database,
                    'name': 'seed',
                    'type': 'BASE TABLE',
                    'comment': None,
                    'owner': role,
                },
                'stats': stats,
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
                    'database': model_database,
                    'name': 'ephemeral_summary',
                    'type': 'BASE TABLE',
                    'comment': None,
                    'owner': role,
                },
                'stats': stats,
                'columns': summary_columns,
            },
            'model.test.view_summary': {
                'unique_id': 'model.test.view_summary',
                'metadata': {
                    'schema': my_schema_name,
                    'database': model_database,
                    'name': 'view_summary',
                    'type': 'VIEW',
                    'comment': None,
                    'owner': role,
                },
                'stats': stats,
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
            model_stats=self._no_stats(),
            seed_stats=self._snowflake_stats(),
            case=lambda x: x.upper(),
            model_database=self.alternative_database)

    def expected_bigquery_catalog(self):
        return self._expected_catalog(
            id_type='INT64',
            text_type='STRING',
            time_type='DATETIME',
            view_type='view',
            table_type='table',
            model_stats=self._bigquery_stats(False),
            seed_stats=self._bigquery_stats(True),
            model_database=self.alternative_database
        )

    @staticmethod
    def _clustered_bigquery_columns(update_type):
        return {
            'id': {
                'comment': None,
                'index': 1,
                'name': 'id',
                'type': 'INT64',
            },
            'first_name': {
                'comment': None,
                'index': 2,
                'name': 'first_name',
                'type': 'STRING',
            },
            'email': {
                'comment': None,
                'index': 3,
                'name': 'email',
                'type': 'STRING',
            },
            'ip_address': {
                'comment': None,
                'index': 4,
                'name': 'ip_address',
                'type': 'STRING',
            },
            'updated_at': {
                'comment': None,
                'index': 5,
                'name': 'updated_at',
                'type': update_type,
            },
        }

    def expected_bigquery_complex_catalog(self):
        my_schema_name = self.unique_schema()
        role = self.get_role()
        table_stats = self._bigquery_stats(True)
        clustering_stats = self._bigquery_stats(True, partition='DAY',
                                                cluster='first_name')
        multi_clustering_stats = self._bigquery_stats(True, partition='DAY',
                                                cluster='first_name,email')
        nesting_columns = {
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
            'model.test.clustered': {
                'unique_id': 'model.test.clustered',
                'metadata': {
                    'comment': None,
                    'name': 'clustered',
                    'owner': None,
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'type': 'table'
                },
                'stats': clustering_stats,
                'columns': self._clustered_bigquery_columns('DATE'),
            },
            'model.test.multi_clustered': {
                'unique_id': 'model.test.multi_clustered',
                'metadata': {
                    'comment': None,
                    'name': 'multi_clustered',
                    'owner': None,
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'type': 'table'
                },
                'stats': multi_clustering_stats,
                'columns': self._clustered_bigquery_columns('DATE'),
            },
            'seed.test.seed': {
                'unique_id': 'seed.test.seed',
                'metadata': {
                    'comment': None,
                    'name': 'seed',
                    'owner': None,
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'type': 'table',
                },
                'stats': table_stats,
                'columns': self._clustered_bigquery_columns('DATETIME'),
            },
            "model.test.nested_view": {
                'unique_id': 'model.test.nested_view',
                "metadata": {
                    "schema": my_schema_name,
                    'database': self.default_database,
                    "name": "nested_view",
                    "type": "view",
                    "owner": role,
                    "comment": None
                },
                'stats': self._bigquery_stats(False),
                "columns": nesting_columns,
            },
            "model.test.nested_table": {
                'unique_id': 'model.test.nested_table',
                "metadata": {
                    "schema": my_schema_name,
                    'database': self.default_database,
                    "name": "nested_table",
                    "type": "table",
                    "owner": role,
                    "comment": None
                },
                'stats': table_stats,
                "columns": nesting_columns,
            }
        }

    def expected_redshift_catalog(self):
        return self._expected_catalog(
            id_type='integer',
            text_type='character varying',
            time_type='timestamp without time zone',
            view_type='VIEW',
            table_type='BASE TABLE',
            model_stats=self._no_stats(),
            seed_stats=self._redshift_stats(),
        )

    def expected_redshift_incremental_catalog(self):
        my_schema_name = self.unique_schema()
        role = self.get_role()
        return {
            'model.test.model': {
                'unique_id': 'model.test.model',
                'metadata': {
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'name': 'model',
                    'type': 'LATE BINDING VIEW',
                    'comment': None,
                    'owner': role,
                },
                # incremental views have no stats
                'stats': self._no_stats(),
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
                    'database': self.default_database,
                    'name': 'seed',
                    'type': 'BASE TABLE',
                    'comment': None,
                    'owner': role,
                },
                'stats': self._redshift_stats(),
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
        helpers_path = _normalize('macros/materializations/helpers.sql')
        self.assertEqual(
            without_sql,
            {
                'path': helpers_path,
                'original_file_path': helpers_path,
                'package_name': 'dbt',
                'root_path': _normalize(os.path.join(
                    os.getcwd(), 'core', 'dbt','include', 'global_project'
                )),
                'name': 'column_list',
                'unique_id': 'macro.dbt.column_list',
                'tags': [],
                'resource_type': 'macro',
                'depends_on': {'macros': []}
            }
        )

    def expected_seeded_manifest(self, model_database=None):
        models_path = self.dir('models')
        model_sql_path = os.path.join(models_path, 'model.sql')
        schema_yml_path = os.path.join(models_path, 'schema.yml')
        my_schema_name = self.unique_schema()

        if model_database is None:
            model_database = self.alternative_database

        config_vars = {'alternate_db': model_database}

        model_config = {
            'database': model_database,
            'enabled': True,
            'materialized': 'view',
            'pre-hook': [],
            'post-hook': [],
            'vars': config_vars,
            'column_types': {},
            'quoting': {},
            'tags': [],
        }

        return {
            'nodes': {
                'model.test.model': {
                    'name': 'model',
                    'root_path': os.getcwd(),
                    'resource_type': 'model',
                    'path': 'model.sql',
                    'original_file_path': model_sql_path,
                    'package_name': 'test',
                    'raw_sql': _read_file(model_sql_path).rstrip('\n'),
                    'refs': [['seed']],
                    'depends_on': {'nodes': ['seed.test.seed'], 'macros': []},
                    'unique_id': 'model.test.model',
                    'empty': False,
                    'fqn': ['test', 'model'],
                    'tags': [],
                    'config': model_config,
                    'schema': my_schema_name,
                    'database': model_database,
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
                    'patch_path': schema_yml_path,
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
                        'quoting': {},
                        'tags': [],
                    },
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'alias': 'seed',
                    'description': '',
                    'columns': {},
                },
                'test.test.not_null_model_id': {
                    'alias': 'not_null_model_id',
                    'column_name': 'id',
                    'columns': {},
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': config_vars,
                        'tags': [],
                    },
                    'depends_on': {'macros': [], 'nodes': ['model.test.model']},
                    'description': '',
                    'empty': False,
                    'fqn': ['test', 'schema_test', 'not_null_model_id'],
                    'name': 'not_null_model_id',
                    'original_file_path': schema_yml_path,
                    'package_name': 'test',
                    'path': _normalize('schema_test/not_null_model_id.sql'),
                    'raw_sql': "{{ test_not_null(model=ref('model'), column_name='id') }}",
                    'refs': [['model']],
                    'resource_type': 'test',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'tags': ['schema'],
                    'unique_id': 'test.test.not_null_model_id'
                },
                'test.test.nothing_model_': {
                    'alias': 'nothing_model_',
                    'columns': {},
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': config_vars,
                        'tags': [],
                    },
                    'depends_on': {'macros': [], 'nodes': ['model.test.model']},
                    'description': '',
                    'empty': False,
                    'fqn': ['test', 'schema_test', 'nothing_model_'],
                    'name': 'nothing_model_',
                    'original_file_path': schema_yml_path,
                    'package_name': 'test',
                    'path': _normalize('schema_test/nothing_model_.sql'),
                    'raw_sql': "{{ test_nothing(model=ref('model'), ) }}",
                    'refs': [['model']],
                    'resource_type': 'test',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'tags': ['schema'],
                    'unique_id': 'test.test.nothing_model_'
                },
                'test.test.unique_model_id': {
                    'alias': 'unique_model_id',
                    'column_name': 'id',
                    'columns': {},
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': config_vars,
                        'tags': [],
                    },
                    'depends_on': {'macros': [], 'nodes': ['model.test.model']},
                    'description': '',
                    'empty': False,
                    'fqn': ['test', 'schema_test', 'unique_model_id'],
                    'name': 'unique_model_id',
                    'original_file_path': schema_yml_path,
                    'package_name': 'test',
                    'path': _normalize('schema_test/unique_model_id.sql'),
                    'raw_sql': "{{ test_unique(model=ref('model'), column_name='id') }}",
                    'refs': [['model']],
                    'resource_type': 'test',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'tags': ['schema'],
                    'unique_id': 'test.test.unique_model_id',
                },
            },
            'parent_map': {
                'model.test.model': ['seed.test.seed'],
                'seed.test.seed': [],
                'test.test.not_null_model_id': ['model.test.model'],
                'test.test.nothing_model_': ['model.test.model'],
                'test.test.unique_model_id': ['model.test.model'],
            },
            'child_map': {
                'model.test.model': [
                    'test.test.not_null_model_id',
                    'test.test.nothing_model_',
                    'test.test.unique_model_id',
                ],
                'seed.test.seed': ['model.test.model'],
                'test.test.not_null_model_id': [],
                'test.test.nothing_model_': [],
                'test.test.unique_model_id': [],
            },
            'docs': {
                'dbt.__overview__': ANY
            },
            'metadata': {
                'project_id': '098f6bcd4621d373cade4e832627b4f6',
                'user_id': None,
                'send_anonymous_usage_stats': False,
            },
            'disabled': [],
        }

    def expected_postgres_references_manifest(self, model_database=None):
        if model_database is None:
            model_database = self.default_database
        config_vars = {'alternate_db': model_database}
        my_schema_name = self.unique_schema()
        docs_path = self.dir('ref_models/docs.md')
        docs_file = _read_file(docs_path).lstrip()
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
                        'vars': config_vars,
                        'tags': [],
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
                    'database': self.default_database,
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
                        'vars': config_vars,
                        'tags': [],
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
                    'database': self.default_database,
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
                        'vars': config_vars,
                        'tags': [],
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
                    'database': self.default_database,
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
                        'vars': {},
                        'tags': [],
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
                    'database': self.default_database,
                    'tags': [],
                    'unique_id': 'seed.test.seed'
                },
            },
            'docs': {
                'dbt.__overview__': ANY,
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
            'disabled': [],
        }

    def expected_bigquery_complex_manifest(self):
        nested_view_sql_path = self.dir('bq_models/nested_view.sql')
        nested_table_sql_path = self.dir('bq_models/nested_table.sql')
        clustered_sql_path = self.dir('bq_models/clustered.sql')
        multi_clustered_sql_path = self.dir('bq_models/multi_clustered.sql')
        my_schema_name = self.unique_schema()
        config_vars = {'alternate_db': self.alternative_database}
        return {
            'nodes': {
                'model.test.clustered': {
                    'alias': 'clustered',
                    'config': {
                        'cluster_by': ['first_name'],
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'table',
                        'partition_by': 'updated_at',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': config_vars,
                        'tags': [],
                    },
                    'depends_on': {'macros': [], 'nodes': ['seed.test.seed']},
                    'empty': False,
                    'fqn': ['test', 'clustered'],
                    'name': 'clustered',
                    'original_file_path': clustered_sql_path,
                    'package_name': 'test',
                    'path': 'clustered.sql',
                    'raw_sql': _read_file(clustered_sql_path).rstrip('\n'),
                    'refs': [['seed']],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'tags': [],
                    'unique_id': 'model.test.clustered',
                    'columns': {
                        'email': {
                            'description': "The user's email",
                            'name': 'email'
                        },
                        'first_name': {
                            'description': "The user's name",
                            'name': 'first_name'
                        },
                        'id': {
                            'description': 'The user id',
                            'name': 'id'
                        },
                        'ip_address': {
                            'description': "The user's IP address",
                            'name': 'ip_address'
                        },
                        'updated_at': {
                            'description': 'When the user was updated',
                            'name': 'updated_at'
                        },
                    },
                    'description': 'A clustered and partitioned copy of the test model',
                    'patch_path': self.dir('bq_models/schema.yml'),
                    'docrefs': [],
                },
                'model.test.multi_clustered': {
                    'alias': 'multi_clustered',
                    'config': {
                        'cluster_by': ['first_name','email'],
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'table',
                        'partition_by': 'updated_at',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'tags': [],
                        'vars': config_vars
                    },
                    'depends_on': {'macros': [], 'nodes': ['seed.test.seed']},
                    'empty': False,
                    'fqn': ['test', 'multi_clustered'],
                    'name': 'multi_clustered',
                    'original_file_path': multi_clustered_sql_path,
                    'package_name': 'test',
                    'path': 'multi_clustered.sql',
                    'raw_sql': _read_file(multi_clustered_sql_path).rstrip('\n'),
                    'refs': [['seed']],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'tags': [],
                    'unique_id': 'model.test.multi_clustered',
                    'columns': {
                        'email': {
                            'description': "The user's email",
                            'name': 'email'
                        },
                        'first_name': {
                            'description': "The user's name",
                            'name': 'first_name'
                        },
                        'id': {
                            'description': 'The user id',
                            'name': 'id'
                        },
                        'ip_address': {
                            'description': "The user's IP address",
                            'name': 'ip_address'
                        },
                        'updated_at': {
                            'description': 'When the user was updated',
                            'name': 'updated_at'
                        },
                    },
                    'description': 'A clustered and partitioned copy of the test model, clustered on multiple columns',
                    'patch_path': self.dir('bq_models/schema.yml'),
                    'docrefs': [],
                },
                'model.test.nested_view': {
                    'alias': 'nested_view',
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': config_vars,
                        'tags': [],
                    },
                    'depends_on': {
                        'macros': [],
                        'nodes': ['model.test.nested_table']
                    },
                    'empty': False,
                    'fqn': ['test', 'nested_view'],
                    'name': 'nested_view',
                    'original_file_path': nested_view_sql_path,
                    'package_name': 'test',
                    'path': 'nested_view.sql',
                    'raw_sql': _read_file(nested_view_sql_path).rstrip('\n'),
                    'refs': [['nested_table']],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'tags': [],
                    'unique_id': 'model.test.nested_view',
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
                'model.test.nested_table': {
                    'alias': 'nested_table',
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'table',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': config_vars,
                        'tags': [],
                    },
                    'depends_on': {
                        'macros': [],
                        'nodes': []
                    },
                    'empty': False,
                    'fqn': ['test', 'nested_table'],
                    'name': 'nested_table',
                    'original_file_path': nested_table_sql_path,
                    'package_name': 'test',
                    'path': 'nested_table.sql',
                    'raw_sql': _read_file(nested_table_sql_path).rstrip('\n'),
                    'refs': [],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'tags': [],
                    'unique_id': 'model.test.nested_table',
                    'columns': {},
                    'description': '',
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
                    'depends_on': {
                        'nodes': [],
                        'macros': [],
                    },
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
                        'quoting': {},
                        'tags': [],
                    },
                    'schema': my_schema_name,
                    'database': self.default_database,
                    'alias': 'seed',
                    'columns': {},
                    'description': '',
                },
            },
            'child_map': {
                'model.test.clustered': [],
                'model.test.multi_clustered': [],
                'model.test.nested_table': ['model.test.nested_view'],
                'model.test.nested_view': [],
                'seed.test.seed': ['model.test.clustered','model.test.multi_clustered']
            },
            'parent_map': {
                'model.test.clustered': ['seed.test.seed'],
                'model.test.multi_clustered': ['seed.test.seed'],
                'seed.test.seed': [],
                'model.test.nested_table': [],
                'model.test.nested_view': ['model.test.nested_table'],
            },
            'docs': {
                'dbt.__overview__': ANY,
            },
            'metadata': {
                'project_id': '098f6bcd4621d373cade4e832627b4f6',
                'user_id': None,
                'send_anonymous_usage_stats': False,
            },
            'disabled': [],
        }

    def expected_redshift_incremental_view_manifest(self):
        model_sql_path = self.dir('rs_models/model.sql')
        my_schema_name = self.unique_schema()
        config_vars = {'alternate_db': self.default_database}
        return {
            "nodes": {
                "model.test.model": {
                    "name": "model",
                    "root_path": os.getcwd(),
                    "resource_type": "model",
                    "path": "model.sql",
                    "original_file_path": model_sql_path,
                    "package_name": "test",
                    "raw_sql": _read_file(model_sql_path).rstrip('\n'),
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
                        "vars": config_vars,
                        "column_types": {},
                        "quoting": {},
                        "tags": [],
                    },
                    "schema": my_schema_name,
                    'database': self.default_database,
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
                        "tags": [],
                    },
                    "schema": my_schema_name,
                    'database': self.default_database,
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
            'docs': {
                'dbt.__overview__': ANY,
            },
            'metadata': {
                'project_id': '098f6bcd4621d373cade4e832627b4f6',
                'user_id': None,
                'send_anonymous_usage_stats': False,
            },
            'disabled': [],
        }

    def verify_manifest(self, expected_manifest):
        self.assertTrue(os.path.exists('./target/manifest.json'))

        with open('./target/manifest.json') as fp:
            manifest = json.load(fp)

        self.assertEqual(
            set(manifest),
            {'nodes', 'macros', 'parent_map', 'child_map', 'generated_at',
             'docs', 'metadata', 'docs', 'disabled'}
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
        self.assertEqual(manifest['disabled'], [])
        self.assertEqual(manifest_without_extras, expected_manifest)

    def _quote(self, value):
        quote_char = '`' if self.adapter_type == 'bigquery' else '"'
        return '{0}{1}{0}'.format(quote_char, value)

    def expected_run_results(self, quote_schema=True, quote_model=False,
                             model_database=None):
        """
        The expected results of this run.
        """
        models_path = self.dir('models')
        model_sql_path = os.path.join(models_path, 'model.sql')
        schema_yml_path = os.path.join(models_path, 'schema.yml')

        if model_database is None:
            model_database = self.alternative_database

        config_vars = {'alternate_db': model_database}

        model_config = {
            'database': model_database,
            'enabled': True,
            'materialized': 'view',
            'pre-hook': [],
            'post-hook': [],
            'vars': config_vars,
            'column_types': {},
            'quoting': {},
            'tags': [],
        }
        schema = self.unique_schema()

        # we are selecting from the seed, which is always in the default db
        compiled_database = self._quote(self.default_database)
        compiled_schema = self._quote(schema) if quote_schema else schema
        compiled_seed = self._quote('seed') if quote_model else 'seed'

        if self.adapter_type == 'bigquery':
            status = 'OK'
            compiled_sql = '\n\nselect * from `{}`.`{}`.seed'.format(
                self.default_database, schema
            )
        else:
            compiled_sql = '\n\nselect * from {}.{}.{}'.format(
                compiled_database, compiled_schema, compiled_seed
            )

        return [
            {
                'error': None,
                'execution_time': AnyFloat(),
                'fail': None,
                'node': {
                    'alias': 'model',
                    'build_path': _normalize(
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
                    'config': model_config,
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
                    'original_file_path': model_sql_path,
                    'package_name': 'test',
                    'patch_path': schema_yml_path,
                    'path': 'model.sql',
                    'raw_sql': _read_file(model_sql_path).rstrip('\n'),
                    'refs': [['seed']],
                    'resource_type': 'model',
                    'root_path': os.getcwd(),
                    'schema': schema,
                    'database': model_database,
                    'tags': [],
                    'unique_id': 'model.test.model',
                    'wrapped_sql': 'None'
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
                    'build_path': _normalize(
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
                        'tags': [],
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
                    'database': self.default_database,
                    'tags': [],
                    'unique_id': 'seed.test.seed',
                    'wrapped_sql': 'None'
                },
                'skip': False,
                'status': None,
            },
            {
                'error': None,
                'execution_time': AnyFloat(),
                'fail': None,
                'node': {
                    'alias': 'not_null_model_id',
                     'build_path': _normalize('target/compiled/test/schema_test/not_null_model_id.sql'),
                     'column_name': 'id',
                     'columns': {},
                     'compiled': True,
                     'compiled_sql': AnyStringWith('id is null'),
                     'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': config_vars,
                        'tags': [],
                    },
                    'depends_on': {'macros': [], 'nodes': ['model.test.model']},
                    'description': '',
                    'empty': False,
                    'extra_ctes': [],
                    'extra_ctes_injected': True,
                    'fqn': ['test', 'schema_test', 'not_null_model_id'],
                    'injected_sql': AnyStringWith('id is null'),
                    'name': 'not_null_model_id',
                    'original_file_path': schema_yml_path,
                    'package_name': 'test',
                    'path': _normalize('schema_test/not_null_model_id.sql'),
                    'raw_sql': "{{ test_not_null(model=ref('model'), column_name='id') }}",
                    'refs': [['model']],
                    'resource_type': 'test',
                    'root_path': os.getcwd(),
                    'schema': schema,
                    'database': self.default_database,
                    'tags': ['schema'],
                    'unique_id': 'test.test.not_null_model_id',
                    'wrapped_sql': AnyStringWith('id is null')
                },
                'skip': False,
                'status': None,
            },
            {
                'error': None,
                'execution_time': AnyFloat(),
                'fail': None,
                'node': {
                    'alias': 'nothing_model_',
                    'build_path': _normalize('target/compiled/test/schema_test/nothing_model_.sql'),
                    'columns': {},
                    'compiled': True,
                    'compiled_sql': AnyStringWith('select 0'),
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': config_vars,
                        'tags': [],
                    },
                    'depends_on': {'macros': [], 'nodes': ['model.test.model']},
                    'description': '',
                    'empty': False,
                    'extra_ctes': [],
                    'extra_ctes_injected': True,
                    'fqn': ['test', 'schema_test', 'nothing_model_'],
                    'injected_sql':  AnyStringWith('select 0'),
                    'name': 'nothing_model_',
                    'original_file_path': schema_yml_path,
                    'package_name': 'test',
                    'path': _normalize('schema_test/nothing_model_.sql'),
                    'raw_sql': "{{ test_nothing(model=ref('model'), ) }}",
                    'refs': [['model']],
                    'resource_type': 'test',
                    'root_path': os.getcwd(),
                    'schema': schema,
                    'database': self.default_database,
                    'tags': ['schema'],
                    'unique_id': 'test.test.nothing_model_',
                    'wrapped_sql':  AnyStringWith('select 0'),
                },
                'skip': False,
                'status': None
            },
            {
                'error': None,
                'execution_time': AnyFloat(),
                'fail': None,
                'node': {
                    'alias': 'unique_model_id',
                    'build_path': _normalize('target/compiled/test/schema_test/unique_model_id.sql'),
                    'column_name': 'id',
                    'columns': {},
                    'compiled': True,
                    'compiled_sql': AnyStringWith('count(*)'),
                    'config': {
                        'column_types': {},
                        'enabled': True,
                        'materialized': 'view',
                        'post-hook': [],
                        'pre-hook': [],
                        'quoting': {},
                        'vars': config_vars,
                        'tags': [],
                    },
                    'depends_on': {'macros': [], 'nodes': ['model.test.model']},
                    'description': '',
                    'empty': False,
                    'extra_ctes': [],
                    'extra_ctes_injected': True,
                    'fqn': ['test', 'schema_test', 'unique_model_id'],
                    'injected_sql': AnyStringWith('count(*)'),
                    'name': 'unique_model_id',
                    'original_file_path': schema_yml_path,
                    'package_name': 'test',
                    'path': _normalize('schema_test/unique_model_id.sql'),
                    'raw_sql': "{{ test_unique(model=ref('model'), column_name='id') }}",
                    'refs': [['model']],
                    'resource_type': 'test',
                    'root_path': os.getcwd(),
                    'schema': schema,
                    'database': self.default_database,
                    'tags': ['schema'],
                    'unique_id': 'test.test.unique_model_id',
                    'wrapped_sql': AnyStringWith('count(*)')
                },
                'skip': False,
                'status': None,
            },
        ]

    def expected_postgres_references_run_results(self):
        my_schema_name = self.unique_schema()
        config_vars = {'alternate_db': self.default_database}
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
            '\n\nselect first_name, ct from "{}"."{}".ephemeral_summary\n'
            'order by ct asc'
        ).format(self.default_database, my_schema_name)

        return [
            {
                'error': None,
                'execution_time': AnyFloat(),
                'fail': None,
                'node': {
                    'alias': 'ephemeral_summary',
                    'build_path': _normalize(
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
                        'vars': config_vars,
                        'column_types': {},
                        'quoting': {},
                        'tags': [],
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
                    'database': self.default_database,
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
                    'build_path': _normalize(
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
                        'vars': config_vars,
                        'column_types': {},
                        'quoting': {},
                        'tags': [],
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
                    'database': self.default_database,
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
                    'build_path': _normalize(
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
                        'tags': [],
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
                    'database': self.default_database,
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
        self.run_and_generate(alternate_db=self.default_database)
        self.verify_catalog(self.expected_postgres_catalog())
        self.verify_manifest(self.expected_seeded_manifest(
            model_database=self.default_database
        ))
        self.verify_run_results(self.expected_run_results(
            model_database=self.default_database
        ))

    @use_profile('postgres')
    def test__postgres_references(self):
        self.run_and_generate(
            {'source-paths': [self.dir('ref_models')]},
            model_count=2,
            alternate_db=self.default_database
        )

        self.verify_catalog(self.expected_postgres_references_catalog())
        self.verify_manifest(self.expected_postgres_references_manifest())
        self.verify_run_results(self.expected_postgres_references_run_results())

    @use_profile('snowflake')
    def test__snowflake__run_and_generate(self):
        self.run_and_generate()

        self.verify_catalog(self.expected_snowflake_catalog())
        self.verify_manifest(self.expected_seeded_manifest())
        self.verify_run_results(self.expected_run_results(
            quote_schema=False,
            quote_model=False
        ))

    @use_profile('snowflake')
    def test__snowflake__run_and_generate_ignore_quoting_parameter(self):
        # with optional adapters, this package could easily just not exist!
        # accordingly, only run it when we think snowflake things should work
        from dbt.adapters.snowflake import connections as snowflake_conn
        old_connect = snowflake_conn.snowflake.connector.connect
        def connect(*args, **kwargs):
            kwargs['session_parameters'] = {
                'QUOTED_IDENTIFIERS_IGNORE_CASE':True
            }
            return old_connect(*args, **kwargs)
        with patch.object(snowflake_conn.snowflake.connector, 'connect', connect):
            self.run_and_generate({
                'quoting': {
                    'identifier': True,
                }
            })

        self.verify_catalog(self.expected_snowflake_catalog())
        self.verify_manifest(self.expected_seeded_manifest())
        self.verify_run_results(self.expected_run_results(quote_schema=False, quote_model=True))

    @use_profile('bigquery')
    def test__bigquery__run_and_generate(self):
        self.run_and_generate()

        self.verify_catalog(self.expected_bigquery_catalog())
        self.verify_manifest(self.expected_seeded_manifest())
        self.verify_run_results(self.expected_run_results())

    @use_profile('bigquery')
    def test__bigquery__complex_models(self):
        self.run_and_generate(
            extra={'source-paths': [self.dir('bq_models')]},
            model_count=4
        )

        self.verify_catalog(self.expected_bigquery_complex_catalog())
        self.verify_manifest(self.expected_bigquery_complex_manifest())

    @use_profile('redshift')
    def test__redshift__run_and_generate(self):
        self.run_and_generate(alternate_db=self.default_database)
        self.verify_catalog(self.expected_redshift_catalog())
        self.verify_manifest(self.expected_seeded_manifest(
            model_database=self.default_database
        ))
        self.verify_run_results(self.expected_run_results(
            model_database=self.default_database
        ))

    @use_profile('redshift')
    def test__redshift__incremental_view(self):
        self.run_and_generate(
            {'source-paths': [self.dir('rs_models')]},
            alternate_db=self.default_database
        )
        self.verify_catalog(self.expected_redshift_incremental_catalog())
        self.verify_manifest(self.expected_redshift_incremental_view_manifest())
