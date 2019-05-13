import unittest
import dbt.main as dbt
import os, shutil
import yaml
import random
import time
import json
from datetime import datetime
from functools import wraps

from nose.plugins.attrib import attr

import dbt.flags as flags

from dbt.adapters.factory import get_adapter, reset_adapters
from dbt.clients.jinja import template_cache
from dbt.config import RuntimeConfig
from dbt.compat import basestring, suppress_warnings

from dbt.logger import GLOBAL_LOGGER as logger
import logging
import warnings


DBT_CONFIG_DIR = os.path.abspath(
    os.path.expanduser(os.environ.get("DBT_CONFIG_DIR", '/home/dbt_test_user/.dbt'))
)

DBT_PROFILES = os.path.join(DBT_CONFIG_DIR, 'profiles.yml')


class FakeArgs(object):
    def __init__(self):
        self.threads = 1
        self.data = False
        self.schema = True
        self.full_refresh = False
        self.models = None
        self.exclude = None
        self.single_threaded = False


class TestArgs(object):
    def __init__(self, kwargs):
        self.which = 'run'
        self.single_threaded = False
        self.profiles_dir = DBT_CONFIG_DIR
        self.__dict__.update(kwargs)


def _profile_from_test_name(test_name):
    adapter_names = ('postgres', 'snowflake', 'redshift', 'bigquery', 'presto')
    adapters_in_name =  sum(x in test_name for x in adapter_names)
    if adapters_in_name > 1:
        raise ValueError('test names must only have 1 profile choice embedded')

    for adapter_name in adapter_names:
        if adapter_name in test_name:
            return adapter_name

    warnings.warn(
        'could not find adapter name in test name {}'.format(test_name)
    )
    return 'postgres'


class DBTIntegrationTest(unittest.TestCase):
    CREATE_SCHEMA_STATEMENT = 'CREATE SCHEMA {}'
    DROP_SCHEMA_STATEMENT = 'DROP SCHEMA IF EXISTS {} CASCADE'

    prefix = "test{}{:04}".format(int(time.time()), random.randint(0, 9999))
    setup_alternate_db = False

    @property
    def database_host(self):
        if os.name == 'nt':
            return 'localhost'
        return 'database'

    def postgres_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'postgres',
                        'threads': 4,
                        'host': self.database_host,
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                    'noaccess': {
                        'type': 'postgres',
                        'threads': 4,
                        'host': self.database_host,
                        'port': 5432,
                        'user': 'noaccess',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    }
                },
                'target': 'default2'
            }
        }

    def redshift_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'redshift',
                        'threads': 1,
                        'host': os.getenv('REDSHIFT_TEST_HOST'),
                        'port': os.getenv('REDSHIFT_TEST_PORT'),
                        'user': os.getenv('REDSHIFT_TEST_USER'),
                        'pass': os.getenv('REDSHIFT_TEST_PASS'),
                        'dbname': os.getenv('REDSHIFT_TEST_DBNAME'),
                        'schema': self.unique_schema()
                    }
                },
                'target': 'default2'
            }
        }

    def snowflake_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'snowflake',
                        'threads': 4,
                        'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
                        'user': os.getenv('SNOWFLAKE_TEST_USER'),
                        'password': os.getenv('SNOWFLAKE_TEST_PASSWORD'),
                        'database': os.getenv('SNOWFLAKE_TEST_DATABASE'),
                        'schema': self.unique_schema(),
                        'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
                    },
                    'noaccess': {
                        'type': 'snowflake',
                        'threads': 4,
                        'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
                        'user': 'noaccess',
                        'password': 'password',
                        'database': os.getenv('SNOWFLAKE_TEST_DATABASE'),
                        'schema': self.unique_schema(),
                        'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
                    }
                },
                'target': 'default2'
            }
        }

    def bigquery_profile(self):
        credentials_json_str = os.getenv('BIGQUERY_SERVICE_ACCOUNT_JSON').replace("'", '')
        credentials = json.loads(credentials_json_str)
        project_id = credentials.get('project_id')

        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'bigquery',
                        'method': 'service-account-json',
                        'threads': 1,
                        'project': project_id,
                        'keyfile_json': credentials,
                        'schema': self.unique_schema(),
                    },
                },
                'target': 'default2'
            }
        }

    def presto_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'presto',
                        'method': 'none',
                        'threads': 1,
                        'schema': self.unique_schema(),
                        'database': 'hive',
                        'host': 'presto',
                        'port': 8080,
                    },
                },
                'target': 'default2'
            }
        }

    @property
    def packages_config(self):
        return None

    def unique_schema(self):
        schema = self.schema

        to_return = "{}_{}".format(self.prefix, schema)

        if self.adapter_type == 'snowflake':
            return to_return.upper()

        return to_return.lower()

    @property
    def default_database(self):
        database = self.config.credentials.database
        if self.adapter_type == 'snowflake':
            return database.upper()
        return database

    @property
    def alternative_database(self):
        if self.adapter_type == 'bigquery':
            return os.environ['BIGQUERY_TEST_ALT_DATABASE']
        elif self.adapter_type == 'snowflake':
            return os.environ['SNOWFLAKE_TEST_ALT_DATABASE']
        return None

    def get_profile(self, adapter_type):
        if adapter_type == 'postgres':
            return self.postgres_profile()
        elif adapter_type == 'snowflake':
            return self.snowflake_profile()
        elif adapter_type == 'bigquery':
            return self.bigquery_profile()
        elif adapter_type == 'redshift':
            return self.redshift_profile()
        elif adapter_type == 'presto':
            return self.presto_profile()
        else:
            raise ValueError('invalid adapter type {}'.format(adapter_type))

    def _pick_profile(self):
        test_name = self.id().split('.')[-1]
        return _profile_from_test_name(test_name)

    def setUp(self):
        self._created_schemas = set()
        flags.reset()
        template_cache.clear()
        # disable capturing warnings
        logging.captureWarnings(False)
        self._clean_files()

        self.use_profile(self._pick_profile())
        self.use_default_project()
        self.set_packages()
        self.load_config()

    def use_default_project(self, overrides=None):
        # create a dbt_project.yml
        base_project_config = {
            'name': 'test',
            'version': '1.0',
            'test-paths': [],
            'source-paths': [self.models],
            'profile': 'test',
        }

        project_config = {}
        project_config.update(base_project_config)
        project_config.update(self.project_config)
        project_config.update(overrides or {})

        with open("dbt_project.yml", 'w') as f:
            yaml.safe_dump(project_config, f, default_flow_style=True)

    def use_profile(self, adapter_type):
        self.adapter_type = adapter_type

        profile_config = {}
        default_profile_config = self.get_profile(adapter_type)

        profile_config.update(default_profile_config)
        profile_config.update(self.profile_config)

        if not os.path.exists(DBT_CONFIG_DIR):
            os.makedirs(DBT_CONFIG_DIR)

        with open(DBT_PROFILES, 'w') as f:
            yaml.safe_dump(profile_config, f, default_flow_style=True)
        self._profile_config = profile_config

    def set_packages(self):
        if self.packages_config is not None:
            with open('packages.yml', 'w') as f:
                yaml.safe_dump(self.packages_config, f, default_flow_style=True)

    def load_config(self):
        # we've written our profile and project. Now we want to instantiate a
        # fresh adapter for the tests.
        # it's important to use a different connection handle here so
        # we don't look into an incomplete transaction
        kwargs = {
            'profile': None,
            'profile_dir': DBT_CONFIG_DIR,
            'target': None,
        }

        config = RuntimeConfig.from_args(TestArgs(kwargs))

        adapter = get_adapter(config)
        adapter.cleanup_connections()
        self.adapter_type = adapter.type()
        self.adapter = adapter
        self.config = config

        self._drop_schemas()
        self._create_schemas()

    def quote_as_configured(self, value, quote_key):
        return self.adapter.quote_as_configured(value, quote_key)

    def _clean_files(self):
        if os.path.exists(DBT_PROFILES):
            os.remove(DBT_PROFILES)
        if os.path.exists('dbt_project.yml'):
            os.remove("dbt_project.yml")
        if os.path.exists('packages.yml'):
            os.remove('packages.yml')
        # quick fix for windows bug that prevents us from deleting dbt_modules
        try:
            if os.path.exists('dbt_modules'):
                shutil.rmtree('dbt_modules')
        except:
            os.rename("dbt_modules", "dbt_modules-{}".format(time.time()))

    def tearDown(self):
        self._clean_files()

        # get any current run adapter and clean up its connections before we
        # reset them. It'll probably be different from ours because
        # handle_and_check() calls reset_adapters().
        adapter = get_adapter(self.config)
        if adapter is not self.adapter:
            adapter.cleanup_connections()
        if not hasattr(self, 'adapter'):
            self.adapter = adapter

        self._drop_schemas()

        self.adapter.cleanup_connections()
        reset_adapters()

    def _get_schema_fqn(self, database, schema):
        schema_fqn = self.quote_as_configured(schema, 'schema')
        if self.adapter_type == 'snowflake':
            database = self.quote_as_configured(database, 'database')
            schema_fqn = '{}.{}'.format(database, schema_fqn)
        return schema_fqn

    def _create_schema_named(self, database, schema):
        if self.adapter_type == 'bigquery':
            self.adapter.create_schema(database, schema, '__test')
        else:
            schema_fqn = self._get_schema_fqn(database, schema)
            self.run_sql(self.CREATE_SCHEMA_STATEMENT.format(schema_fqn))
            self._created_schemas.add(schema_fqn)

    def _drop_schema_named(self, database, schema):
        if self.adapter_type == 'bigquery' or self.adapter_type == 'presto':
            self.adapter.drop_schema(
                database, schema, '__test'
            )
        else:
            schema_fqn = self._get_schema_fqn(database, schema)
            self.run_sql(self.DROP_SCHEMA_STATEMENT.format(schema_fqn))

    def _create_schemas(self):
        schema = self.unique_schema()
        self._create_schema_named(self.default_database, schema)
        if self.setup_alternate_db and self.adapter_type == 'snowflake':
            self._create_schema_named(self.alternative_database, schema)

    def _drop_schemas_adapter(self):
        schema = self.unique_schema()
        if self.adapter_type == 'bigquery' or self.adapter_type == 'presto':
            self._drop_schema_named(self.default_database, schema)
            if self.setup_alternate_db and self.alternative_database:
                self._drop_schema_named(self.alternative_database, schema)

    def _drop_schemas_sql(self):
        schema = self.unique_schema()
        # we always want to drop these if necessary, we'll clear it soon.
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, schema)
        )
        # on postgres/redshift, this will make you sad
        drop_alternative = self.setup_alternate_db and \
                self.adapter_type not in {'postgres', 'redshift'} and \
                self.alternative_database
        if drop_alternative:
            self._created_schemas.add(
                self._get_schema_fqn(self.alternative_database, schema)
            )

        for schema_fqn in self._created_schemas:
            self.run_sql(self.DROP_SCHEMA_STATEMENT.format(schema_fqn))

        self._created_schemas.clear()

    def _drop_schemas(self):
        if self.adapter_type == 'bigquery' or self.adapter_type == 'presto':
            self._drop_schemas_adapter()
        else:
            self._drop_schemas_sql()

    @property
    def project_config(self):
        return {}

    @property
    def profile_config(self):
        return {}

    def run_dbt(self, args=None, expect_pass=True, strict=True):
        if args is None:
            args = ["run"]

        if strict:
            args = ["--strict"] + args
        args.append('--log-cache-events')
        logger.info("Invoking dbt with {}".format(args))

        res, success = dbt.handle_and_check(args)
        self.assertEqual(
            success, expect_pass,
            "dbt exit state did not match expected")

        return res

    def run_dbt_and_check(self, args=None):
        if args is None:
            args = ["run"]

        args = ["--strict"] + args
        logger.info("Invoking dbt with {}".format(args))
        return dbt.handle_and_check(args)

    def run_sql_file(self, path, kwargs=None):
        with open(path, 'r') as f:
            statements = f.read().split(";")
            for statement in statements:
                self.run_sql(statement, kwargs=kwargs)

    # horrible hack to support snowflake for right now
    def transform_sql(self, query, kwargs=None):
        to_return = query

        if self.adapter_type == 'snowflake':
            to_return = to_return.replace("BIGSERIAL", "BIGINT AUTOINCREMENT")

        base_kwargs = {
            'schema': self.unique_schema(),
            'database': self.adapter.quote(self.default_database),
        }
        if kwargs is None:
            kwargs = {}
        base_kwargs.update(kwargs)


        to_return = to_return.format(**base_kwargs)

        return to_return

    def run_sql_bigquery(self, sql, fetch):
        """Run an SQL query on a bigquery adapter. No cursors, transactions,
        etc. to worry about"""

        do_fetch = fetch != 'None'
        _, res = self.adapter.execute(sql, fetch=do_fetch)

        # convert dataframe to matrix-ish repr
        if fetch == 'one':
            return res[0]
        else:
            return list(res)

    def run_sql_presto(self, sql, fetch, connection_name=None):
        conn = self.adapter.acquire_connection(connection_name)
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if fetch == 'one':
                return cursor.fetchall()[0]
            elif fetch == 'all':
                return cursor.fetchall()
            else:
                # we have to fetch.
                cursor.fetchall()
        except Exception as e:
            conn.handle.rollback()
            conn.transaction_open = False
            print(sql)
            print(e)
            raise
        else:
            conn.handle.commit()
            conn.transaction_open = False


    def run_sql(self, query, fetch='None', kwargs=None, connection_name=None):
        if connection_name is None:
            connection_name = '__test'

        if query.strip() == "":
            return

        sql = self.transform_sql(query, kwargs=kwargs)
        if self.adapter_type == 'bigquery':
            return self.run_sql_bigquery(sql, fetch)
        elif self.adapter_type == 'presto':
            return self.run_sql_presto(sql, fetch, connection_name)

        conn = self.adapter.acquire_connection(connection_name)
        with conn.handle.cursor() as cursor:
            logger.debug('test connection "{}" executing: {}'.format(connection_name, sql))
            try:
                cursor.execute(sql)
                conn.handle.commit()
                if fetch == 'one':
                    return cursor.fetchone()
                elif fetch == 'all':
                    return cursor.fetchall()
                else:
                    return
            except BaseException as e:
                conn.handle.rollback()
                print(query)
                print(e)
                raise e
            finally:
                conn.transaction_open = False

    def _ilike(self, target, value):
        # presto has this regex substitution monstrosity instead of 'ilike'
        if self.adapter_type == 'presto':
            return r"regexp_like({}, '(?i)\A{}\Z')".format(target, value)
        else:
            return "{} ilike '{}'".format(target, value)

    def get_many_table_columns(self, tables, schema, database=None):
        if self.adapter_type == 'bigquery':
            result = []
            for table in tables:
                relation = self._make_relation(table, schema, database)
                columns = self.adapter.get_columns_in_relation(relation)
                for col in columns:
                    result.append((table, col.column, col.dtype, col.char_size))
            result.sort(key=lambda x: '{}.{}'.format(x[0], x[1]))
            return result
        elif self.adapter_type == 'presto':
            columns = 'table_name, column_name, data_type'
        else:
            columns = 'table_name, column_name, data_type, character_maximum_length'

        sql = """
                select {columns}
                from {db_string}information_schema.columns
                where {schema_filter}
                  and ({table_filter})
                order by column_name asc"""

        db_string = ''
        if database:
            db_string = self.quote_as_configured(database, 'database') + '.'

        table_filters_s = " OR ".join(
            self._ilike('table_name', table.replace('"', ''))
            for table in tables
        )
        schema_filter = self._ilike('table_schema', schema)

        sql = sql.format(
                columns=columns,
                schema_filter=schema_filter,
                table_filter=table_filters_s,
                db_string=db_string)

        columns = self.run_sql(sql, fetch='all')
        return sorted(map(self.filter_many_columns, columns),
                      key=lambda x: "{}.{}".format(x[0], x[1]))

    def filter_many_columns(self, column):
        if len(column) == 3:
            table_name, column_name, data_type = column
            char_size = None
        else:
            table_name, column_name, data_type, char_size = column
        # in snowflake, all varchar widths are created equal
        if self.adapter_type == 'snowflake':
            if char_size and char_size < 16777216:
                char_size = 16777216
        return (table_name, column_name, data_type, char_size)

    def get_relation_columns(self, relation):
        columns = self.adapter.get_columns_in_relation(
            relation,
            model_name='__test'
        )

        return sorted(((c.name, c.dtype, c.char_size) for c in columns),
                      key=lambda x: x[0])

    def get_table_columns(self, table, schema=None, database=None):
        schema = self.unique_schema() if schema is None else schema
        database = self.default_database if database is None else database
        relation = self.adapter.Relation.create(
            database = database,
            schema=schema,
            identifier=table,
            type='table',
            quote_policy=self.config.quoting
        )
        return self.get_relation_columns(relation)

    def get_table_columns_as_dict(self, tables, schema=None):
        col_matrix = self.get_many_table_columns(tables, schema)
        res = {}
        for row in col_matrix:
            table_name = row[0]
            col_def = row[1:]
            if table_name not in res:
                res[table_name] = []
            res[table_name].append(col_def)
        return res

    def get_models_in_schema(self, schema=None):
        schema = self.unique_schema() if schema is None else schema
        sql = """
                select table_name,
                        case when table_type = 'BASE TABLE' then 'table'
                             when table_type = 'VIEW' then 'view'
                             else table_type
                        end as materialization
                from information_schema.tables
                where {}
                order by table_name
                """

        sql = sql.format(self._ilike('table_schema', schema))
        result = self.run_sql(sql, fetch='all')

        return {model_name: materialization for (model_name, materialization) in result}

    def _assertTablesEqualSql(self, relation_a, relation_b, columns=None):
        if columns is None:
            columns = self.get_relation_columns(relation_a)

        if self.adapter_type == 'snowflake':
            columns_csv = ", ".join(['"{}"'.format(record[0]) for record in columns])
        else:
            columns_csv = ", ".join(['{}'.format(record[0]) for record in columns])

        if self.adapter_type == 'bigquery':
            except_operator = 'EXCEPT DISTINCT'
        else:
            except_operator = 'EXCEPT'

        sql = """
            SELECT COUNT(*) FROM (
                (SELECT {columns} FROM {relation_a} {except_op}
                 SELECT {columns} FROM {relation_b})
                 UNION ALL
                (SELECT {columns} FROM {relation_b} {except_op}
                 SELECT {columns} FROM {relation_a})
            ) AS a""".format(
                columns=columns_csv,
                relation_a=str(relation_a),
                relation_b=str(relation_b),
                except_op=except_operator
            )

        return sql

    def assertTablesEqual(self, table_a, table_b,
                          table_a_schema=None, table_b_schema=None,
                          table_a_db=None, table_b_db=None):
        if table_a_schema is None:
            table_a_schema = self.unique_schema()

        if table_b_schema is None:
            table_b_schema = self.unique_schema()

        if table_a_db is None:
            table_a_db = self.default_database

        if table_b_db is None:
            table_b_db = self.default_database

        relation_a = self._make_relation(table_a, table_a_schema, table_a_db)
        relation_b = self._make_relation(table_b, table_b_schema, table_b_db)


        self._assertTableColumnsEqual(relation_a, relation_b)
        self._assertTableRowCountsEqual(relation_a, relation_b)

        sql = self._assertTablesEqualSql(relation_a, relation_b)
        result = self.run_sql(sql, fetch='one')

        self.assertEquals(
            result[0],
            0,
            sql
        )

    def _make_relation(self, identifier, schema=None, database=None):
        if schema is None:
            schema = self.unique_schema()
        if database is None:
            database = self.default_database
        return self.adapter.Relation.create(
            database=database,
            schema=schema,
            identifier=identifier,
            quote_policy=self.config.quoting
        )

    def get_many_relation_columns(self, relations):
        """Returns a dict of (datbase, schema) -> (dict of (table_name -> list of columns))
        """
        schema_fqns = {}
        for rel in relations:
            this_schema = schema_fqns.setdefault((rel.database, rel.schema), [])
            this_schema.append(rel.identifier)

        column_specs = {}
        for key, tables in schema_fqns.items():
            database, schema = key
            columns = self.get_many_table_columns(tables, schema, database=database)
            table_columns = {}
            for col in columns:
                table_columns.setdefault(col[0], []).append(col[1:])
            for rel_name, columns in table_columns.items():
                key = (database, schema, rel_name)
                column_specs[key] = columns

        return column_specs


    def assertManyRelationsEqual(self, relations, default_schema=None, default_database=None):
        if default_schema is None:
            default_schema = self.unique_schema()
        if default_database is None:
            default_database = self.default_database

        specs = []
        for relation in relations:
            if not isinstance(relation, (tuple, list)):
                relation = [relation]

            assert len(relation) <= 3

            if len(relation) == 3:
                relation = self._make_relation(*relation)
            elif len(relation) == 2:
                relation = self._make_relation(relation[0], relation[1], default_database)
            elif len(relation) == 1:
                relation = self._make_relation(relation[0], default_schema, default_database)
            else:
                raise ValueError('relation must be a sequence of 1, 2, or 3 values')

            specs.append(relation)

        column_specs = self.get_many_relation_columns(specs)

        # make sure everyone has equal column definitions
        first_columns = None
        for relation in specs:
            key = (relation.database, relation.schema, relation.identifier)
            columns = column_specs[key]
            if first_columns is None:
                first_columns = columns
            else:
                self.assertEqual(
                    first_columns, columns,
                    '{} did not match {}'.format(str(specs[0]), str(relation))
                )

        # make sure every one has the same number of rows in each column
        first_row_count = None
        query = ' union all '.join(
            'select count(*) as num_rows from {}'.format(r) for r in specs
        )
        table_row_counts = self.run_sql(query, fetch='all')
        for row in table_row_counts:
            if first_row_count is None:
                first_row_count = row[0]
            else:
                self.assertEqual(first_row_count, row[0])

        # make sure everyone has the same data. if we got here, everyone had
        # the same column specs!
        first_relation = None
        for relation in specs:
            if first_relation is None:
                first_relation = relation
            else:
                sql = self._assertTablesEqualSql(first_relation, relation,
                                                 columns=first_columns)
                result = self.run_sql(sql, fetch='one')
                self.assertEqual(result[0], 0, sql)


    def assertManyTablesEqual(self, *args):
        schema = self.unique_schema()
        database = self.default_database

        all_tables = []
        for table_equivalencies in args:
            all_tables += list(table_equivalencies)

        all_cols = self.get_table_columns_as_dict(all_tables, schema)

        for table_equivalencies in args:
            first_table = table_equivalencies[0]
            base_result = all_cols[first_table]

            for other_table in table_equivalencies[1:]:
                other_result = all_cols[other_table]

                self.assertEquals(base_result, other_result)
                first_relation = self._make_relation(first_table)
                other_relation = self._make_relation(other_table)

                self._assertTableRowCountsEqual(first_relation, other_relation)
                sql = self._assertTablesEqualSql(first_relation,
                                                 other_relation,
                                                 columns=base_result)
                result = self.run_sql(sql, fetch='one')

                self.assertEquals(
                    result[0],
                    0,
                    sql
                )

                self.assertTrue(len(base_result) > 0)
                self.assertTrue(len(other_result) > 0)

    def _assertTableRowCountsEqual(self, relation_a, relation_b):
        cmp_query = """
            with table_a as (

                select count(*) as num_rows from {}

            ), table_b as (

                select count(*) as num_rows from {}

            )

            select table_a.num_rows - table_b.num_rows as difference
            from table_a, table_b

        """.format(str(relation_a), str(relation_b))


        res = self.run_sql(cmp_query, fetch='one')

        self.assertEquals(int(res[0]), 0, "Row count of table {} doesn't match row count of table {}. ({} rows different)".format(
                relation_a.identifier,
                relation_b.identifier,
                res[0]
            )
        )

    def assertTableDoesNotExist(self, table, schema=None):
        columns = self.get_table_columns(table, schema)

        self.assertEquals(
            len(columns),
            0
        )

    def assertTableDoesExist(self, table, schema=None):
        columns = self.get_table_columns(table, schema)

        self.assertGreater(
            len(columns),
            0
        )

    def _assertTableColumnsEqual(self, relation_a, relation_b):
        table_a_result = self.get_relation_columns(relation_a)
        table_b_result = self.get_relation_columns(relation_b)

        text_types = {'text', 'character varying', 'character', 'varchar'}

        self.assertEqual(len(table_a_result), len(table_b_result))
        for a_column, b_column in zip(table_a_result, table_b_result):
            a_name, a_type, a_size = a_column
            b_name, b_type, b_size = b_column
            self.assertEqual(a_name, b_name,
                '{} vs {}: column "{}" != "{}"'.format(
                    relation_a, relation_b, a_name, b_name
                ))

            self.assertEqual(a_type, b_type,
                '{} vs {}: column "{}" has type "{}" != "{}"'.format(
                    relation_a, relation_b, a_name, a_type, b_type
                ))

            if self.adapter_type == 'presto' and None in (a_size, b_size):
                # None is compatible with any size
                continue

            self.assertEqual(a_size, b_size,
                '{} vs {}: column "{}" has size "{}" != "{}"'.format(
                    relation_a, relation_b, a_name, a_size, b_size
                ))

    def assertEquals(self, *args, **kwargs):
        # assertEquals is deprecated. This makes the warnings less chatty
        self.assertEqual(*args, **kwargs)

    def assertBetween(self, timestr, start, end=None):
        datefmt = '%Y-%m-%dT%H:%M:%S.%fZ'
        if end is None:
            end = datetime.utcnow()

        parsed = datetime.strptime(timestr, datefmt)

        self.assertLessEqual(start, parsed,
            'parsed date {} happened before {}'.format(
                parsed,
                start.strftime(datefmt))
        )
        self.assertGreaterEqual(end, parsed,
            'parsed date {} happened after {}'.format(
                parsed,
                end.strftime(datefmt))
        )

def use_profile(profile_name):
    """A decorator to declare a test method as using a particular profile.
    Handles both setting the nose attr and calling self.use_profile.

    Use like this:

    class TestSomething(DBIntegrationTest):
        @use_profile('postgres')
        def test_postgres_thing(self):
            self.assertEqual(self.adapter_type, 'postgres')

        @use_profile('snowflake')
        def test_snowflake_thing(self):
            self.assertEqual(self.adapter_type, 'snowflake')
    """
    def outer(wrapped):
        @attr(type=profile_name)
        @wraps(wrapped)
        def func(self, *args, **kwargs):
            return wrapped(self, *args, **kwargs)
        # sanity check at import time
        assert _profile_from_test_name(wrapped.__name__) == profile_name
        return func
    return outer


class AnyFloat(object):
    """Any float. Use this in assertEqual() calls to assert that it is a float.
    """
    def __eq__(self, other):
        return isinstance(other, float)


class AnyStringWith(object):
    def __init__(self, contains=None):
        self.contains = contains

    def __eq__(self, other):
        if not isinstance(other, basestring):
            return False

        if self.contains is None:
            return True

        return self.contains in other

    def __repr__(self):
        return 'AnyStringWith<{!r}>'.format(self.contains)
