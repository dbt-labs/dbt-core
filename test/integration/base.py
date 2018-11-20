import unittest
import dbt.main as dbt
import os, shutil
import yaml
import random
import time
import json
from functools import wraps

from nose.plugins.attrib import attr

import dbt.flags as flags

from dbt.adapters.factory import get_adapter, reset_adapters
from dbt.clients.jinja import template_cache
from dbt.config import RuntimeConfig

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
        self.__dict__.update(kwargs)


def _profile_from_test_name(test_name):
    adapter_names = ('postgres', 'snowflake', 'redshift', 'bigquery')
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

    prefix = "test{}{:04}".format(int(time.time()), random.randint(0, 9999))

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
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                    'noaccess': {
                        'type': 'postgres',
                        'threads': 4,
                        'host': 'database',
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

    @property
    def packages_config(self):
        return None

    def unique_schema(self):
        schema = self.schema

        to_return = "{}_{}".format(self.prefix, schema)

        if self.adapter_type == 'snowflake':
            return to_return.upper()

        return to_return.lower()

    def get_profile(self, adapter_type):
        if adapter_type == 'postgres':
            return self.postgres_profile()
        elif adapter_type == 'snowflake':
            return self.snowflake_profile()
        elif adapter_type == 'bigquery':
            return self.bigquery_profile()
        elif adapter_type == 'redshift':
            return self.redshift_profile()
        else:
            raise ValueError('invalid adapter type {}'.format(adapter_type))

    def _pick_profile(self):
        test_name = self.id().split('.')[-1]
        return _profile_from_test_name(test_name)

    def setUp(self):
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
        connection = adapter.acquire_connection('__test')
        self.handle = connection.handle
        self.adapter_type = connection.type
        self.adapter = adapter
        self.config = config

        self._drop_schema()
        self._create_schema()

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

        if not hasattr(self, 'adapter'):
            self.adapter = get_adapter(self.config)

        self._drop_schema()

        # hack for BQ -- TODO
        if hasattr(self.handle, 'close'):
            self.handle.close()

        self.adapter.cleanup_connections()
        reset_adapters()

    def _create_schema(self):
        if self.adapter_type == 'bigquery':
            self.adapter.create_schema(self.unique_schema(), '__test')
        else:
            schema = self.quote_as_configured(self.unique_schema(), 'schema')
            self.run_sql('CREATE SCHEMA {}'.format(schema))
            self._created_schema = schema

    def _drop_schema(self):
        if self.adapter_type == 'bigquery':
            self.adapter.drop_schema(self.unique_schema(), '__test')
        else:
            had_existing = False
            try:
                schema = self._created_schema
                had_existing = True
            except AttributeError:
                # we never created it, we think. This can be wrong if a test creates
                # its own schemas that don't match the configured quoting strategy
                schema = self.quote_as_configured(self.unique_schema(), 'schema')
            self.run_sql('DROP SCHEMA IF EXISTS {} CASCADE'.format(schema))
            if had_existing:
                # avoid repeatedly deleting the wrong schema
                del self._created_schema

    @property
    def project_config(self):
        return {}

    @property
    def profile_config(self):
        return {}

    def run_dbt(self, args=None, expect_pass=True, strict=True, clear_adapters=True):
        # clear the adapter cache
        if clear_adapters:
            reset_adapters()
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

    def run_sql_file(self, path):
        with open(path, 'r') as f:
            statements = f.read().split(";")
            for statement in statements:
                self.run_sql(statement)

    # horrible hack to support snowflake for right now
    def transform_sql(self, query):
        to_return = query

        if self.adapter_type == 'snowflake':
            to_return = to_return.replace("BIGSERIAL", "BIGINT AUTOINCREMENT")

        to_return = to_return.format(schema=self.unique_schema())

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

    def run_sql(self, query, fetch='None'):
        if query.strip() == "":
            return

        sql = self.transform_sql(query)
        if self.adapter_type == 'bigquery':
            return self.run_sql_bigquery(sql, fetch)

        with self.handle.cursor() as cursor:
            try:
                cursor.execute(sql)
                self.handle.commit()
                if fetch == 'one':
                    return cursor.fetchone()
                elif fetch == 'all':
                    return cursor.fetchall()
                else:
                    return
            except BaseException as e:
                self.handle.rollback()
                print(query)
                print(e)
                raise e

    def get_many_table_columns(self, tables, schema):
        sql = """
                select table_name, column_name, data_type, character_maximum_length
                from information_schema.columns
                where table_schema ilike '{schema_filter}'
                  and ({table_filter})
                order by column_name asc"""


        table_filters = ["table_name ilike '{}'".format(table.replace('"', '')) for table in tables]
        table_filters_s = " OR ".join(table_filters)

        sql = sql.format(
                schema_filter=schema,
                table_filter=table_filters_s)

        columns = self.run_sql(sql, fetch='all')
        return sorted(map(self.filter_many_columns, columns),
                      key=lambda x: "{}.{}".format(x[0], x[1]))

    def filter_many_columns(self, column):
        table_name, column_name, data_type, char_size = column
        # in snowflake, all varchar widths are created equal
        if self.adapter_type == 'snowflake':
            if char_size and char_size < 16777216:
                char_size = 16777216
        return (table_name, column_name, data_type, char_size)

    def get_table_columns(self, table, schema=None):
        schema = self.unique_schema() if schema is None else schema
        columns = self.adapter.get_columns_in_table(schema, table)

        return sorted(((c.name, c.dtype, c.char_size) for c in columns),
                      key=lambda x: x[0])

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
                where table_schema ilike '{}'
                order by table_name
                """

        result = self.run_sql(sql.format(schema), fetch='all')

        return {model_name: materialization for (model_name, materialization) in result}

    def _assertTablesEqualSql(self, table_a_schema, table_a, table_b_schema, table_b, columns=None):
        if columns is None:
            columns = self.get_table_columns(table_a, table_a_schema)

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
                (SELECT {columns} FROM {table_a_schema}.{table_a} {except_op}
                 SELECT {columns} FROM {table_b_schema}.{table_b})
                 UNION ALL
                (SELECT {columns} FROM {table_b_schema}.{table_b} {except_op}
                 SELECT {columns} FROM {table_a_schema}.{table_a})
            ) AS a""".format(
                columns=columns_csv,
                table_a_schema=self.quote_as_configured(table_a_schema, 'schema'),
                table_b_schema=self.quote_as_configured(table_b_schema, 'schema'),
                table_a=self.quote_as_configured(table_a, 'identifier'),
                table_b=self.quote_as_configured(table_b, 'identifier'),
                except_op=except_operator
            )

        return sql

    def assertTablesEqual(self, table_a, table_b,
                          table_a_schema=None, table_b_schema=None):
        table_a_schema = self.unique_schema() \
                         if table_a_schema is None else table_a_schema

        table_b_schema = self.unique_schema() \
                         if table_b_schema is None else table_b_schema

        self.assertTableColumnsEqual(table_a, table_b,
                                     table_a_schema, table_b_schema)
        self.assertTableRowCountsEqual(table_a, table_b,
                                       table_a_schema, table_b_schema)

        sql = self._assertTablesEqualSql(table_a_schema, table_a,
                                         table_b_schema, table_b)
        result = self.run_sql(sql, fetch='one')

        self.assertEquals(
            result[0],
            0,
            sql
        )

    def assertManyTablesEqual(self, *args):
        schema = self.unique_schema()

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

                self.assertTableRowCountsEqual(first_table, other_table)
                sql = self._assertTablesEqualSql(schema, first_table,
                                                 schema, other_table,
                                                 columns=base_result)
                result = self.run_sql(sql, fetch='one')

                self.assertEquals(
                    result[0],
                    0,
                    sql
                )

                self.assertTrue(len(base_result) > 0)
                self.assertTrue(len(other_result) > 0)

    def assertTableRowCountsEqual(self, table_a, table_b,
                                  table_a_schema=None, table_b_schema=None):
        table_a_schema = self.unique_schema() \
                         if table_a_schema is None else table_a_schema

        table_b_schema = self.unique_schema() \
                         if table_b_schema is None else table_b_schema

        cmp_query = """
            with table_a as (

                select count(*) as num_rows from {}.{}

            ), table_b as (

                select count(*) as num_rows from {}.{}

            )

            select table_a.num_rows - table_b.num_rows as difference
            from table_a, table_b

        """.format(self.quote_as_configured(table_a_schema, 'schema'),
                   self.quote_as_configured(table_a, 'identifier'),
                   self.quote_as_configured(table_b_schema, 'schema'),
                   self.quote_as_configured(table_b, 'identifier'))


        res = self.run_sql(cmp_query, fetch='one')

        self.assertEquals(int(res[0]), 0, "Row count of table {} doesn't match row count of table {}. ({} rows different)".format(
                table_a,
                table_b,
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

    def assertTableColumnsEqual(self, table_a, table_b, table_a_schema=None, table_b_schema=None):
        table_a_schema = self.unique_schema() if table_a_schema is None else table_a_schema
        table_b_schema = self.unique_schema() if table_b_schema is None else table_b_schema

        table_a_result = self.get_table_columns(table_a, table_a_schema)
        table_b_result = self.get_table_columns(table_b, table_b_schema)

        self.assertEquals(
            table_a_result,
            table_b_result
        )

    def assertEquals(self, *args, **kwargs):
        # assertEquals is deprecated. This makes the warnings less chatty
        self.assertEqual(*args, **kwargs)


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
