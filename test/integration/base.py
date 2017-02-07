import unittest
import dbt.main as dbt
import os, shutil
import yaml
import time

from dbt.adapters.factory import get_adapter

DBT_CONFIG_DIR = os.path.expanduser(os.environ.get("DBT_CONFIG_DIR", '/root/.dbt'))
DBT_PROFILES = os.path.join(DBT_CONFIG_DIR, 'profiles.yml')

class DBTIntegrationTest(unittest.TestCase):

    def postgres_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.schema
                    },
                    'noaccess': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': 'database',
                        'port': 5432,
                        'user': 'noaccess',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.schema
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
                        'threads': 1,
                        'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
                        'user': os.getenv('SNOWFLAKE_TEST_USER'),
                        'password': os.getenv('SNOWFLAKE_TEST_PASSWORD'),
                        'database': os.getenv('SNOWFLAKE_TEST_DATABASE'),
                        'schema': self.schema,
                        'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
                    },
                    'noaccess': {
                        'type': 'snowflake',
                        'threads': 1,
                        'account': os.getenv('SNOWFLAKE_TEST_ACCOUNT'),
                        'user': 'noaccess',
                        'password': 'password',
                        'database': os.getenv('SNOWFLAKE_TEST_DATABASE'),
                        'schema': self.schema,
                        'warehouse': os.getenv('SNOWFLAKE_TEST_WAREHOUSE'),
                    }
                },
                'target': 'default2'
            }
        }

    def get_profile(self, adapter_type):
        if adapter_type == 'postgres':
            return self.postgres_profile()
        elif adapter_type == 'snowflake':
            return self.snowflake_profile()

    def setUp(self):
        # create a dbt_project.yml

        base_project_config = {
            'name': 'test',
            'version': '1.0',
            'test-paths': [],
            'source-paths': [self.models],
            'profile': 'test'
        }

        project_config = {}
        project_config.update(base_project_config)
        project_config.update(self.project_config)

        with open("dbt_project.yml", 'w') as f:
            yaml.safe_dump(project_config, f, default_flow_style=True)

        # create profiles

        profile_config = {}
        default_profile_config = self.postgres_profile()
        profile_config.update(default_profile_config)
        profile_config.update(self.profile_config)

        if not os.path.exists(DBT_CONFIG_DIR):
            os.makedirs(DBT_CONFIG_DIR)

        with open(DBT_PROFILES, 'w') as f:
            yaml.safe_dump(profile_config, f, default_flow_style=True)

        target = profile_config.get('test').get('target')

        if target is None:
            target = profile_config.get('test').get('run-target')

        profile = profile_config.get('test').get('outputs').get(target)

        adapter = get_adapter(profile)

        # it's important to use a different connection handle here so
        # we don't look into an incomplete transaction
        connection = adapter.acquire_connection(profile)
        self.handle = connection.get('handle')
        self.adapter_type = profile.get('type')

        self.run_sql('DROP SCHEMA IF EXISTS "{}" CASCADE'.format(self.schema))
        self.run_sql('CREATE SCHEMA "{}"'.format(self.schema))

    def use_default_project(self):
        # create a dbt_project.yml
        base_project_config = {
            'name': 'test',
            'version': '1.0',
            'test-paths': [],
            'source-paths': [self.models],
            'profile': 'test'
        }

        project_config = {}
        project_config.update(base_project_config)
        project_config.update(self.project_config)

        with open("dbt_project.yml", 'w') as f:
            yaml.safe_dump(project_config, f, default_flow_style=True)

    def use_profile(self, adapter_type):
        profile_config = {}
        default_profile_config = self.get_profile(adapter_type)

        profile_config.update(default_profile_config)
        profile_config.update(self.profile_config)

        if not os.path.exists(DBT_CONFIG_DIR):
            os.makedirs(DBT_CONFIG_DIR)

        with open(DBT_PROFILES, 'w') as f:
            yaml.safe_dump(profile_config, f, default_flow_style=True)

        profile = profile_config.get('test').get('outputs').get('default2')
        adapter = get_adapter(profile)

        # it's important to use a different connection handle here so
        # we don't look into an incomplete transaction
        connection = adapter.acquire_connection(profile)
        self.handle = connection.get('handle')
        self.adapter_type = profile.get('type')

        self.run_sql('DROP SCHEMA IF EXISTS "{}" CASCADE'.format(self.schema))
        self.run_sql('CREATE SCHEMA "{}"'.format(self.schema))

    def tearDown(self):
        os.remove(DBT_PROFILES)
        os.remove("dbt_project.yml")

        # quick fix for windows bug that prevents us from deleting dbt_modules
        try:
            if os.path.exists('dbt_modules'):
                shutil.rmtree('dbt_modules')
        except:
            os.rename("dbt_modules", "dbt_modules-{}".format(time.time()))

    @property
    def project_config(self):
        return {}

    @property
    def profile_config(self):
        return {}

    def run_dbt(self, args=None):
        if args is None:
            args = ["run"]

        args = ["--strict"] + args

        return dbt.handle(args)

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

        return to_return

    def run_sql(self, query, fetch='None'):
        if query.strip() == "":
            return

        with self.handle.cursor() as cursor:
            try:
                cursor.execute(self.transform_sql(query))
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

    def get_table_columns(self, table):
        sql = """
                select column_name, data_type, character_maximum_length
                from information_schema.columns
                where table_name = '{}'
                and table_schema = '{}'
                order by column_name asc"""

        result = self.run_sql(sql.format(table, self.schema), fetch='all')

        return result

    def get_models_in_schema(self):
        sql = """
                select table_name,
                        case when table_type = 'BASE TABLE' then 'table'
                             when table_type = 'VIEW' then 'view'
                             else table_type
                        end as materialization
                from information_schema.tables
                where table_schema = '{}'
                order by table_name
                """

        result = self.run_sql(sql.format(self.schema), fetch='all')

        return {model_name: materialization for (model_name, materialization) in result}

    def assertTablesEqual(self, table_a, table_b):
        self.assertTableColumnsEqual(table_a, table_b)
        self.assertTableRowCountsEqual(table_a, table_b)

        columns = self.get_table_columns(table_a)
        columns_csv = ", ".join(['"{}"'.format(record[0])
                                 for record in columns])

        table_sql = "SELECT {} FROM {}"

        sql = """
            SELECT COUNT(*) FROM (
                (SELECT {columns} FROM "{schema}"."{table_a}" EXCEPT
                 SELECT {columns} FROM "{schema}"."{table_b}")
                 UNION ALL
                (SELECT {columns} FROM "{schema}"."{table_b}" EXCEPT
                 SELECT {columns} FROM "{schema}"."{table_a}")
            ) AS a""".format(
                columns=columns_csv,
                schema=self.schema,
                table_a=table_a,
                table_b=table_b
            )

        result = self.run_sql(sql, fetch='one')

        self.assertEquals(
            result[0],
            0,
            sql
        )

    def assertTableRowCountsEqual(self, table_a, table_b):
        table_a_result = self.run_sql('SELECT COUNT(*) FROM "{}"."{}"'.format(self.schema, table_a), fetch='one')
        table_b_result = self.run_sql('SELECT COUNT(*) FROM "{}"."{}"'.format(self.schema, table_b), fetch='one')

        self.assertEquals(
            table_a_result[0],
            table_b_result[0],
            "Row count of table {} ({}) doesn't match row count of table {} ({})".format(
                table_a,
                table_a_result[0],
                table_b,
                table_b_result[0]
            )
        )


    def assertTableColumnsEqual(self, table_a, table_b):
        table_a_result = self.get_table_columns(table_a)
        table_b_result = self.get_table_columns(table_b)

        self.assertEquals(
            table_a_result,
            table_b_result
        )
