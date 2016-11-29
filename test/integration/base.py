import unittest
import dbt.main as dbt
import os, shutil
import yaml
import time

from test.integration.connection import handle

DBT_CONFIG_DIR = os.path.expanduser(os.environ.get("DBT_CONFIG_DIR", '/root/.dbt'))
DBT_PROFILES = os.path.join(DBT_CONFIG_DIR, 'profiles.yml')

class DBTIntegrationTest(unittest.TestCase):

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

        profile_config = {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': self.schema
                    }
                },
                'run-target': 'default'
            }
        }

        if not os.path.exists(DBT_CONFIG_DIR):
            os.makedirs(DBT_CONFIG_DIR)

        with open(DBT_PROFILES, 'w') as f:
            yaml.safe_dump(profile_config, f, default_flow_style=True)

        self.run_sql("DROP SCHEMA IF EXISTS {} CASCADE;".format(self.schema))
        self.run_sql("CREATE SCHEMA {};".format(self.schema))

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

    def run_dbt(self, args=None):
        if args is None:
            args = ["run"]

        dbt.handle(args)

    def run_sql_file(self, path):
        with open(path, 'r') as f:
            return self.run_sql(f.read())

    def run_sql(self, query, fetch='all'):
        with handle.cursor() as cursor:
            try:
                cursor.execute(query)
                handle.commit()
                if fetch == 'one':
                    output = cursor.fetchone()
                else:
                    output = cursor.fetchall()
                return output
            except BaseException as e:
                handle.rollback()
                print(e)

    def get_table_columns(self, table):
        sql = """
                select column_name, data_type, character_maximum_length
                from information_schema.columns
                where table_name = '{}'
                and table_schema = '{}'
                order by column_name asc"""

        result = self.run_sql(sql.format(table, self.schema))

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

        result = self.run_sql(sql.format(self.schema))

        return {model_name: materialization for (model_name, materialization) in result}

    def assertTablesEqual(self, table_a, table_b):
        self.assertTableColumnsEqual(table_a, table_b)
        self.assertTableRowCountsEqual(table_a, table_b)

        columns = self.get_table_columns(table_a)
        columns_csv = ", ".join([record[0] for record in columns])

        table_sql = "SELECT {} FROM {}"

        sql = """
            SELECT COUNT(*) FROM (
                (SELECT {columns} FROM {schema}.{table_a} EXCEPT SELECT {columns} FROM {schema}.{table_b})
                 UNION ALL
                (SELECT {columns} FROM {schema}.{table_b} EXCEPT SELECT {columns} FROM {schema}.{table_a})
            ) AS _""".format(
                columns=columns_csv,
                schema=self.schema,
                table_a=table_a,
                table_b=table_b
            )

        result = self.run_sql(sql, fetch='one')

        self.assertEquals(
            result[0],
            0,
            "{} rows had mismatches."
        )

    def assertTableRowCountsEqual(self, table_a, table_b):
        table_a_result = self.run_sql("SELECT COUNT(*) FROM {}.{}".format(self.schema, table_a), fetch='one')
        table_b_result = self.run_sql("SELECT COUNT(*) FROM {}.{}".format(self.schema, table_b), fetch='one')

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
