
import os
import fnmatch
from csvkit import table as csv_table, sql as csv_sql
from sqlalchemy.dialects import postgresql as postgresql_dialect
import psycopg2

from dbt.source import Source
import dbt.targets

class Seeder:
    def __init__(self, project):
        self.project = project
        run_environment = self.project.run_environment()
        self.target = dbt.targets.get_target(run_environment)

    def find_csvs(self):
        return Source(self.project).get_csvs(self.project['data-paths'])

    def drop_table(self, cursor, schema, table):
        sql = 'drop table if exists "{schema}"."{table}" cascade'.format(schema=schema, table=table)
        print("Dropping table {}.{}".format(schema, table))
        cursor.execute(sql)

    def truncate_table(self, cursor, schema, table):
        sql = 'truncate table "{schema}"."{table}"'.format(schema=schema, table=table)
        print("Truncating table {}.{}".format(schema, table))
        cursor.execute(sql)

    def create_table(self, cursor, schema, table, virtual_table):
        sql_table = csv_sql.make_table(virtual_table, db_schema=schema)
        create_table_sql = csv_sql.make_create_table_statement(sql_table, dialect='postgresql')
        print("Creating table {}.{}".format(schema, table))
        cursor.execute(create_table_sql)

    def insert_into_table(self, cursor, schema, table, virtual_table):
        headers = virtual_table.headers()

        header_csv = ", ".join(['"{}"'.format(h) for h in headers])
        base_insert = 'INSERT INTO "{schema}"."{table}" ({header_csv}) VALUES '.format(schema=schema, table=table, header_csv=header_csv)
        records = []

        def quote_or_null(s):
            if s is None:
                return 'null'
            else:
                return "'{}'".format(s)

        for row in virtual_table.to_rows():
          record_csv = ', '.join([quote_or_null(val) for val in row])
          record_csv_wrapped = "({})".format(record_csv)
          records.append(record_csv_wrapped)
        insert_sql = "{} {}".format(base_insert, ",\n".join(records))
        print("Inserting {} records into table {}.{}".format(len(virtual_table.to_rows()), schema, table))
        cursor.execute(insert_sql)

    def existing_tables(self, cursor, schema):
        sql = "select tablename as name from pg_tables where schemaname = '{schema}'".format(schema=schema)

        cursor.execute(sql)
        existing = set([row[0] for row in cursor.fetchall()])
        return existing
    
    def do_seed(self, schema, cursor, drop_existing):
        existing_tables = self.existing_tables(cursor, schema)

        csvs = self.find_csvs()
        for csv in csvs:

            table_name = csv.name
            fh = open(csv.filepath)
            virtual_table = csv_table.Table.from_csv(fh, table_name)

            if table_name in existing_tables:
                if drop_existing:
                    self.drop_table(cursor, schema, table_name)
                    self.create_table(cursor, schema, table_name, virtual_table)
                else:
                    self.truncate_table(cursor, schema, table_name)
            else:
                self.create_table(cursor, schema, table_name, virtual_table)

            try:
                self.insert_into_table(cursor, schema, table_name, virtual_table)
            except psycopg2.ProgrammingError as e:
                print('Encountered an error while inserting into table "{}"."{}"'.format(schema, table_name))
                print('Check for formatting errors in {}'.format(csv.filepath))
                print('Try --drop-existing to delete and recreate the table instead')
                print(str(e))


    def seed(self, drop_existing=False):
        schema = self.target.schema

        with self.target.get_handle() as handle:
            with handle.cursor() as cursor:
                self.do_seed(schema, cursor, drop_existing)
