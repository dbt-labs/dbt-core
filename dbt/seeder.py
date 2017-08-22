from csvkit import table as csv_table, sql as csv_sql
from dbt.adapters.factory import get_adapter
import psycopg2

from dbt.source import Source
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.exceptions


class Seeder:
    def __init__(self, project):
        self.project = project

    def find_csvs(self):
        return Source(self.project).get_csvs(self.project['data-paths'])

    def create_table(self, cursor, schema, table, virtual_table):
        sql_table = csv_sql.make_table(virtual_table, db_schema=schema)
        create_table_sql = csv_sql.make_create_table_statement(
            sql_table, dialect='postgresql'
        )
        logger.info("Creating table {}.{}".format(schema, table))
        cursor.execute(create_table_sql)

    def insert_into_table(self, cursor, schema, table, virtual_table):
        headers = virtual_table.headers()

        header_csv = ", ".join(['"{}"'.format(h) for h in headers])
        base_insert = ('INSERT INTO "{schema}"."{table}" ({header_csv}) '
                       'VALUES '.format(
                           schema=schema,
                           table=table,
                           header_csv=header_csv
                       ))
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
        logger.info("Inserting {} records into table {}.{}"
                    .format(len(virtual_table.to_rows()), schema, table))
        cursor.execute(insert_sql)

    def do_seed(self, schema, cursor, drop_existing):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        existing = adapter.query_for_existing(profile, schema)

        csvs = self.find_csvs()
        statuses = []
        for csv in csvs:
            table_name = csv.name
            fh = open(csv.filepath)
            virtual_table = csv_table.Table.from_csv(fh, table_name)

            if table_name in existing:
                if drop_existing:
                    adapter.drop(profile, table_name, existing[table_name])
                    self.create_table(
                        cursor,
                        schema,
                        table_name,
                        virtual_table
                    )
                else:
                    adapter.truncate(profile, table_name, table_name)
            else:
                self.create_table(cursor, schema, table_name, virtual_table)

            try:
                self.insert_into_table(
                    cursor, schema, table_name, virtual_table
                )
                statuses.append(True)

            except psycopg2.ProgrammingError as e:
                statuses.append(False)
                logger.info(
                    'Encountered an error while inserting into table "{}"."{}"'
                    .format(schema, table_name)
                )
                logger.info(
                    'Check for formatting errors in {}'.format(csv.filepath)
                )
                logger.info(
                    'Try --drop-existing to delete and recreate the table '
                    'instead'
                )
                logger.info(str(e))
        return all(statuses)

    def seed(self, drop_existing=False):
        profile = self.project.run_environment()

        if profile.get('type') not in 'snowflake':
            raise dbt.exceptions.NotImplementedException(
                "`seed` operation is not supported for snowflake.")

        adapter = get_adapter(profile)
        connection = adapter.get_connection(profile)

        schema = connection.get('credentials', {}).get('schema')

        with connection.get('handle') as handle:
            with handle.cursor() as cursor:
                return self.do_seed(schema, cursor, drop_existing)
