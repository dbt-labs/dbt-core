import psycopg2

from contextlib import contextmanager

import dbt.adapters.default
import dbt.compat
import dbt.exceptions
from dbt.utils import max_digits

from dbt.logger import GLOBAL_LOGGER as logger


class PostgresAdapter(dbt.adapters.default.DefaultAdapter):

    @classmethod
    @contextmanager
    def exception_handler(cls, profile, sql, model_name=None,
                          connection_name=None):
        connection = cls.get_connection(profile, connection_name)

        try:
            yield

        except psycopg2.DatabaseError as e:
            logger.debug('Postgres error: {}'.format(str(e)))

            cls.release_connection(profile, connection_name)
            raise dbt.exceptions.DatabaseException(
                dbt.compat.to_string(e).strip())

        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            cls.release_connection(profile, connection_name)
            raise dbt.exceptions.RuntimeException(e)

    @classmethod
    def type(cls):
        return 'postgres'

    @classmethod
    def date_function(cls):
        return 'datenow()'

    @classmethod
    def get_status(cls, cursor):
        return cursor.statusmessage

    @classmethod
    def open_connection(cls, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()

        try:
            credentials = connection.get('credentials', {})
            handle = psycopg2.connect(
                dbname=credentials.get('dbname'),
                user=credentials.get('user'),
                host=credentials.get('host'),
                password=credentials.get('pass'),
                port=credentials.get('port'),
                connect_timeout=10)

            result['handle'] = handle
            result['state'] = 'open'
        except psycopg2.Error as e:
            logger.debug("Got an error when attempting to open a postgres "
                         "connection: '{}'"
                         .format(e))

            result['handle'] = None
            result['state'] = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return result

    @classmethod
    def alter_column_type(cls, profile, schema, table, column_name,
                          new_column_type, model_name=None):
        """
        1. Create a new column (w/ temp name and correct type)
        2. Copy data over to it
        3. Drop the existing column (cascade!)
        4. Rename the new column to existing column
        """

        opts = {
            "schema": schema,
            "table": table,
            "old_column": column_name,
            "tmp_column": "{}__dbt_alter".format(column_name),
            "dtype": new_column_type
        }

        sql = """
        alter table "{schema}"."{table}" add column "{tmp_column}" {dtype};
        update "{schema}"."{table}" set "{tmp_column}" = "{old_column}";
        alter table "{schema}"."{table}" drop column "{old_column}" cascade;
        alter table "{schema}"."{table}" rename column "{tmp_column}" to "{old_column}";
        """.format(**opts).strip()  # noqa

        connection, cursor = cls.add_query(profile, sql, model_name)

        return connection, cursor

    @classmethod
    def query_for_existing(cls, profile, schemas, model_name=None):
        if not isinstance(schemas, (list, tuple)):
            schemas = [schemas]

        schema_list = ",".join(["'{}'".format(schema) for schema in schemas])

        sql = """
        select tablename as name, 'table' as type from pg_tables
        where schemaname in ({schema_list})
        union all
        select viewname as name, 'view' as type from pg_views
        where schemaname in ({schema_list})
        """.format(schema_list=schema_list).strip()  # noqa

        connection, cursor = cls.add_query(profile, sql, model_name,
                                           auto_begin=False)

        results = cursor.fetchall()

        existing = [(name, relation_type) for (name, relation_type) in results]

        return dict(existing)

    @classmethod
    def get_existing_schemas(cls, profile, model_name=None):
        sql = "select distinct nspname from pg_namespace"

        connection, cursor = cls.add_query(profile, sql, model_name,
                                           auto_begin=False)
        results = cursor.fetchall()

        return [row[0] for row in results]

    @classmethod
    def check_schema_exists(cls, profile, schema, model_name=None):
        sql = """
        select count(*) from pg_namespace where nspname = '{schema}'
        """.format(schema=schema).strip()  # noqa

        connection, cursor = cls.add_query(profile, sql, model_name,
                                           auto_begin=False)
        results = cursor.fetchone()

        return results[0] > 0

    @classmethod
    def cancel_connection(cls, profile, connection):
        connection_name = connection.get('name')
        pid = connection.get('handle').get_backend_pid()

        sql = "select pg_terminate_backend({})".format(pid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, pid))

        _, cursor = cls.add_query(profile, sql, 'master')
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "text"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        import agate
        column = agate_table.columns[col_idx]
        precision = max_digits(column.values_without_nulls())
        # agate uses the term Precision but in this context, it is really the
        # scale - ie. the number of decimal places
        scale = agate_table.aggregate(agate.MaxPrecision(col_idx))
        if not scale:
            return "integer"
        return "numeric({}, {})".format(precision, scale)

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "boolean"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "timestamp without time zone"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"

    @classmethod
    def create_csv_table(cls, profile, schema, table_name, agate_table):
        col_sqls = []
        for idx, col_name in enumerate(agate_table.column_names):
            type_ = cls.convert_agate_type(agate_table, idx)
            col_sqls.append('{} {}'.format(col_name, type_))
        sql = 'create table "{}"."{}" ({})'.format(schema, table_name,
                                                   ", ".join(col_sqls))
        return cls.add_query(profile, sql)

    @classmethod
    def reset_csv_table(cls, profile, schema, table_name, agate_table,
                        full_refresh=False):
        if full_refresh:
            cls.drop_table(profile, schema, table_name, None)
            cls.create_csv_table(profile, schema, table_name, agate_table)
        else:
            cls.truncate(profile, schema, table_name)

    @classmethod
    def load_csv_rows(cls, profile, schema, table_name, agate_table):
        bindings = []
        placeholders = []
        cols_sql = ", ".join(c for c in agate_table.column_names)

        for row in agate_table.rows:
            bindings += row
            placeholders.append("({})".format(
                ", ".join("%s" for _ in agate_table.column_names)))

        sql = ('insert into {}.{} ({}) values {}'
               .format(cls.quote(schema),
                       cls.quote(table_name),
                       cols_sql,
                       ",\n".join(placeholders)))

        cls.add_query(profile, sql, bindings=bindings)
