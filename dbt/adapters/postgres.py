import psycopg2

from contextlib import contextmanager

import dbt.adapters.default
import dbt.exceptions

from dbt.logger import GLOBAL_LOGGER as logger


RELATION_PERMISSION_DENIED_MESSAGE = """
The user '{user}' does not have sufficient permissions to create the model
'{model}' in the schema '{schema}'. Please adjust the permissions of the
'{user}' user on the '{schema}' schema. With a superuser account, execute the
following commands, then re-run dbt.

grant usage, create on schema "{schema}" to "{user}";
grant select, insert, delete on all tables in schema "{schema}" to "{user}";"""

RELATION_NOT_OWNER_MESSAGE = """
The user '{user}' does not have sufficient permissions to drop the model
'{model}' in the schema '{schema}'. This is likely because the relation was
created by a different user. Either delete the model "{schema}"."{model}"
manually, or adjust the permissions of the '{user}' user in the '{schema}'
schema."""


class PostgresAdapter(dbt.adapters.default.DefaultAdapter):

    @classmethod
    @contextmanager
    def exception_handler(cls, profile, sql, model_name=None,
                          connection_name=None):
        connection = cls.get_connection(profile, connection_name)
        schema = connection.get('credentials', {}).get('schema')

        try:
            yield
        except psycopg2.ProgrammingError as e:
            logger.debug('Postgres error: {}'.format(str(e)))

            cls.rollback(connection)
            error_data = {
                "model": model_name,
                "schema": schema,
                "user": connection.get('credentials', {}).get('user')
            }

            if 'must be owner of relation' in e.diag.message_primary:
                raise RuntimeError(
                    RELATION_NOT_OWNER_MESSAGE.format(**error_data))
            elif "permission denied for" in e.diag.message_primary:
                raise RuntimeError(
                    RELATION_PERMISSION_DENIED_MESSAGE.format(**error_data))
            else:
                raise e
        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            cls.rollback(connection)
            raise e

    @classmethod
    def type(cls):
        return 'postgres'

    @classmethod
    def date_function(cls):
        return 'datenow()'

    @classmethod
    def dist_qualifier(cls, dist):
        return ''

    @classmethod
    def sort_qualifier(cls, sort_type, sort):
        return ''

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
    def query_for_existing(cls, profile, schema, model_name=None):
        sql = """
        select tablename as name, 'table' as type from pg_tables
        where schemaname = '{schema}'
        union all
        select viewname as name, 'view' as type from pg_views
        where schemaname = '{schema}'
        """.format(schema=schema).strip()  # noqa

        connection, cursor = cls.add_query(profile, sql, model_name)

        results = cursor.fetchall()

        existing = [(name, relation_type) for (name, relation_type) in results]

        return dict(existing)
