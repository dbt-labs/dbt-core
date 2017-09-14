from __future__ import absolute_import

import re

import snowflake.connector
import snowflake.connector.errors

from contextlib import contextmanager

import dbt.compat
import dbt.exceptions
import dbt.flags as flags

from dbt.adapters.postgres import PostgresAdapter
from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger


class SnowflakeAdapter(PostgresAdapter):

    @classmethod
    @contextmanager
    def exception_handler(cls, profile, sql, model_name=None,
                          connection_name='master'):
        connection = cls.get_connection(profile, connection_name)

        try:
            yield
        except snowflake.connector.errors.ProgrammingError as e:
            msg = dbt.compat.to_string(e)

            logger.debug('Snowflake error: {}'.format(msg))

            if 'Empty SQL statement' in msg:
                logger.debug("got empty sql statement, moving on")
            elif 'This session does not have a current database' in msg:
                cls.release_connection(profile, connection_name)
                raise dbt.exceptions.FailedToConnectException(
                    ('{}\n\nThis error sometimes occurs when invalid '
                     'credentials are provided, or when your default role '
                     'does not have access to use the specified database. '
                     'Please double check your profile and try again.')
                    .format(msg))
            else:
                cls.release_connection(profile, connection_name)
                raise dbt.exceptions.DatabaseException(msg)
        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            cls.release_connection(profile, connection_name)
            raise dbt.exceptions.RuntimeException(e.msg)

    @classmethod
    def type(cls):
        return 'snowflake'

    @classmethod
    def date_function(cls):
        return 'CURRENT_TIMESTAMP()'

    @classmethod
    def get_status(cls, cursor):
        state = cursor.sqlstate

        if state is None:
            state = 'SUCCESS'

        return "{} {}".format(state, cursor.rowcount)

    @classmethod
    def open_connection(cls, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()

        try:
            credentials = connection.get('credentials', {})
            handle = snowflake.connector.connect(
                account=credentials.get('account'),
                user=credentials.get('user'),
                password=credentials.get('password'),
                database=credentials.get('database'),
                schema=credentials.get('schema'),
                warehouse=credentials.get('warehouse'),
                role=credentials.get('role', None),
                autocommit=False
            )

            result['handle'] = handle
            result['state'] = 'open'
        except snowflake.connector.errors.Error as e:
            logger.debug("Got an error when attempting to open a snowflake "
                         "connection: '{}'"
                         .format(e))

            result['handle'] = None
            result['state'] = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return result

    @classmethod
    def query_for_existing(cls, profile, schemas, model_name=None):
        if not isinstance(schemas, (list, tuple)):
            schemas = [schemas]

        schema_list = ",".join(["'{}'".format(schema) for schema in schemas])

        sql = """
        select TABLE_NAME as name, TABLE_TYPE as type
        from INFORMATION_SCHEMA.TABLES
        where TABLE_SCHEMA in ({schema_list})
        """.format(schema_list=schema_list).strip()  # noqa

        _, cursor = cls.add_query(profile, sql, model_name, auto_begin=False)
        results = cursor.fetchall()

        relation_type_lookup = {
            'BASE TABLE': 'table',
            'VIEW': 'view'
        }

        existing = [(name, relation_type_lookup.get(relation_type))
                    for (name, relation_type) in results]

        return dict(existing)

    @classmethod
    def rename(cls, profile, schema, from_name, to_name, model_name=None):
        sql = (('alter table "{schema}"."{from_name}" '
                'rename to "{schema}"."{to_name}"')
               .format(schema=schema,
                       from_name=from_name,
                       to_name=to_name))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def add_begin_query(cls, profile, name):
        return cls.add_query(profile, 'BEGIN', name, auto_begin=False,
                             select_schema=False)

    @classmethod
    def create_schema(cls, profile, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)
        sql = cls.get_create_schema_sql(schema)
        return cls.add_query(profile, sql, model_name, select_schema=False)

    @classmethod
    def get_existing_schemas(cls, profile, model_name=None):
        sql = "select distinct SCHEMA_NAME from INFORMATION_SCHEMA.SCHEMATA"

        connection, cursor = cls.add_query(profile, sql, model_name,
                                           select_schema=False,
                                           auto_begin=False)
        results = cursor.fetchall()

        return [row[0] for row in results]

    @classmethod
    def check_schema_exists(cls, profile, schema, model_name=None):
        sql = """
        select count(*)
        from INFORMATION_SCHEMA.SCHEMATA
        where SCHEMA_NAME = '{schema}'
        """.format(schema=schema).strip()  # noqa

        connection, cursor = cls.add_query(profile, sql, model_name,
                                           select_schema=False,
                                           auto_begin=False)
        results = cursor.fetchone()

        return results[0] > 0

    @classmethod
    def add_query(cls, profile, sql, model_name=None, auto_begin=True,
                  select_schema=True):
        # snowflake only allows one query per api call.
        queries = sql.strip().split(";")
        cursor = None

        if select_schema:
            super(PostgresAdapter, cls).add_query(
                profile,
                'use schema "{}"'.format(cls.get_default_schema(profile)),
                model_name,
                auto_begin)

        for individual_query in queries:
            # hack -- after the last ';', remove comments and don't run
            # empty queries. this avoids using exceptions as flow control,
            # and also allows us to return the status of the last cursor
            without_comments = re.sub(
                re.compile('^.*(--.*)$', re.MULTILINE),
                '', individual_query).strip()

            if without_comments == "":
                continue

            connection, cursor = super(PostgresAdapter, cls).add_query(
                profile, individual_query, model_name, auto_begin)

        return connection, cursor

    @classmethod
    def cancel_connection(cls, profile, connection):
        handle = connection['handle']
        sid = handle.session_id

        connection_name = connection.get('name')

        sql = 'select system$abort_session({})'.format(sid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, sid))

        _, cursor = cls.add_query(profile, sql, 'master')
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))
