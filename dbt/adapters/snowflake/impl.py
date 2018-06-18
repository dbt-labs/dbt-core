from __future__ import absolute_import

import re
from io import StringIO

import snowflake.connector
import snowflake.connector.errors

from contextlib import contextmanager

import dbt.compat
import dbt.exceptions

from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import filter_null_values


class SnowflakeAdapter(PostgresAdapter):

    Relation = SnowflakeRelation

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
    def list_relations(cls, profile, project_cfg, schema, model_name=None):
        sql = """
        select
          table_name as name, table_schema as schema, table_type as type
        from information_schema.tables
        where table_schema ilike '{schema}'
        """.format(schema=schema).strip()  # noqa

        _, cursor = cls.add_query(
            profile, sql, model_name, auto_begin=False)

        results = cursor.fetchall()

        relation_type_lookup = {
            'BASE TABLE': 'table',
            'VIEW': 'view'

        }
        return [cls.Relation.create(
            database=profile.get('database'),
            schema=_schema,
            identifier=name,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=relation_type_lookup.get(type))
                for (name, _schema, type) in results]

    @classmethod
    def rename_relation(cls, profile, project_cfg, from_relation,
                        to_relation, model_name=None):
        sql = 'alter table {} rename to {}'.format(
            from_relation, to_relation)

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def add_begin_query(cls, profile, name):
        return cls.add_query(profile, 'BEGIN', name, auto_begin=False)

    @classmethod
    def get_existing_schemas(cls, profile, project_cfg, model_name=None):
        sql = "select distinct schema_name from information_schema.schemata"

        connection, cursor = cls.add_query(profile, sql, model_name,
                                           auto_begin=False)
        results = cursor.fetchall()

        return [row[0] for row in results]

    @classmethod
    def check_schema_exists(cls, profile, project_cfg,
                            schema, model_name=None):
        sql = """
        select count(*)
        from information_schema.schemata
        where upper(schema_name) = upper('{schema}')
        """.format(schema=schema).strip()  # noqa

        connection, cursor = cls.add_query(profile, sql, model_name,
                                           auto_begin=False)
        results = cursor.fetchone()

        return results[0] > 0

    @classmethod
    def _split_queries(cls, sql):
        "Splits sql statements at semicolons into discrete queries"

        sql_s = dbt.compat.to_string(sql)
        sql_buf = StringIO(sql_s)
        split_query = snowflake.connector.util_text.split_statements(sql_buf)
        return [part[0] for part in split_query]

    @classmethod
    def add_query(cls, profile, sql, model_name=None, auto_begin=True,
                  bindings=None, abridge_sql_log=False):

        connection = None
        cursor = None

        if bindings:
            # The snowflake connector is more strict than, eg., psycopg2 -
            # which allows any iterable thing to be passed as a binding.
            bindings = tuple(bindings)

        queries = cls._split_queries(sql)

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
                profile, individual_query, model_name, auto_begin,
                bindings=bindings, abridge_sql_log=abridge_sql_log)

        if cursor is None:
            raise dbt.exceptions.RuntimeException(
                    "Tried to run an empty query on model '{}'. If you are "
                    "conditionally running\nsql, eg. in a model hook, make "
                    "sure your `else` clause contains valid sql!\n\n"
                    "Provided SQL:\n{}".format(model_name, sql))

        return connection, cursor

    @classmethod
    def _make_match_kwargs(cls, project_cfg, schema, identifier):
        if identifier is not None and \
           project_cfg.get('quoting', {}).get('identifier', True) is False:
            identifier = identifier.upper()

        if schema is not None and \
           project_cfg.get('quoting', {}).get('schema', True) is False:
            schema = schema.upper()

        return filter_null_values({'identifier': identifier,
                                   'schema': schema})

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

    @classmethod
    def _get_columns_in_table_sql(cls, schema_name, table_name, database):
        schema_filter = '1=1'
        if schema_name is not None:
            schema_filter = "table_schema ilike '{}'".format(schema_name)

        db_prefix = '' if database is None else '{}.'.format(database)

        sql = """
        select
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision || ',' || numeric_scale as numeric_size

        from {db_prefix}information_schema.columns
        where table_name ilike '{table_name}'
          and {schema_filter}
        order by ordinal_position
        """.format(db_prefix=db_prefix,
                   table_name=table_name,
                   schema_filter=schema_filter).strip()

        return sql
