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

    @contextmanager
    def exception_handler(self, sql, model_name=None,
                          connection_name='master'):
        connection = self.get_connection(connection_name)

        try:
            yield
        except snowflake.connector.errors.ProgrammingError as e:
            msg = dbt.compat.to_string(e)

            logger.debug('Snowflake error: {}'.format(msg))

            if 'Empty SQL statement' in msg:
                logger.debug("got empty sql statement, moving on")
            elif 'This session does not have a current database' in msg:
                self.release_connection(connection_name)
                raise dbt.exceptions.FailedToConnectException(
                    ('{}\n\nThis error sometimes occurs when invalid '
                     'credentials are provided, or when your default role '
                     'does not have access to use the specified database. '
                     'Please double check your profile and try again.')
                    .format(msg))
            else:
                self.release_connection(connection_name)
                raise dbt.exceptions.DatabaseException(msg)
        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            self.release_connection(connection_name)
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
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        try:
            credentials = connection.credentials
            handle = snowflake.connector.connect(
                account=credentials.account,
                user=credentials.user,
                password=credentials.password,
                database=credentials.database,
                schema=credentials.schema,
                warehouse=credentials.warehouse,
                role=credentials.get('role', None),
                autocommit=False,
                client_session_keep_alive=credentials.get(
                    'client_session_keep_alive', False)
            )

            connection.handle = handle
            connection.state = 'open'
        except snowflake.connector.errors.Error as e:
            logger.debug("Got an error when attempting to open a snowflake "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def _link_cached_relations(self, manifest, schemas):
        pass

    def _list_relations(self, schema, model_name=None):
        sql = """
        select
          table_name as name, table_schema as schema, table_type as type
        from information_schema.tables
        where table_schema ilike '{schema}'
        """.format(schema=schema).strip()  # noqa

        _, cursor = self.add_query(sql, model_name, auto_begin=False)

        results = cursor.fetchall()

        relation_type_lookup = {
            'BASE TABLE': 'table',
            'VIEW': 'view'

        }
        return [self.Relation.create(
            database=self.config.credentials.database,
            schema=_schema,
            identifier=name,
            quote_policy={
                'identifier': True,
                'schema': True,
            },
            type=relation_type_lookup.get(type))
                for (name, _schema, type) in results]

    def rename_relation(self, from_relation, to_relation,
                        model_name=None):
        self.cache.rename(from_relation, to_relation)
        sql = 'alter table {} rename to {}'.format(
            from_relation, to_relation)

        connection, cursor = self.add_query(sql, model_name)

    def add_begin_query(self, name):
        return self.add_query('BEGIN', name, auto_begin=False)

    def get_existing_schemas(self, model_name=None):
        sql = "select distinct schema_name from information_schema.schemata"

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)
        results = cursor.fetchall()

        return [row[0] for row in results]

    def check_schema_exists(self, schema, model_name=None):
        sql = """
        select count(*)
        from information_schema.schemata
        where upper(schema_name) = upper('{schema}')
        """.format(schema=schema).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)
        results = cursor.fetchone()

        return results[0] > 0

    @classmethod
    def _split_queries(cls, sql):
        "Splits sql statements at semicolons into discrete queries"

        sql_s = dbt.compat.to_string(sql)
        sql_buf = StringIO(sql_s)
        split_query = snowflake.connector.util_text.split_statements(sql_buf)
        return [part[0] for part in split_query]

    def add_query(self, sql, model_name=None, auto_begin=True,
                  bindings=None, abridge_sql_log=False):

        connection = None
        cursor = None

        if bindings:
            # The snowflake connector is more strict than, eg., psycopg2 -
            # which allows any iterable thing to be passed as a binding.
            bindings = tuple(bindings)

        queries = self._split_queries(sql)

        for individual_query in queries:
            # hack -- after the last ';', remove comments and don't run
            # empty queries. this avoids using exceptions as flow control,
            # and also allows us to return the status of the last cursor
            without_comments = re.sub(
                re.compile('^.*(--.*)$', re.MULTILINE),
                '', individual_query).strip()

            if without_comments == "":
                continue

            connection, cursor = super(SnowflakeAdapter, self).add_query(
                individual_query, model_name, auto_begin, bindings=bindings,
                abridge_sql_log=abridge_sql_log
            )

        if cursor is None:
            raise dbt.exceptions.RuntimeException(
                    "Tried to run an empty query on model '{}'. If you are "
                    "conditionally running\nsql, eg. in a model hook, make "
                    "sure your `else` clause contains valid sql!\n\n"
                    "Provided SQL:\n{}".format(model_name, sql))

        return connection, cursor

    @classmethod
    def _catalog_filter_table(cls, table, manifest):
        # On snowflake, users can set QUOTED_IDENTIFIERS_IGNORE_CASE, so force
        # the column names to their lowercased forms.
        lowered = table.rename(
            column_names=[c.lower() for c in table.column_names]
        )
        return super(SnowflakeAdapter, cls)._catalog_filter_table(lowered,
                                                                  manifest)

    def _make_match_kwargs(self, schema, identifier):
        quoting = self.config.quoting
        if identifier is not None and quoting['identifier'] is False:
            identifier = identifier.upper()

        if schema is not None and quoting['schema'] is False:
            schema = schema.upper()

        return filter_null_values({'identifier': identifier,
                                   'schema': schema})

    def cancel_connection(self, connection):
        handle = connection.handle
        sid = handle.session_id

        connection_name = connection.name

        sql = 'select system$abort_session({})'.format(sid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, sid))

        _, cursor = self.add_query(sql, 'master')
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
