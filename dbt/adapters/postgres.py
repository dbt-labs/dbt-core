import copy
import psycopg2
import re
import time
import yaml

from contextlib import contextmanager

import dbt.exceptions
import dbt.flags as flags

from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.schema import Column, READ_PERMISSION_DENIED_ERROR

connection_cache = {}

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


@contextmanager
def exception_handler(connection, cursor, model_name, query):
    handle = connection.get('handle')
    schema = connection.get('credentials', {}).get('schema')

    try:
        yield
    except psycopg2.ProgrammingError as e:
        handle.rollback()
        error_data = {"model": model_name,
                      "schema": schema,
                      "user": connection.get('credentials', {}).get('user')}
        if 'must be owner of relation' in e.diag.message_primary:
            raise RuntimeError(
                RELATION_NOT_OWNER_MESSAGE.format(**error_data))
        elif "permission denied for" in e.diag.message_primary:
            raise RuntimeError(
                RELATION_PERMISSION_DENIED_MESSAGE.format(**error_data))
        else:
            raise e
    except Exception as e:
        handle.rollback()
        logger.debug("Error running SQL: %s", query)
        logger.debug("rolling back connection")
        raise e


class PostgresAdapter:

    # TODO: wrap sql-related things into the adapter rather than having
    #       the compiler call this to get the context
    date_function = 'datenow()'

    @classmethod
    def acquire_connection(cls, profile):
        # profile requires some marshalling right now because it includes a
        # wee bit of global config.
        # TODO remove this
        credentials = copy.deepcopy(profile)

        credentials.pop('type', None)
        credentials.pop('threads', None)

        result = {
            'type': 'postgres',
            'state': 'init',
            'handle': None,
            'credentials': credentials
        }

        logger.info('Connecting to postgres.')

        if flags.STRICT_MODE:
            validate_connection(result)

        return cls.open_connection(result)

    @staticmethod
    def hash_profile(profile):
        return ("{}--{}--{}--{}".format(
            profile.get('host'),
            profile.get('dbname'),
            profile.get('schema'),
            profile.get('user'),
        ))

    @classmethod
    def get_connection(cls, profile):
        profile_hash = cls.hash_profile(profile)

        if connection_cache.get(profile_hash):
            connection = connection_cache.get(profile_hash)
            return connection

        connection = cls.acquire_connection(profile)
        connection_cache[profile_hash] = connection

        return cls.get_connection(profile)

    @staticmethod
    def get_connection_spec(connection):
        credentials = connection.get('credentials')

        return ("dbname='{}' user='{}' host='{}' password='{}' port='{}' "
                "connect_timeout=10".format(
                    credentials.get('dbname'),
                    credentials.get('user'),
                    credentials.get('host'),
                    credentials.get('pass'),
                    credentials.get('port'),
                ))

    @classmethod
    def open_connection(cls, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()

        try:
            handle = psycopg2.connect(cls.get_connection_spec(connection))

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
    def create_schema(cls, profile, schema, model_name=None):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        query = ('create schema if not exists "{schema}"'
                 .format(schema=schema))

        handle, cursor = cls.add_query_to_transaction(
            query, connection, model_name)

    @classmethod
    def dist_qualifier(cls, dist):
        return ''

    @classmethod
    def sort_qualifier(cls, sort_type, sort):
        return ''

    @classmethod
    def create_table(cls, profile, schema, table, columns, sort, dist):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        fields = ['"{field}" {data_type}'.format(
            field=column.name, data_type=column.data_type
        ) for column in columns]
        fields_csv = ",\n  ".join(fields)
        dist = cls.dist_qualifier(dist)
        sort = cls.sort_qualifier('compound', sort)
        sql = """
        create table if not exists "{schema}"."{table}" (
        {fields}
        )
        {dist} {sort}
        """.format(
            schema=schema,
            table=table,
            fields=fields_csv,
            sort=sort,
            dist=dist)

        logger.debug('creating table "%s"."%s"'.format(schema, table))

        cls.add_query_to_transaction(
            sql, connection, table)

    @classmethod
    def get_default_schema(cls, profile):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        return connection.get('credentials', {}).get('schema')

    @classmethod
    def drop(cls, profile, relation, relation_type, model_name=None):
        if relation_type == 'view':
            return cls.drop_view(profile, relation, model_name)
        elif relation_type == 'table':
            return cls.drop_table(profile, relation, model_name)
        else:
            raise RuntimeError(
                "Invalid relation_type '{}'"
                .format(relation_type))

    @classmethod
    def drop_view(cls, profile, view, model_name):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        query = ('drop view if exists "{schema}"."{view}" cascade'
                 .format(
                     schema=schema,
                     view=view))

        handle, cursor = cls.add_query_to_transaction(
            query, connection, model_name)

    @classmethod
    def drop_table(cls, profile, table, model_name):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        query = ('drop table if exists "{schema}"."{table}" cascade'
                 .format(
                     schema=schema,
                     table=table))

        handle, cursor = cls.add_query_to_transaction(
            query, connection, model_name)

    @classmethod
    def truncate(cls, profile, table, model_name=None):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        query = ('truncate table "{schema}"."{table}"'
                 .format(
                     schema=schema,
                     table=table))

        handle, cursor = cls.add_query_to_transaction(
            query, connection, model_name)

    @classmethod
    def rename(cls, profile, from_name, to_name, model_name=None):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        query = ('alter table "{schema}"."{from_name}" rename to "{to_name}"'
                 .format(
                     schema=schema,
                     from_name=from_name,
                     to_name=to_name))

        handle, cursor = cls.add_query_to_transaction(
            query, connection, model_name)

    @classmethod
    def execute_model(cls, profile, model):
        parts = re.split(r'-- (DBT_OPERATION .*)', model.compiled_contents)
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        for i, part in enumerate(parts):
            matches = re.match(r'^DBT_OPERATION ({.*})$', part)
            if matches is not None:
                instruction_string = matches.groups()[0]
                instruction = yaml.safe_load(instruction_string)
                function = instruction['function']
                kwargs = instruction['args']

                def call_expand_target_column_types(kwargs):
                    kwargs.update({'profile': profile})
                    return cls.expand_target_column_types(**kwargs)

                func_map = {
                    'expand_column_types_if_needed':
                    call_expand_target_column_types
                }

                func_map[function](kwargs)
            else:
                handle, cursor = cls.add_query_to_transaction(
                    part, connection, model.name)

        handle.commit()

        status = cls.get_status(cursor)
        cursor.close()

        return status

    @classmethod
    def get_missing_columns(cls, profile,
                            from_schema, from_table,
                            to_schema, to_table):
        """Returns dict of {column:type} for columns in from_table that are
        missing from to_table"""
        from_columns = {col.name: col for col in
                        cls.get_columns_in_table(
                            profile, from_schema, from_table)}
        to_columns = {col.name: col for col in
                      cls.get_columns_in_table(
                          profile, to_schema, to_table)}

        missing_columns = set(from_columns.keys()) - set(to_columns.keys())

        return [col for (col_name, col) in from_columns.items()
                if col_name in missing_columns]

    @classmethod
    def get_columns_in_table(cls, profile, schema_name, table_name):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        query = """
        select column_name, data_type, character_maximum_length
        from information_schema.columns
        where table_name = '{table_name}'
        """.format(table_name=table_name).strip()

        if schema_name is not None:
            query += (" AND table_schema = '{schema_name}'"
                      .format(schema_name=schema_name))

        handle, cursor = cls.add_query_to_transaction(
            query, connection, table_name)

        data = cursor.fetchall()
        columns = []

        for row in data:
            name, data_type, char_size = row
            column = Column(name, data_type, char_size)
            columns.append(column)

        return columns

    @classmethod
    def expand_target_column_types(cls, profile,
                                   temp_table,
                                   to_schema, to_table):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        reference_columns = {col.name: col for col in
                             cls.get_columns_in_table(
                                 profile, None, temp_table)}
        target_columns = {col.name: col for col in
                          cls.get_columns_in_table(
                              profile, to_schema, to_table)}

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)

            if target_column is not None and \
               target_column.can_expand_to(reference_column):
                new_type = Column.string_type(reference_column.string_size())
                logger.debug("Changing col type from %s to %s in table %s.%s",
                             target_column.data_type,
                             new_type,
                             to_schema,
                             to_table)

                cls.alter_column_type(
                    connection, to_schema, to_table, column_name, new_type)

    @classmethod
    def alter_column_type(cls, connection,
                          schema, table, column_name, new_column_type):
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

        query = """
        alter table "{schema}"."{table}" add column "{tmp_column}" {dtype};
        update "{schema}"."{table}" set "{tmp_column}" = "{old_column}";
        alter table "{schema}"."{table}" drop column "{old_column}" cascade;
        alter table "{schema}"."{table}" rename column "{tmp_column}" to "{old_column}";
        """.format(**opts).strip()  # noqa

        handle, cursor = cls.add_query_to_transaction(
            query, connection, table)

        return cls.get_status(cursor)

    @classmethod
    def table_exists(cls, profile, schema, table):
        tables = cls.query_for_existing(profile, schema)
        exists = tables.get(table) is not None
        return exists

    @classmethod
    def query_for_existing(cls, profile, schema):
        query = """
        select tablename as name, 'table' as type from pg_tables
        where schemaname = '{schema}'
        union all
        select viewname as name, 'view' as type from pg_views
        where schemaname = '{schema}'
        """.format(schema=schema).strip()  # noqa

        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        _, cursor = cls.add_query_to_transaction(
            query, connection, schema)
        results = cursor.fetchall()

        existing = [(name, relation_type) for (name, relation_type) in results]

        return dict(existing)

    @classmethod
    def execute_all(cls, profile, queries, model_name=None):
        if len(queries) == 0:
            return

        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        handle = connection.get('handle')

        for i, query in enumerate(queries):
            handle, cursor = cls.add_query_to_transaction(
                query, connection, model_name)

        return cls.get_status(cursor)

    @classmethod
    def execute_one(cls, profile, query, model_name=None):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        handle = connection.get('handle')

        return cls.add_query_to_transaction(
            query, connection, model_name)

    @classmethod
    def commit(cls, profile):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        handle = connection.get('handle')
        handle.commit()

    @classmethod
    def get_status(cls, cursor):
        return cursor.statusmessage

    @classmethod
    def add_query_to_transaction(cls, query, connection, model_name=None):
        handle = connection.get('handle')
        cursor = handle.cursor()

        with exception_handler(connection, cursor, model_name, query):
            logger.debug("SQL: %s", query)
            pre = time.time()
            cursor.execute(query)
            post = time.time()
            logger.debug(
                "SQL status: %s in %0.2f seconds",
                cls.get_status(cursor), post-pre)
            return handle, cursor
