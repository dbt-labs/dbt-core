from __future__ import absolute_import

import copy
import re
import time
import yaml

import snowflake.connector
import snowflake.connector.errors

from contextlib import contextmanager

import dbt.exceptions
import dbt.flags as flags

from dbt.adapters.postgres import PostgresAdapter
from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger

connection_cache = {}


@contextmanager
def exception_handler(connection, cursor, model_name, query):
    handle = connection.get('handle')
    schema = connection.get('credentials', {}).get('schema')

    try:
        yield
    except snowflake.connector.errors.ProgrammingError as e:
        if 'Empty SQL statement' in e.msg:
            logger.debug("got empty sql statement, moving on")
        else:
            handle.rollback()
            raise dbt.exceptions.ProgrammingException(str(e))
    except Exception as e:
        handle.rollback()
        logger.debug("Error running SQL: %s", query)
        logger.debug("rolling back connection")
        raise e


class SnowflakeAdapter(PostgresAdapter):

    date_function = 'CURRENT_TIMESTAMP()'

    @classmethod
    def acquire_connection(cls, profile):

        # profile requires some marshalling right now because it includes a
        # wee bit of global config.
        # TODO remove this
        credentials = copy.deepcopy(profile)

        credentials.pop('type', None)
        credentials.pop('threads', None)

        result = {
            'type': 'snowflake',
            'state': 'init',
            'handle': None,
            'credentials': credentials
        }

        logger.info('Connecting to snowflake.')

        if flags.STRICT_MODE:
            validate_connection(result)

        return cls.open_connection(result)

    @staticmethod
    def hash_profile(profile):
        return ("{}--{}--{}--{}--{}".format(
            profile.get('account'),
            profile.get('database'),
            profile.get('schema'),
            profile.get('user'),
            profile.get('warehouse'),
        ))

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
    def query_for_existing(cls, profile, schema):
        query = """
        select TABLE_NAME as name, TABLE_TYPE as type
        from INFORMATION_SCHEMA.TABLES
        where TABLE_SCHEMA = '{schema}'
        """.format(schema=schema).strip()  # noqa

        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        _, cursor = cls.add_query_to_transaction(
            query, connection, schema)
        results = cursor.fetchall()

        relation_type_lookup = {
            'BASE TABLE': 'table',
            'VIEW': 'view'
        }

        existing = [(name, relation_type_lookup.get(relation_type))
                    for (name, relation_type) in results]

        return dict(existing)

    @classmethod
    def get_status(cls, cursor):
        state = cursor.sqlstate

        if state is None:
            state = 'SUCCESS'

        return "{} {}".format(state, cursor.rowcount)

    @classmethod
    def rename(cls, profile, from_name, to_name, model_name=None):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        # in snowflake, if you fail to include the quoted schema in the
        # identifier, the new table will have `schema.upper()` as its new
        # schema
        query = ('''
        alter table "{schema}"."{from_name}"
        rename to "{schema}"."{to_name}"
        '''.format(
            schema=schema,
            from_name=from_name,
            to_name=to_name)).strip()

        handle, cursor = cls.add_query_to_transaction(
            query, connection, model_name)

    @classmethod
    def execute_model(cls, profile, model):
        parts = re.split(r'-- (DBT_OPERATION .*)', model.compiled_contents)
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        # snowflake requires a schema to be specified for temporary tables
        # TODO setup templates to be adapter-specific. then we can just use
        #      the existing schema for temp tables.
        cls.add_query_to_transaction(
            'USE SCHEMA "{}"'.format(
                connection.get('credentials', {}).get('schema')),
            connection)

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
    def add_query_to_transaction(cls, query, connection, model_name=None):
        handle = connection.get('handle')
        cursor = handle.cursor()

        # snowflake only allows one query per api call.
        queries = query.strip().split(";")

        for individual_query in queries:
            # hack -- after the last ';', remove comments and don't run
            # empty queries. this avoids using exceptions as flow control,
            # and also allows us to return the status of the last cursor
            without_comments = re.sub(
                re.compile('^.*(--.*)$', re.MULTILINE),
                '', individual_query).strip()

            if without_comments == "":
                continue

            with exception_handler(connection, cursor,
                                   model_name, individual_query):
                logger.debug("SQL: %s", individual_query)
                pre = time.time()
                cursor.execute(individual_query)
                post = time.time()
                logger.debug(
                    "SQL status: %s in %0.2f seconds",
                    cls.get_status(cursor), post-pre)

        return handle, cursor
