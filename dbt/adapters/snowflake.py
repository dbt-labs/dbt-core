import copy

import snowflake.connector
import snowflake.connector.errors

from contextlib import contextmanager

import dbt.flags as flags

from dbt.adapters.postgres import PostgresAdapter
from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger

connection_cache = {}


@contextmanager
def exception_handler(connection, cursor, model_name):
    handle = connection.get('handle')
    schema = connection.get('credentials', {}).get('schema')

    try:
        yield
    except Exception as e:
        handle.rollback()
        logger.exception("Error running SQL: %s", sql)
        logger.debug("rolling back connection")
        raise e
    finally:
        cursor.close()


class SnowflakeAdapter(PostgresAdapter):

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

        logger.debug('Acquiring snowflake connection')

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

        return result
