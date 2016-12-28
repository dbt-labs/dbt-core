import psycopg2

from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger


class PostgresAdapter:

    @staticmethod
    def acquire_connection(cfg, profile):
        result = {
            'type': 'postgres',
            'state': 'init',
            'handle': None
        }

        logger.debug('Acquiring postgres connection')

        if cfg.get('STRICT_MODE', False):
            logger.debug('Strict mode on, validating connection')
            validate_connection(result)

        return PostgresAdapter.open_connection(cfg, profile, result)

    @staticmethod
    def get_connection():
        pass

    @staticmethod
    def create_table():
        pass

    @staticmethod
    def drop_table():
        pass

    # private API below

    @staticmethod
    def open_connection(cfg, profile, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()

        try:
            handle = psycopg2.connect(PostgresAdapter.profile_to_spec(profile))

            result['handle'] = handle
            result['state'] = 'open'
        except psycopg2.Error as e:
            logger.debug("Got an error when attempting to open a postgres "
                         "connection: '{}'"
                         .format(e))
            result['handle'] = None
            result['state'] = 'fail'

        return result

    @staticmethod
    def profile_to_spec(profile):
        return ("dbname='{}' user='{}' host='{}' password='{}' port='{}' "
                "connect_timeout=10".format(
                    profile.get('dbname'),
                    profile.get('user'),
                    profile.get('host'),
                    profile.get('password'),
                    profile.get('port'),
                ))
