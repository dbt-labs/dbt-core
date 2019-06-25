from contextlib import contextmanager

import psycopg2

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger


POSTGRES_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'database': {
            'type': 'string',
        },
        'host': {
            'type': 'string',
        },
        'user': {
            'type': 'string',
        },
        'password': {
            'type': 'string',
        },
        'port': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 65535,
        },
        'schema': {
            'type': 'string',
        },
        'search_path': {
            'type': 'string',
        },
        'keepalives_idle': {
            'type': 'integer',
        },
    },
    'required': ['database', 'host', 'user', 'password', 'port', 'schema'],
}


class PostgresCredentials(Credentials):
    SCHEMA = POSTGRES_CREDENTIALS_CONTRACT
    ALIASES = {
        'dbname': 'database',
        'pass': 'password'
    }

    @property
    def type(self):
        return 'postgres'

    def _connection_keys(self):
        return ('host', 'port', 'user', 'database', 'schema', 'search_path')


class PostgresConnectionManager(SQLConnectionManager):
    DEFAULT_TCP_KEEPALIVE = 0  # 0 means to use the default value
    TYPE = 'postgres'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except psycopg2.DatabaseError as e:
            logger.debug('Postgres error: {}'.format(str(e)))

            try:
                # attempt to release the connection
                self.release()
            except psycopg2.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(str(e).strip())

        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(e)

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials.incorporate())
        kwargs = {}
        keepalives_idle = credentials.get('keepalives_idle',
                                          cls.DEFAULT_TCP_KEEPALIVE)
        # we don't want to pass 0 along to connect() as postgres will try to
        # call an invalid setsockopt() call (contrary to the docs).
        if keepalives_idle:
            kwargs['keepalives_idle'] = keepalives_idle

        # psycopg2 doesn't support search_path officially,
        # see https://github.com/psycopg/psycopg2/issues/465
        search_path = credentials.get('search_path')
        if search_path is not None and search_path != '':
            # see https://postgresql.org/docs/9.5/libpq-connect.html
            kwargs['options'] = '-c search_path={}'.format(
                search_path.replace(' ', '\\ '))

        try:
            handle = psycopg2.connect(
                dbname=credentials.database,
                user=credentials.user,
                host=credentials.host,
                password=credentials.password,
                port=credentials.port,
                connect_timeout=10,
                **kwargs)

            connection.handle = handle
            connection.state = 'open'
        except psycopg2.Error as e:
            logger.debug("Got an error when attempting to open a postgres "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        connection_name = connection.name
        pid = connection.handle.get_backend_pid()

        sql = "select pg_terminate_backend({})".format(pid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, pid))

        _, cursor = self.add_query(sql)
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_status(cls, cursor):
        return cursor.statusmessage
