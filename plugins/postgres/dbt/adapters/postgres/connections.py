from contextlib import contextmanager

import psycopg2

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.helper_types import Port
from dataclasses import dataclass
from typing import Optional


@dataclass
class PostgresCredentials(Credentials):
    host: str
    user: str
    port: Port
    password: str  # on postgres the password is mandatory
    role: Optional[str] = None
    search_path: Optional[str] = None
    keepalives_idle: int = 0  # 0 means to use the default value
    sslmode: Optional[str] = None
    application_name: Optional[str] = 'dbt'

    _ALIASES = {
        'dbname': 'database',
        'pass': 'password'
    }

    @property
    def type(self):
        return 'postgres'

    def _connection_keys(self):
        return ('host', 'port', 'user', 'database', 'schema', 'search_path',
                'keepalives_idle', 'sslmode')


class PostgresConnectionManager(SQLConnectionManager):
    TYPE = 'postgres'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except psycopg2.DatabaseError as e:
            logger.debug('Postgres error: {}'.format(str(e)))

            try:
                self.rollback_if_open()
            except psycopg2.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(e) from e

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {}
        # we don't want to pass 0 along to connect() as postgres will try to
        # call an invalid setsockopt() call (contrary to the docs).
        if credentials.keepalives_idle:
            kwargs['keepalives_idle'] = credentials.keepalives_idle

        # psycopg2 doesn't support search_path officially,
        # see https://github.com/psycopg/psycopg2/issues/465
        search_path = credentials.search_path
        if search_path is not None and search_path != '':
            # see https://postgresql.org/docs/9.5/libpq-connect.html
            kwargs['options'] = '-c search_path={}'.format(
                search_path.replace(' ', '\\ '))

        if credentials.sslmode:
            kwargs['sslmode'] = credentials.sslmode

        if credentials.application_name:
            kwargs['application_name'] = credentials.application_name

        try:
            handle = psycopg2.connect(
                dbname=credentials.database,
                user=credentials.user,
                host=credentials.host,
                password=credentials.password,
                port=credentials.port,
                connect_timeout=10,
                **kwargs)

            if credentials.role:
                handle.cursor().execute('set role {}'.format(credentials.role))

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
        try:
            pid = connection.handle.get_backend_pid()
        except psycopg2.InterfaceError as exc:
            # if the connection is already closed, not much to cancel!
            if 'already closed' in str(exc):
                logger.debug(
                    f'Connection {connection_name} was already closed'
                )
                return
            # probably bad, re-raise it
            raise

        sql = "select pg_terminate_backend({})".format(pid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, pid))

        _, cursor = self.add_query(sql)
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        message = str(cursor.statusmessage)
        rows = cursor.rowcount
        status_message_parts = message.split() if message is not None else []
        status_messsage_strings = [
            part
            for part in status_message_parts
            if not part.isdigit()
        ]
        code = ' '.join(status_messsage_strings)
        return AdapterResponse(
            _message=message,
            code=code,
            rows_affected=rows
        )
