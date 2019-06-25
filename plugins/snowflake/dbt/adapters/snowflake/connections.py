import re
from io import StringIO
from contextlib import contextmanager

import snowflake.connector
import snowflake.connector.errors

import dbt.exceptions
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger


SNOWFLAKE_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'account': {
            'type': 'string',
        },
        'user': {
            'type': 'string',
        },
        'password': {
            'type': 'string',
        },
        'authenticator': {
            'type': 'string',
            'description': "Either 'externalbrowser', or a valid Okta url"
        },
        'private_key_path': {
            'type': 'string',
        },
        'private_key_passphrase': {
            'type': 'string',
        },
        'database': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
        'warehouse': {
            'type': 'string',
        },
        'role': {
            'type': 'string',
        },
        'client_session_keep_alive': {
            'type': 'boolean',
        }
    },
    'required': ['account', 'user', 'database', 'schema'],
}


class SnowflakeCredentials(Credentials):
    SCHEMA = SNOWFLAKE_CREDENTIALS_CONTRACT

    @property
    def type(self):
        return 'snowflake'

    def _connection_keys(self):
        return ('account', 'user', 'database', 'schema', 'warehouse', 'role')


class SnowflakeConnectionManager(SQLConnectionManager):
    TYPE = 'snowflake'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except snowflake.connector.errors.ProgrammingError as e:
            msg = str(e)

            logger.debug('Snowflake error: {}'.format(msg))

            if 'Empty SQL statement' in msg:
                logger.debug("got empty sql statement, moving on")
            elif 'This session does not have a current database' in msg:
                self.release()
                raise dbt.exceptions.FailedToConnectException(
                    ('{}\n\nThis error sometimes occurs when invalid '
                     'credentials are provided, or when your default role '
                     'does not have access to use the specified database. '
                     'Please double check your profile and try again.')
                    .format(msg))
            else:
                self.release()
                raise dbt.exceptions.DatabaseException(msg)
        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise dbt.exceptions.RuntimeException(e.msg)

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        try:
            credentials = connection.credentials
            # Pull all of the optional authentication args for the connector,
            # let connector handle the actual arg validation
            auth_args = {auth_key: credentials[auth_key]
                         for auth_key in ['user', 'password', 'authenticator']
                         if auth_key in credentials}

            auth_args['private_key'] = cls._get_private_key(
                credentials.get('private_key_path'),
                credentials.get('private_key_passphrase'))

            handle = snowflake.connector.connect(
                account=credentials.account,
                database=credentials.database,
                schema=credentials.schema,
                warehouse=credentials.warehouse,
                role=credentials.get('role', None),
                autocommit=False,
                client_session_keep_alive=credentials.get(
                    'client_session_keep_alive', False),
                **auth_args
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

    def cancel(self, connection):
        handle = connection.handle
        sid = handle.session_id

        connection_name = connection.name

        sql = 'select system$abort_session({})'.format(sid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, sid))

        _, cursor = self.add_query(sql)
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))

    @classmethod
    def get_status(cls, cursor):
        state = cursor.sqlstate

        if state is None:
            state = 'SUCCESS'

        return "{} {}".format(state, cursor.rowcount)

    @classmethod
    def _split_queries(cls, sql):
        "Splits sql statements at semicolons into discrete queries"

        sql_s = str(sql)
        sql_buf = StringIO(sql_s)
        split_query = snowflake.connector.util_text.split_statements(sql_buf)
        return [part[0] for part in split_query]

    @classmethod
    def _get_private_key(cls, private_key_path, private_key_passphrase):
        """Get Snowflake private key by path or None."""
        if private_key_path is None or private_key_passphrase is None:
            return None

        with open(private_key_path, 'rb') as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=private_key_passphrase.encode(),
                backend=default_backend())

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

    def add_query(self, sql, auto_begin=True,
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

            connection, cursor = super().add_query(
                individual_query, auto_begin,
                bindings=bindings,
                abridge_sql_log=abridge_sql_log
            )

        if cursor is None:
            raise dbt.exceptions.RuntimeException(
                "Tried to run an empty query on model '{}'. If you are "
                "conditionally running\nsql, eg. in a model hook, make "
                "sure your `else` clause contains valid sql!\n\n"
                "Provided SQL:\n{}"
                .format(self.nice_connection_name(), sql)
            )

        return connection, cursor

    @classmethod
    def _rollback_handle(cls, connection):
        """On snowflake, rolling back the handle of an aborted session raises
        an exception.
        """
        try:
            connection.handle.rollback()
        except snowflake.connector.errors.ProgrammingError as e:
            msg = str(e)
            if 'Session no longer exists' not in msg:
                raise
