import base64
import datetime
import pytz
import re
from contextlib import contextmanager
from dataclasses import dataclass
from io import StringIO
from typing import Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import requests
import snowflake.connector
import snowflake.connector.errors

from dbt.exceptions import (
    InternalException, RuntimeException, FailedToConnectException,
    DatabaseException, warn_or_error
)
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger


_TOKEN_REQUEST_URL = 'https://{}.snowflakecomputing.com/oauth/token-request'


@dataclass
class SnowflakeCredentials(Credentials):
    account: str
    user: str
    warehouse: Optional[str]
    role: Optional[str]
    password: Optional[str]
    authenticator: Optional[str]
    private_key_path: Optional[str]
    private_key_passphrase: Optional[str]
    token: Optional[str]
    oauth_client_id: Optional[str]
    oauth_client_secret: Optional[str]
    query_tag: Optional[str]
    client_session_keep_alive: bool = False

    def __post_init__(self):
        if (
            self.authenticator != 'oauth' and
            (self.oauth_client_secret or self.oauth_client_id or self.token)
        ):
            # the user probably forgot to set 'authenticator' like I keep doing
            warn_or_error(
                'Authenticator is not set to oauth, but an oauth-only '
                'parameter is set! Did you mean to set authenticator: oauth?'
            )

    @property
    def type(self):
        return 'snowflake'

    def _connection_keys(self):
        return (
            'account', 'user', 'database', 'schema', 'warehouse', 'role',
            'client_session_keep_alive'
        )

    def auth_args(self):
        # Pull all of the optional authentication args for the connector,
        # let connector handle the actual arg validation
        result = {}
        if self.password:
            result['password'] = self.password
        if self.authenticator:
            result['authenticator'] = self.authenticator
            if self.authenticator == 'oauth':
                token = self.token
                # if we have a client ID/client secret, the token is a refresh
                # token, not an access token
                if self.oauth_client_id and self.oauth_client_secret:
                    token = self._get_access_token()
                elif self.oauth_client_id:
                    warn_or_error(
                        'Invalid profile: got an oauth_client_id, but not an '
                        'oauth_client_secret!'
                    )
                elif self.oauth_client_secret:
                    warn_or_error(
                        'Invalid profile: got an oauth_client_secret, but not '
                        'an oauth_client_id!'
                    )

                result['token'] = token
            # enable the token cache
            result['client_store_temporary_credential'] = True
        result['private_key'] = self._get_private_key()
        return result

    def _get_access_token(self) -> str:
        if self.authenticator != 'oauth':
            raise InternalException('Can only get access tokens for oauth')
        missing = any(
            x is None for x in
            (self.oauth_client_id, self.oauth_client_secret, self.token)
        )
        if missing:
            raise InternalException(
                'need a client ID a client secret, and a refresh token to get '
                'an access token'
            )
        # should the full url be a config item?
        token_url = _TOKEN_REQUEST_URL.format(self.account)
        # I think this is only used to redirect on success, which we ignore
        # (it does not have to match the integration's settings in snowflake)
        redirect_uri = 'http://localhost:9999'
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.token,
            'redirect_uri': redirect_uri
        }

        auth = base64.b64encode(
            f'{self.oauth_client_id}:{self.oauth_client_secret}'
            .encode('ascii')
        ).decode('ascii')
        headers = {
            'Authorization': f'Basic {auth}',
            'Content-type': 'application/x-www-form-urlencoded;charset=utf-8'
        }
        result = requests.post(token_url, headers=headers, data=data)
        result_json = result.json()
        if 'access_token' not in result_json:
            raise DatabaseException(f'Did not get a token: {result_json}')
        return result_json['access_token']

    def _get_private_key(self):
        """Get Snowflake private key by path or None."""
        if not self.private_key_path:
            return None

        if self.private_key_passphrase:
            encoded_passphrase = self.private_key_passphrase.encode()
        else:
            encoded_passphrase = None

        with open(self.private_key_path, 'rb') as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=encoded_passphrase,
                backend=default_backend())

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())


class SnowflakeConnectionManager(SQLConnectionManager):
    TYPE = 'snowflake'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except snowflake.connector.errors.ProgrammingError as e:
            msg = str(e)

            logger.debug('Snowflake query id: {}'.format(e.sfqid))
            logger.debug('Snowflake error: {}'.format(msg))

            if 'Empty SQL statement' in msg:
                logger.debug("got empty sql statement, moving on")
            elif 'This session does not have a current database' in msg:
                raise FailedToConnectException(
                    ('{}\n\nThis error sometimes occurs when invalid '
                     'credentials are provided, or when your default role '
                     'does not have access to use the specified database. '
                     'Please double check your profile and try again.')
                    .format(msg))
            else:
                raise DatabaseException(msg)
        except Exception as e:
            if isinstance(e, snowflake.connector.errors.Error):
                logger.debug('Snowflake query id: {}'.format(e.sfqid))

            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise RuntimeException(str(e)) from e

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        try:
            creds = connection.credentials

            handle = snowflake.connector.connect(
                account=creds.account,
                user=creds.user,
                database=creds.database,
                schema=creds.schema,
                warehouse=creds.warehouse,
                role=creds.role,
                autocommit=False,
                client_session_keep_alive=creds.client_session_keep_alive,
                application='dbt',
                **creds.auth_args()
            )

            if creds.query_tag:
                handle.cursor().execute(
                    ("alter session set query_tag = '{}'")
                    .format(creds.query_tag))

            connection.handle = handle
            connection.state = 'open'
        except snowflake.connector.errors.Error as e:
            logger.debug("Got an error when attempting to open a snowflake "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise FailedToConnectException(str(e))

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
    def process_results(cls, column_names, rows):
        # Override for Snowflake. The datetime objects returned by
        # snowflake-connector-python are not pickleable, so we need
        # to replace them with sane timezones
        fixed = []
        for row in rows:
            fixed_row = []
            for col in row:
                if isinstance(col, datetime.datetime) and col.tzinfo:
                    offset = col.utcoffset()
                    offset_seconds = offset.total_seconds()
                    new_timezone = pytz.FixedOffset(offset_seconds // 60)
                    col = col.astimezone(tz=new_timezone)
                fixed_row.append(col)

            fixed.append(fixed_row)

        return super().process_results(column_names, fixed)

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
            conn = self.get_thread_connection()
            if conn is None or conn.name is None:
                conn_name = '<None>'
            else:
                conn_name = conn.name

            raise RuntimeException(
                "Tried to run an empty query on model '{}'. If you are "
                "conditionally running\nsql, eg. in a model hook, make "
                "sure your `else` clause contains valid sql!\n\n"
                "Provided SQL:\n{}"
                .format(conn_name, sql)
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
