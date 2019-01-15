from collections import Iterable
from contextlib import contextmanager
from datetime import datetime
from getpass import getuser
import re

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.compat import basestring, NUMBERS, to_string
from dbt.exceptions import RuntimeException
from dbt.logger import GLOBAL_LOGGER as logger

import prestodb
from prestodb.transaction import IsolationLevel
from prestodb.auth import KerberosAuthentication
import sqlparse


PRESTO_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'database': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
        'host': {
            'type': 'string',
        },
        'port': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 65535,
        },
        'method': {
            # TODO: what do most people use? Kerberos is what the official one
            # implements.
            'enum': ['none', 'kerberos'],
        },
        'userinfo-json': {
            'type': 'object',
        },
    },
    'required': ['database', 'schema', 'host', 'port'],
}


class PrestoCredentials(Credentials):
    SCHEMA = PRESTO_CREDENTIALS_CONTRACT
    ALIASES = {
        'catalog': 'database',
    }

    @property
    def type(self):
        return 'presto'

    def _connection_keys(self):
        return ('host', 'port', 'database', 'username')


class ConnectionWrapper(object):
    """Wrap a Presto connection in a way that accomplishes two tasks:

        - prefetch results from execute() calls so that presto calls actually
            persist to the db but then present the usual cursor interface
        - provide `cancel()` on the same object as `commit()`/`rollback()`/...

    """
    def __init__(self, handle):
        self.handle = handle
        self._cursor = None
        self._fetch_result = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor is not None:
            self._cursor.cancel()

    def close(self):
        # this is a noop on presto, but pass it through anyway
        self.handle.close()

    def commit(self):
        self.handle.commit()

    def rollback(self):
        self.handle.rollback()

    def start_transaction(self):
        self.handle.start_transaction()

    def fetchall(self):
        if self._cursor is None:
            return None

        if self._fetch_result is not None:
            ret = self._fetch_result
            self._fetch_result = None
            return ret

        return None

    def execute(self, sql, bindings=None):

        if bindings is not None:
            # presto doesn't actually pass bindings along so we have to do the
            # escaping and formatting ourselves
            bindings = tuple(self._escape_value(b) for b in bindings)
            sql = sql % bindings

        result = self._cursor.execute(sql)
        self._fetch_result = self._cursor.fetchall()
        return result

    @property
    def description(self):
        return self._cursor.description

    @classmethod
    def _escape_value(cls, value):
        """A not very comprehensive system for escaping bindings.

        I think "'" (a single quote) is the only character that matters.
        """
        if value is None:
            return 'NULL'
        elif isinstance(value, basestring):
            return "'{}'".format(value.replace("'", "''"))
        elif isinstance(value, NUMBERS):
            return value
        elif isinstance(value, datetime):
            time_formatted = value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            return "TIMESTAMP '{}'".format(time_formatted)
        else:
            raise ValueError('Cannot escape {}'.format(type(value)))


class PrestoConnectionManager(SQLConnectionManager):
    TYPE = 'presto'

    @contextmanager
    def exception_handler(self, sql, connection_name='master'):
        try:
            yield
        # TODO: introspect into `DatabaseError`s and expose `errorName`,
        # `errorType`, etc instead of stack traces full of garbage!
        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            raise RuntimeException(to_string(exc))

    def add_begin_query(self, name):
        connection = self.get(name)
        with self.exception_handler('handle.start_transaction()', name):
            connection.handle.start_transaction()

    def add_commit_query(self, name):
        connection = self.get(name)
        with self.exception_handler('handle.commit()', name):
            connection.handle.commit()

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials
        if credentials.method == 'kerberos':
            auth = KerberosAuthentication()
        else:
            auth = prestodb.constants.DEFAULT_AUTH

        # it's impossible for presto to fail here as 'connections' are actually
        # just cursor factories.
        presto_conn = prestodb.dbapi.connect(
            host=credentials.host,
            port=credentials.get('port', 8080),
            user=credentials.get('username', getuser()),
            catalog=credentials.database,
            schema=credentials.schema,
            auth=auth,
            isolation_level=IsolationLevel.SERIALIZABLE,
        )
        connection.state = 'open'
        connection.handle = ConnectionWrapper(presto_conn)
        return connection

    @classmethod
    def get_status(cls, cursor):
        # this is lame, but the cursor doesn't give us anything useful.
        return 'OK'

    def cancel(self, connection):
        connection.handle.cancel()

    def add_query(self, sql, model_name=None, auto_begin=True,
                  bindings=None, abridge_sql_log=False):

        connection = None
        cursor = None

        # TODO: is this sufficient? Largely copy+pasted from snowflake, so
        # there's some common behavior here we can maybe factor out into the
        # SQLAdapter?
        queries = [q.rstrip(';') for q in sqlparse.split(sql)]

        for individual_query in queries:
            # hack -- after the last ';', remove comments and don't run
            # empty queries. this avoids using exceptions as flow control,
            # and also allows us to return the status of the last cursor
            without_comments = re.sub(
                re.compile('^.*(--.*)$', re.MULTILINE),
                '', individual_query).strip()

            if without_comments == "":
                continue

            parent = super(PrestoConnectionManager, self)
            connection, cursor = parent.add_query(
                individual_query, model_name, auto_begin, bindings,
                abridge_sql_log
            )

        if cursor is None:
            raise RuntimeException(
                    "Tried to run an empty query on model '{}'. If you are "
                    "conditionally running\nsql, eg. in a model hook, make "
                    "sure your `else` clause contains valid sql!\n\n"
                    "Provided SQL:\n{}".format(model_name, sql))

        return connection, cursor

    def execute(self, sql, name=None, auto_begin=False, fetch=False):
        self.get(name)
        _, cursor = self.add_query(sql, name, auto_begin)
        status = self.get_status(cursor)
        table = self.get_result_from_cursor(cursor)
        return status, table
