import abc
import multiprocessing
import os
from threading import get_ident

import dbt.exceptions
import dbt.flags
from dbt.api import APIObject
from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import translate_aliases


class Credentials(APIObject):
    """Common base class for credentials. This is not valid to instantiate"""
    SCHEMA = NotImplemented
    # map credential aliases to their canonical names.
    ALIASES = {}

    def __init__(self, **kwargs):
        renamed = self.translate_aliases(kwargs)
        super().__init__(**renamed)

    @property
    def type(self):
        raise NotImplementedError(
            'type not implemented for base credentials class'
        )

    def connection_info(self):
        """Return an ordered iterator of key/value pairs for pretty-printing.
        """
        for key in self._connection_keys():
            if key in self._contents:
                yield key, self._contents[key]

    def _connection_keys(self):
        """The credential object keys that should be printed to users in
        'dbt debug' output. This is specific to each adapter.
        """
        raise NotImplementedError

    def incorporate(self, **kwargs):
        # implementation note: we have to do this here, or
        # incorporate(alias_name=...) will result in duplicate keys in the
        # merged dict that APIObject.incorporate() creates.
        renamed = self.translate_aliases(kwargs)
        return super().incorporate(**renamed)

    def serialize(self, with_aliases=False):
        serialized = super().serialize()
        if with_aliases:
            serialized.update({
                new_name: serialized[canonical_name]
                for new_name, canonical_name in self.ALIASES.items()
                if canonical_name in serialized
            })
        return serialized

    @classmethod
    def translate_aliases(cls, kwargs):
        return translate_aliases(kwargs, cls.ALIASES)


class BaseConnectionManager(metaclass=abc.ABCMeta):
    """Methods to implement:
        - exception_handler
        - cancel_open
        - open
        - begin
        - commit
        - clear_transaction
        - execute

    You must also set the 'TYPE' class attribute with a class-unique constant
    string.
    """
    TYPE = NotImplemented

    def __init__(self, profile):
        self.profile = profile
        self.thread_connections = {}
        self.lock = multiprocessing.RLock()

    @staticmethod
    def get_thread_identifier():
        # note that get_ident() may be re-used, but we should never experience
        # that within a single process
        return (os.getpid(), get_ident())

    def get_thread_connection(self):
        key = self.get_thread_identifier()
        with self.lock:
            if key not in self.thread_connections:
                raise RuntimeError(
                    'connection never acquired for thread {}, have {}'
                    .format(key, list(self.thread_connections))
                )
            return self.thread_connections[key]

    def get_if_exists(self):
        key = self.get_thread_identifier()
        with self.lock:
            return self.thread_connections.get(key)

    def clear_thread_connection(self):
        key = self.get_thread_identifier()
        with self.lock:
            if key in self.thread_connections:
                del self.thread_connections[key]

    def clear_transaction(self):
        """Clear any existing transactions."""
        conn = self.get_thread_connection()
        if conn is not None:
            if conn.transaction_open:
                self._rollback(conn)
            self.begin()
            self.commit()

    @abc.abstractmethod
    def exception_handler(self, sql):
        """Create a context manager that handles exceptions caused by database
        interactions.

        :param str sql: The SQL string that the block inside the context
            manager is executing.
        :return: A context manager that handles exceptions raised by the
            underlying database.
        """
        raise dbt.exceptions.NotImplementedException(
            '`exception_handler` is not implemented for this adapter!')

    def set_connection_name(self, name=None):
        if name is None:
            # if a name isn't specified, we'll re-use a single handle
            # named 'master'
            name = 'master'

        conn = self.get_if_exists()
        thread_id_key = self.get_thread_identifier()

        if conn is None:
            conn = Connection(
                type=self.TYPE,
                name=None,
                state='init',
                transaction_open=False,
                handle=None,
                credentials=self.profile.credentials
            )
            self.thread_connections[thread_id_key] = conn

        if conn.name == name and conn.state == 'open':
            return conn

        logger.debug('Acquiring new {} connection "{}".'
                     .format(self.TYPE, name))

        if conn.state == 'open':
            logger.debug(
                'Re-using an available connection from the pool (formerly {}).'
                .format(conn.name))
        else:
            logger.debug('Opening a new connection, currently in state {}'
                         .format(conn.state))
            self.open(conn)

        conn.name = name
        return conn

    @abc.abstractmethod
    def cancel_open(self):
        """Cancel all open connections on the adapter. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            '`cancel_open` is not implemented for this adapter!'
        )

    @abc.abstractclassmethod
    def open(cls, connection):
        """Open a connection on the adapter.

        This may mutate the given connection (in particular, its state and its
        handle).

        This should be thread-safe, or hold the lock if necessary. The given
        connection should not be in either in_use or available.

        :param Connection connection: A connection object to open.
        :return: A connection with a handle attached and an 'open' state.
        :rtype: Connection
        """
        raise dbt.exceptions.NotImplementedException(
            '`open` is not implemented for this adapter!'
        )

    def release(self):
        with self.lock:
            conn = self.get_if_exists()
            if conn is None:
                return

        try:
            if conn.state == 'open':
                if conn.transaction_open is True:
                    self._rollback(conn)
            else:
                self.close(conn)
        except Exception:
            # if rollback or close failed, remove our busted connection
            self.clear_thread_connection()
            raise

    def cleanup_all(self):
        with self.lock:
            for connection in self.thread_connections.values():
                if connection.state not in {'closed', 'init'}:
                    logger.debug("Connection '{}' was left open."
                                 .format(connection.name))
                else:
                    logger.debug("Connection '{}' was properly closed."
                                 .format(connection.name))
                self.close(connection)

            # garbage collect these connections
            self.thread_connections.clear()

    @abc.abstractmethod
    def begin(self):
        """Begin a transaction. (passable)

        :param str name: The name of the connection to use.
        """
        raise dbt.exceptions.NotImplementedException(
            '`begin` is not implemented for this adapter!'
        )

    @abc.abstractmethod
    def commit(self):
        """Commit a transaction. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            '`commit` is not implemented for this adapter!'
        )

    @classmethod
    def _rollback_handle(cls, connection):
        """Perform the actual rollback operation."""
        connection.handle.rollback()

    @classmethod
    def _close_handle(cls, connection):
        """Perform the actual close operation."""
        # On windows, sometimes connection handles don't have a close() attr.
        if hasattr(connection.handle, 'close'):
            logger.debug('On {}: Close'.format(connection.name))
            connection.handle.close()
        else:
            logger.debug('On {}: No close available on handle'
                         .format(connection.name))

    @classmethod
    def _rollback(cls, connection):
        """Roll back the given connection.
        """
        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to rollback transaction on connection "{}", but '
                'it does not have one open!'.format(connection.name))

        logger.debug('On {}: ROLLBACK'.format(connection.name))
        cls._rollback_handle(connection)

        connection.transaction_open = False

        return connection

    @classmethod
    def close(cls, connection):
        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        # if the connection is in closed or init, there's nothing to do
        if connection.state in {'closed', 'init'}:
            return connection

        if connection.transaction_open and connection.handle:
            cls._rollback_handle(connection)
        connection.transaction_open = False

        cls._close_handle(connection)
        connection.state = 'closed'

        return connection

    def commit_if_has_connection(self):
        """If the named connection exists, commit the current transaction.

        :param str name: The name of the connection to use.
        """
        connection = self.get_if_exists()
        if connection:
            self.commit()

    @abc.abstractmethod
    def execute(self, sql, auto_begin=False, fetch=False):
        """Execute the given SQL.

        :param str sql: The sql to execute.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :return: A tuple of the status and the results (empty if fetch=False).
        :rtype: Tuple[str, agate.Table]
        """
        raise dbt.exceptions.NotImplementedException(
            '`execute` is not implemented for this adapter!'
        )
