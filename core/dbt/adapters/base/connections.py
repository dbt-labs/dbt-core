import abc
import os
# multiprocessing.RLock is a function returning this type
from multiprocessing.synchronize import RLock
from threading import get_ident
from typing import (
    Dict, Tuple, Hashable, Optional, ContextManager, List, Union
)

import agate

import dbt.exceptions
from dbt.contracts.connection import (
    Connection, Identifier, ConnectionState,
    AdapterRequiredConfig, LazyHandle, AdapterResponse
)
from dbt.contracts.graph.manifest import Manifest
from dbt.adapters.base.query_headers import (
    MacroQueryStringSetter,
)
from dbt.logger import GLOBAL_LOGGER as logger
from dbt import flags


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
    TYPE: str = NotImplemented

    def __init__(self, profile: AdapterRequiredConfig):
        self.profile = profile
        self.thread_connections: Dict[Hashable, Connection] = {}
        self.lock: RLock = flags.MP_CONTEXT.RLock()
        self.query_header: Optional[MacroQueryStringSetter] = None

    def set_query_header(self, manifest: Manifest) -> None:
        self.query_header = MacroQueryStringSetter(self.profile, manifest)

    @staticmethod
    def get_thread_identifier() -> Hashable:
        # note that get_ident() may be re-used, but we should never experience
        # that within a single process
        return (os.getpid(), get_ident())

    def get_thread_connection(self) -> Connection:
        key = self.get_thread_identifier()
        with self.lock:
            if key not in self.thread_connections:
                raise dbt.exceptions.InvalidConnectionException(
                    key, list(self.thread_connections)
                )
            return self.thread_connections[key]

    def set_thread_connection(self, conn: Connection) -> None:
        key = self.get_thread_identifier()
        if key in self.thread_connections:
            raise dbt.exceptions.InternalException(
                'In set_thread_connection, existing connection exists for {}'
            )
        self.thread_connections[key] = conn

    def get_if_exists(self) -> Optional[Connection]:
        key = self.get_thread_identifier()
        with self.lock:
            return self.thread_connections.get(key)

    def clear_thread_connection(self) -> None:
        key = self.get_thread_identifier()
        with self.lock:
            if key in self.thread_connections:
                del self.thread_connections[key]

    def clear_transaction(self) -> None:
        """Clear any existing transactions."""
        conn = self.get_thread_connection()
        if conn is not None:
            if conn.transaction_open:
                self._rollback(conn)
            self.begin()
            self.commit()

    def rollback_if_open(self) -> None:
        conn = self.get_if_exists()
        if conn is not None and conn.handle and conn.transaction_open:
            self._rollback(conn)

    @abc.abstractmethod
    def exception_handler(self, sql: str) -> ContextManager:
        """Create a context manager that handles exceptions caused by database
        interactions.

        :param str sql: The SQL string that the block inside the context
            manager is executing.
        :return: A context manager that handles exceptions raised by the
            underlying database.
        """
        raise dbt.exceptions.NotImplementedException(
            '`exception_handler` is not implemented for this adapter!')

    def set_connection_name(self, name: Optional[str] = None) -> Connection:
        conn_name: str
        if name is None:
            # if a name isn't specified, we'll re-use a single handle
            # named 'master'
            conn_name = 'master'
        else:
            if not isinstance(name, str):
                raise dbt.exceptions.CompilerException(
                    f'For connection name, got {name} - not a string!'
                )
            assert isinstance(name, str)
            conn_name = name

        conn = self.get_if_exists()
        if conn is None:
            conn = Connection(
                type=Identifier(self.TYPE),
                name=None,
                state=ConnectionState.INIT,
                transaction_open=False,
                handle=None,
                credentials=self.profile.credentials
            )
            self.set_thread_connection(conn)

        if conn.name == conn_name and conn.state == 'open':
            return conn

        logger.debug(
            'Acquiring new {} connection "{}".'.format(self.TYPE, conn_name))

        if conn.state == 'open':
            logger.debug(
                'Re-using an available connection from the pool (formerly {}).'
                .format(conn.name)
            )
        else:
            conn.handle = LazyHandle(self.open)

        conn.name = conn_name
        return conn

    @abc.abstractmethod
    def cancel_open(self) -> Optional[List[str]]:
        """Cancel all open connections on the adapter. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            '`cancel_open` is not implemented for this adapter!'
        )

    @abc.abstractclassmethod
    def open(cls, connection: Connection) -> Connection:
        """Open the given connection on the adapter and return it.

        This may mutate the given connection (in particular, its state and its
        handle).

        This should be thread-safe, or hold the lock if necessary. The given
        connection should not be in either in_use or available.
        """
        raise dbt.exceptions.NotImplementedException(
            '`open` is not implemented for this adapter!'
        )

    def release(self) -> None:
        with self.lock:
            conn = self.get_if_exists()
            if conn is None:
                return

        try:
            # always close the connection. close() calls _rollback() if there
            # is an open transaction
            self.close(conn)
        except Exception:
            # if rollback or close failed, remove our busted connection
            self.clear_thread_connection()
            raise

    def cleanup_all(self) -> None:
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
    def begin(self) -> None:
        """Begin a transaction. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            '`begin` is not implemented for this adapter!'
        )

    @abc.abstractmethod
    def commit(self) -> None:
        """Commit a transaction. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            '`commit` is not implemented for this adapter!'
        )

    @classmethod
    def _rollback_handle(cls, connection: Connection) -> None:
        """Perform the actual rollback operation."""
        try:
            connection.handle.rollback()
        except Exception:
            logger.debug(
                'Failed to rollback {}'.format(connection.name),
                exc_info=True
            )

    @classmethod
    def _close_handle(cls, connection: Connection) -> None:
        """Perform the actual close operation."""
        # On windows, sometimes connection handles don't have a close() attr.
        if hasattr(connection.handle, 'close'):
            logger.debug(f'On {connection.name}: Close')
            connection.handle.close()
        else:
            logger.debug(f'On {connection.name}: No close available on handle')

    @classmethod
    def _rollback(cls, connection: Connection) -> None:
        """Roll back the given connection."""
        if flags.STRICT_MODE:
            if not isinstance(connection, Connection):
                raise dbt.exceptions.CompilerException(
                    f'In _rollback, got {connection} - not a Connection!'
                )

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                f'Tried to rollback transaction on connection '
                f'"{connection.name}", but it does not have one open!'
            )

        logger.debug(f'On {connection.name}: ROLLBACK')
        cls._rollback_handle(connection)

        connection.transaction_open = False

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        if flags.STRICT_MODE:
            if not isinstance(connection, Connection):
                raise dbt.exceptions.CompilerException(
                    f'In close, got {connection} - not a Connection!'
                )

        # if the connection is in closed or init, there's nothing to do
        if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
            return connection

        if connection.transaction_open and connection.handle:
            logger.debug('On {}: ROLLBACK'.format(connection.name))
            cls._rollback_handle(connection)
        connection.transaction_open = False

        cls._close_handle(connection)
        connection.state = ConnectionState.CLOSED

        return connection

    def commit_if_has_connection(self) -> None:
        """If the named connection exists, commit the current transaction."""
        connection = self.get_if_exists()
        if connection:
            self.commit()

    def _add_query_comment(self, sql: str) -> str:
        if self.query_header is None:
            return sql
        return self.query_header.add(sql)

    @abc.abstractmethod
    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[Union[str, AdapterResponse], agate.Table]:
        """Execute the given SQL.

        :param str sql: The sql to execute.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :return: A tuple of the status and the results (empty if fetch=False).
        :rtype: Tuple[Union[str, AdapterResponse], agate.Table]
        """
        raise dbt.exceptions.NotImplementedException(
            '`execute` is not implemented for this adapter!'
        )
