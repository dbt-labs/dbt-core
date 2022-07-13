import abc
import os
from time import sleep

# multiprocessing.RLock is a function returning this type
from multiprocessing.synchronize import RLock
from threading import get_ident
from typing import (
    Dict,
    Tuple,
    Hashable,
    Optional,
    ContextManager,
    List,
    Type,
    Union,
    Iterable,
    Callable,
)

import agate

import dbt.exceptions
from dbt.contracts.connection import (
    Connection,
    Identifier,
    ConnectionState,
    AdapterRequiredConfig,
    LazyHandle,
    AdapterResponse,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.adapters.base.query_headers import (
    MacroQueryStringSetter,
)
from dbt.events import AdapterLogger
from dbt.events.functions import fire_event
from dbt.events.types import (
    NewConnection,
    ConnectionReused,
    ConnectionLeftOpen,
    ConnectionLeftOpen2,
    ConnectionClosed,
    ConnectionClosed2,
    Rollback,
    RollbackFailed,
)
from dbt import flags

SleepTime = Union[int, float]


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
                raise dbt.exceptions.InvalidConnectionException(key, list(self.thread_connections))
            return self.thread_connections[key]

    def set_thread_connection(self, conn: Connection) -> None:
        key = self.get_thread_identifier()
        if key in self.thread_connections:
            raise dbt.exceptions.InternalException(
                "In set_thread_connection, existing connection exists for {}"
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
            "`exception_handler` is not implemented for this adapter!"
        )

    def set_connection_name(self, name: Optional[str] = None) -> Connection:
        conn_name: str
        if name is None:
            # if a name isn't specified, we'll re-use a single handle
            # named 'master'
            conn_name = "master"
        else:
            if not isinstance(name, str):
                raise dbt.exceptions.CompilerException(
                    f"For connection name, got {name} - not a string!"
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
                credentials=self.profile.credentials,
            )
            self.set_thread_connection(conn)

        if conn.name == conn_name and conn.state == "open":
            return conn

        fire_event(NewConnection(conn_name=conn_name, conn_type=self.TYPE))

        if conn.state == "open":
            fire_event(ConnectionReused(conn_name=conn_name))
        else:
            conn.handle = LazyHandle(self.open)

        conn.name = conn_name
        return conn

    @classmethod
    def set_connection_handle(
        cls,
        connection: Connection,
        logger: AdapterLogger,
        retry_limit: int = 1,
        timeout: Union[Callable[[int, int], SleepTime], SleepTime] = 3,
        retry_on_exceptions: Optional[Iterable[Type[Exception]]] = None,
        not_retry_on_exceptions: Optional[Iterable[Type[Exception]]] = None,
        **kwargs,
    ) -> Connection:
        """Given a Connection, set its handle by calling Connection.credentials.acquire_handle.

        The acquire_handle method call will be retried up to retry_limit times to deal with
        transient connection errors. By default, one retry will be attempted if retry_on_exceptions
        or not_retry_on_exceptions are set.

        :param Connection connection: An instance of a Connection that needs a handle to be set,
            usually when attempting to open it.
        :param AdapterLogger logger: A logger to emit messages on retry attempts or errors. When
            handling expected errors, we call debug, and call warning on unexpected errors or when
            all retry attempts have been exhausted.
        :param int retry_limit: How many times to retry the call to acquire_handle. If this limit
            is exceeded before a successful call, a FailedToConnectException will be raised.
        :param int timeout: Time to wait between attempts to acquire_handle. Can also take a
            Callable that takes the attempt number and the max number of attempts and returns an
            int or float to be passed to time.sleep.
        :param Iterable retry_on_exceptions: An iterable of exception classes that if raised by
            acquire_handle should trigger a retry. If None (default) and not_retry_on_exceptions is
            also None, no exceptions will trigger a retry.
        :param Iterable not_retry_on_exceptions: An iterable of exception classes that if raised by
            acquire_handle should not trigger a retry. An empty iterable may be used to retry on
            all exceptions. This argument is ignored if retry_on_exceptions is not None.
        :param kwargs: Optional additional keyword arguments to be passed to
            Connection.credentials.acquire_handle.
        :raises dbt.exceptions.FailedToConnectException: Upon exhausting all retry attempts without
            successfully acquiring a handle, or upon the first non-expected Exception if retry_all
            is set to False.
        :return: The given connection with its appropriate state and handle attributes set
            depending on whether we successfully acquired a handle or not.
        """
        attempt = 1
        latest_exc = None
        is_inclusive = True

        if retry_on_exceptions is not None:
            exception_iter = retry_on_exceptions
        elif not_retry_on_exceptions is not None:
            exception_iter = not_retry_on_exceptions
            is_inclusive = False
        else:
            exception_iter = ()

        max_attempts = 1 + retry_limit
        acquire_handle = connection.credentials.acquire_handle

        while attempt <= max_attempts:
            try:
                handle = acquire_handle(**kwargs)

            except Exception as e:
                latest_exc = e

                if any(issubclass(type(e), exc) for exc in exception_iter) is is_inclusive:
                    logger.debug(
                        "Got an error when attempting to open a "
                        f"{cls.TYPE} connection. Retrying due to "
                        "exception subclass found in retry_on_exceptions"
                        "or not present in not_retry_on_exceptions"
                        "This was attempt number: {attempt} of "
                        f"{max_attempts}. "
                        f"Retrying in {timeout} "
                        f"seconds. Error: '{e}'"
                    )

                    timeout_secs = timeout(attempt, max_attempts) if callable(timeout) else timeout
                    sleep(timeout_secs)
                    continue

                logger.warning(
                    "Got unexpected an error when attempting to open a "
                    f"{cls.TYPE} connection. This was attempt number: "
                    f"{attempt} of {max_attempts}. Error: '{e}'"
                )
                break

            finally:
                attempt += 1

            connection.handle = handle
            connection.state = ConnectionState.OPEN
            return connection

        connection.handle = None
        connection.state = ConnectionState.FAIL
        raise dbt.exceptions.FailedToConnectException(str(latest_exc))

    @abc.abstractmethod
    def cancel_open(self) -> Optional[List[str]]:
        """Cancel all open connections on the adapter. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            "`cancel_open` is not implemented for this adapter!"
        )

    @abc.abstractclassmethod
    def open(cls, connection: Connection) -> Connection:
        """Open the given connection on the adapter and return it.

        This may mutate the given connection (in particular, its state and its
        handle).

        This should be thread-safe, or hold the lock if necessary. The given
        connection should not be in either in_use or available.
        """
        raise dbt.exceptions.NotImplementedException("`open` is not implemented for this adapter!")

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
                if connection.state not in {"closed", "init"}:
                    fire_event(ConnectionLeftOpen(conn_name=connection.name))
                else:
                    fire_event(ConnectionClosed(conn_name=connection.name))
                self.close(connection)

            # garbage collect these connections
            self.thread_connections.clear()

    @abc.abstractmethod
    def begin(self) -> None:
        """Begin a transaction. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            "`begin` is not implemented for this adapter!"
        )

    @abc.abstractmethod
    def commit(self) -> None:
        """Commit a transaction. (passable)"""
        raise dbt.exceptions.NotImplementedException(
            "`commit` is not implemented for this adapter!"
        )

    @classmethod
    def _rollback_handle(cls, connection: Connection) -> None:
        """Perform the actual rollback operation."""
        try:
            connection.handle.rollback()
        except Exception:
            fire_event(RollbackFailed(conn_name=connection.name))

    @classmethod
    def _close_handle(cls, connection: Connection) -> None:
        """Perform the actual close operation."""
        # On windows, sometimes connection handles don't have a close() attr.
        if hasattr(connection.handle, "close"):
            fire_event(ConnectionClosed2(conn_name=connection.name))
            connection.handle.close()
        else:
            fire_event(ConnectionLeftOpen2(conn_name=connection.name))

    @classmethod
    def _rollback(cls, connection: Connection) -> None:
        """Roll back the given connection."""
        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                f"Tried to rollback transaction on connection "
                f'"{connection.name}", but it does not have one open!'
            )

        fire_event(Rollback(conn_name=connection.name))
        cls._rollback_handle(connection)

        connection.transaction_open = False

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        # if the connection is in closed or init, there's nothing to do
        if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
            return connection

        if connection.transaction_open and connection.handle:
            fire_event(Rollback(conn_name=connection.name))
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
    ) -> Tuple[AdapterResponse, agate.Table]:
        """Execute the given SQL.

        :param str sql: The sql to execute.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :return: A tuple of the query status and results (empty if fetch=False).
        :rtype: Tuple[AdapterResponse, agate.Table]
        """
        raise dbt.exceptions.NotImplementedException(
            "`execute` is not implemented for this adapter!"
        )
