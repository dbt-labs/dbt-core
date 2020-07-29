import abc
import time
from typing import List, Optional, Tuple, Any, Iterable, Dict

import agate

import dbt.clients.agate_helper
import dbt.exceptions
from dbt.adapters.base import BaseConnectionManager
from dbt.contracts.connection import Connection, ConnectionState
from dbt.logger import GLOBAL_LOGGER as logger
from dbt import flags


class SQLConnectionManager(BaseConnectionManager):
    """The default connection manager with some common SQL methods implemented.

    Methods to implement:
        - exception_handler
        - cancel
        - get_status
        - open
    """
    @abc.abstractmethod
    def cancel(self, connection: Connection):
        """Cancel the given connection."""
        raise dbt.exceptions.NotImplementedException(
            '`cancel` is not implemented for this adapter!'
        )

    def cancel_open(self) -> List[str]:
        names = []
        this_connection = self.get_if_exists()
        with self.lock:
            for connection in self.thread_connections.values():
                if connection is this_connection:
                    continue

                # if the connection failed, the handle will be None so we have
                # nothing to cancel.
                if (
                    connection.handle is not None and
                    connection.state == ConnectionState.OPEN
                ):
                    self.cancel(connection)
                if connection.name is not None:
                    names.append(connection.name)
        return names

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug('Using {} connection "{}".'
                     .format(self.TYPE, connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = '{}...'.format(sql[:512])
            else:
                log_sql = sql

            logger.debug(
                'On {connection_name}: {sql}',
                connection_name=connection.name,
                sql=log_sql,
            )
            pre = time.time()

            cursor = connection.handle.cursor()
            cursor.execute(sql, bindings)

            logger.debug(
                "SQL status: {status} in {elapsed:0.2f} seconds",
                status=self.get_status(cursor),
                elapsed=(time.time() - pre)
            )

            return connection, cursor

    @abc.abstractclassmethod
    def get_status(cls, cursor: Any) -> str:
        """Get the status of the cursor."""
        raise dbt.exceptions.NotImplementedException(
            '`get_status` is not implemented for this adapter!'
        )

    @classmethod
    def process_results(
        cls,
        column_names: Iterable[str],
        rows: Iterable[Any]
    ) -> List[Dict[str, Any]]:

        return [dict(zip(column_names, row)) for row in rows]

    @classmethod
    def get_result_from_cursor(cls, cursor: Any) -> agate.Table:
        data: List[Any] = []
        column_names: List[str] = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            data = cls.process_results(column_names, rows)

        return dbt.clients.agate_helper.table_from_data_flat(
            data,
            column_names
        )

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[str, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        status = self.get_status(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return status, table

    def add_begin_query(self):
        return self.add_query('BEGIN', auto_begin=False)

    def add_commit_query(self):
        return self.add_query('COMMIT', auto_begin=False)

    def begin(self):
        connection = self.get_thread_connection()

        if flags.STRICT_MODE:
            if not isinstance(connection, Connection):
                raise dbt.exceptions.CompilerException(
                    f'In begin, got {connection} - not a Connection!'
                )

        if connection.transaction_open is True:
            raise dbt.exceptions.InternalException(
                'Tried to begin a new transaction on connection "{}", but '
                'it already had one open!'.format(connection.name))

        self.add_begin_query()

        connection.transaction_open = True
        return connection

    def commit(self):
        connection = self.get_thread_connection()
        if flags.STRICT_MODE:
            if not isinstance(connection, Connection):
                raise dbt.exceptions.CompilerException(
                    f'In commit, got {connection} - not a Connection!'
                )

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to commit transaction on connection "{}", but '
                'it does not have one open!'.format(connection.name))

        logger.debug('On {}: COMMIT'.format(connection.name))
        self.add_commit_query()

        connection.transaction_open = False

        return connection
