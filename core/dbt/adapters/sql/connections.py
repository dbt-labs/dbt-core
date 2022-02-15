import abc
import time
from typing import List, Optional, Tuple, Any, Iterable, Dict, Union

import agate

import dbt.clients.agate_helper
import dbt.exceptions
from dbt.adapters.base import BaseConnectionManager
from dbt.contracts.connection import Connection, ConnectionState, AdapterResponse
from dbt.events.functions import fire_event
from dbt.events.types import ConnectionUsed, SQLQuery, SQLCommit, SQLQueryStatus


class SQLConnectionManager(BaseConnectionManager):
    """The default connection manager with some common SQL methods implemented.

    Methods to implement:
        - exception_handler
        - cancel
        - get_response
        - open
    """

    @abc.abstractmethod
    def cancel(self, connection: Connection):
        """Cancel the given connection."""
        raise dbt.exceptions.NotImplementedException(
            "`cancel` is not implemented for this adapter!"
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
                if connection.handle is not None and connection.state == ConnectionState.OPEN:
                    self.cancel(connection)
                if connection.name is not None:
                    names.append(connection.name)
        return names

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql

            fire_event(SQLQuery(conn_name=connection.name, sql=log_sql))
            pre = time.time()

            cursor = connection.handle.cursor()
            cursor.execute(sql, bindings)

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)), elapsed=round((time.time() - pre), 2)
                )
            )

            return connection, cursor

    @abc.abstractclassmethod
    def get_response(cls, cursor: Any) -> Union[AdapterResponse, str]:
        """Get the status of the cursor."""
        raise dbt.exceptions.NotImplementedException(
            "`get_response` is not implemented for this adapter!"
        )

    @classmethod
    def process_results(
        cls, column_names: Iterable[str], rows: Iterable[Any]
    ) -> List[Dict[str, Any]]:
        # TODO CT-211
        unique_col_names = dict()  # type: ignore[var-annotated]
        # TODO CT-211
        for idx in range(len(column_names)):  # type: ignore[arg-type]
            # TODO CT-211
            col_name = column_names[idx]  # type: ignore[index]
            if col_name in unique_col_names:
                unique_col_names[col_name] += 1
                # TODO CT-211
                column_names[idx] = f"{col_name}_{unique_col_names[col_name]}"  # type: ignore[index] # noqa
            else:
                # TODO CT-211
                unique_col_names[column_names[idx]] = 1  # type: ignore[index]
        return [dict(zip(column_names, row)) for row in rows]

    @classmethod
    def get_result_from_cursor(cls, cursor: Any) -> agate.Table:
        data: List[Any] = []
        column_names: List[str] = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            data = cls.process_results(column_names, rows)

        return dbt.clients.agate_helper.table_from_data_flat(data, column_names)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[Union[AdapterResponse, str], agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return response, table

    def add_begin_query(self):
        return self.add_query("BEGIN", auto_begin=False)

    def add_commit_query(self):
        return self.add_query("COMMIT", auto_begin=False)

    def begin(self):
        connection = self.get_thread_connection()
        if connection.transaction_open is True:
            raise dbt.exceptions.InternalException(
                'Tried to begin a new transaction on connection "{}", but '
                "it already had one open!".format(connection.name)
            )

        self.add_begin_query()

        connection.transaction_open = True
        return connection

    def commit(self):
        connection = self.get_thread_connection()
        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to commit transaction on connection "{}", but '
                "it does not have one open!".format(connection.name)
            )

        fire_event(SQLCommit(conn_name=connection.name))
        self.add_commit_query()

        connection.transaction_open = False

        return connection
