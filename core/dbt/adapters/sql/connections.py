import abc
import time

import dbt.clients.agate_helper
import dbt.exceptions
from dbt.contracts.connection import Connection
from dbt.adapters.base import BaseConnectionManager
from dbt.compat import abstractclassmethod
from dbt.logger import GLOBAL_LOGGER as logger


class SQLConnectionManager(BaseConnectionManager):
    """The default connection manager with some common SQL methods implemented.

    Methods to implement:
        - exception_handler
        - cancel
        - get_status
        - open
    """
    @abc.abstractmethod
    def cancel(self, connection):
        """Cancel the given connection.

        :param Connection connection: The connection to cancel.
        """
        raise dbt.exceptions.NotImplementedException(
            '`cancel` is not implemented for this adapter!'
        )

    def cancel_open(self):
        names = []
        with self.lock:
            for name, connection in self.in_use.items():
                if name == 'master':
                    continue

                self.cancel(connection)
                names.append(name)
        return names

    def add_query(self, sql, name=None, auto_begin=True, bindings=None,
                  abridge_sql_log=False):
        connection = self.get(name)
        connection_name = connection.name

        if auto_begin and connection.transaction_open is False:
            self.begin(connection_name)

        logger.debug('Using {} connection "{}".'
                     .format(self.TYPE, connection_name))

        with self.exception_handler(sql, connection_name):
            if abridge_sql_log:
                logger.debug('On %s: %s....', connection_name, sql[0:512])
            else:
                logger.debug('On %s: %s', connection_name, sql)
            pre = time.time()

            cursor = connection.handle.cursor()
            cursor.execute(sql, bindings)

            logger.debug("SQL status: %s in %0.2f seconds",
                         self.get_status(cursor), (time.time() - pre))

            return connection, cursor

    @abstractclassmethod
    def get_status(cls, cursor):
        """Get the status of the cursor.

        :param cursor: A database handle to get status from
        :return: The current status
        :rtype: str
        """
        raise dbt.exceptions.NotImplementedException(
            '`get_status` is not implemented for this adapter!'
        )

    @classmethod
    def get_result_from_cursor(cls, cursor):
        data = []
        column_names = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            raw_results = cursor.fetchall()
            data = [dict(zip(column_names, row))
                    for row in raw_results]

        return dbt.clients.agate_helper.table_from_data(data, column_names)

    def execute(self, sql, name=None, auto_begin=False, fetch=False):
        self.get(name)
        _, cursor = self.add_query(sql, name, auto_begin)
        status = self.get_status(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return status, table

    def add_begin_query(self, name):
        return self.add_query('BEGIN', name, auto_begin=False)

    def add_commit_query(self, name):
        return self.add_query('COMMIT', name, auto_begin=False)

    def begin(self, name):
        connection = self.get(name)

        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        if connection.transaction_open is True:
            raise dbt.exceptions.InternalException(
                'Tried to begin a new transaction on connection "{}", but '
                'it already had one open!'.format(connection.get('name')))

        self.add_begin_query(name)

        connection.transaction_open = True
        self.in_use[name] = connection

        return connection

    def commit(self, connection):

        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        connection = self.get(connection.name)

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to commit transaction on connection "{}", but '
                'it does not have one open!'.format(connection.name))

        logger.debug('On {}: COMMIT'.format(connection.name))
        self.add_commit_query(connection.name)

        connection.transaction_open = False
        self.in_use[connection.name] = connection

        return connection
