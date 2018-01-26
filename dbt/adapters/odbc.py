import pyodbc

import dbt.exceptions

from dbt.adapters.default import DefaultAdapter, \
    connections_available, connections_in_use

from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger


class ODBCAdapter(dbt.adapters.default.DefaultAdapter):

    @classmethod
    def get_odbc_connection_string(cls, credentials):
        raise dbt.exceptions.NotImplementedException(
            '`get_odbc_connection_string` is not implemented for '
            'this adapter!')

    @classmethod
    def get_status(cls, cursor):
        return cursor.rowcount

    @classmethod
    def open_connection(cls, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()

        try:
            credentials = connection.get('credentials', {})
            handle = pyodbc.connect(
                cls.get_odbc_connection_string(credentials),
                autocommit=False)

            result['handle'] = handle
            result['state'] = 'open'
        except Exception as e:
            logger.debug("Got an error when attempting to open an odbc "
                         "connection: '{}'"
                         .format(e))
            result['handle'] = None
            result['state'] = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return result

    @classmethod
    def cancel_connection(cls, profile, connection):
        pass

    @classmethod
    def commit(cls, profile, connection):
        global connections_in_use

        if dbt.flags.STRICT_MODE:
            validate_connection(connection)

        connection = cls.reload(connection)

        if connection['transaction_open'] is False:
            raise dbt.exceptions.InternalException(
                'Tried to commit transaction on connection "{}", but '
                'it does not have one open!'.format(connection.get('name')))

        connection.get('handle').commit()

        connection['transaction_open'] = False
        connections_in_use[connection.get('name')] = connection

        return connection

    @classmethod
    def check_schema_exists(cls, profile, schema, model_name=None):
        return (schema in cls.get_existing_schemas(profile, model_name))
