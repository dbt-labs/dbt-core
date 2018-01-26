import pyodbc

import dbt.exceptions

from dbt.adapters.sql_server import SqlServerAdapter

from dbt.logger import GLOBAL_LOGGER as logger


class AzureDataWarehouseAdapter(SqlServerAdapter):

    @classmethod
    def type(cls):
        return 'azure_dw'

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
                autocommit=True)

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
    def begin(cls, profile, name='master'):
        pass

    @classmethod
    def commit(cls, profile, connection):
        pass

    @classmethod
    def rollback(cls, connection):
        pass

    @classmethod
    def drop_relation(cls, profile, schema, rel_name, rel_type, model_name):
        relation = cls.quote_schema_and_table(profile, schema, rel_name)
        sql = 'drop {} {}'.format(rel_type, relation)

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def rename(cls, profile, schema, from_name, to_name, model_name=None):
        from_relation = cls.quote_schema_and_table(profile, schema, from_name)

        sql = "rename object {} to {}".format(from_relation, to_name)

        connection, cursor = cls.add_query(profile, sql, model_name)
