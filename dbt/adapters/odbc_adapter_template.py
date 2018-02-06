from contextlib import contextmanager

import dbt.adapters.odbc
import dbt.compat
import dbt.exceptions

from dbt.logger import GLOBAL_LOGGER as logger


class NewAdapter(dbt.adapters.odbc.ODBCAdapter):

    @classmethod
    @contextmanager
    def exception_handler(cls, profile, sql, model_name=None,
                          connection_name=None):
        ## scaffold -- see https://github.com/mkleehammer/pyodbc/wiki/Exceptions
        try:
            yield

        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            cls.release_connection(profile, connection_name)
            raise dbt.exceptions.RuntimeException(str(e))

    @classmethod
    def type(cls):
        raise dbt.exceptions.NotImplementedException(
            "`type` is not implemented for this adapter.")

    @classmethod
    def date_function(cls):
        raise dbt.exceptions.NotImplementedException(
            "`date_function` is not implemented for this adapter.")

    @classmethod
    def get_odbc_connection_string(cls, credentials):
        raise dbt.exceptions.NotImplementedException(
            "`get_odbc_connection_string` is not implemented "
            "for this adapter.")

    @classmethod
    def rename(cls, profile, schema, from_name, to_name, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            "`rename` is not implemented for this adapter.")

    @classmethod
    def get_columns_in_table(cls, profile, schema_name, table_name,
                             model_name=None):
        raise dbt.exceptions.NotImplementedException(
            "`get_columns_in_table` is not implemented for this adapter.")

    @classmethod
    def alter_column_type(cls, profile, schema, table, column_name,
                          new_column_type, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            "`alter_column_type` is not implemented for this adapter.")

    @classmethod
    def query_for_existing(cls, profile, schemas, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            "`query_for_existing` is not implemented for this adapter.")

    @classmethod
    def get_existing_schemas(cls, profile, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            "`get_existing_schemas` is not implemented for this adapter.")

    @classmethod
    def quote(cls, identifier):
        raise dbt.exceptions.NotImplementedException(
            "`quote` is not implemented for this adapter.")
