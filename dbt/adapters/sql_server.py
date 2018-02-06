import pyodbc
import re

from contextlib import contextmanager

import dbt.adapters.odbc
import dbt.compat
import dbt.exceptions

from dbt.logger import GLOBAL_LOGGER as logger


class SqlServerAdapter(dbt.adapters.odbc.ODBCAdapter):

    @classmethod
    @contextmanager
    def exception_handler(cls, profile, sql, model_name=None,
                          connection_name=None):
        # scaffold -- see https://github.com/mkleehammer/pyodbc/wiki/Exceptions
        try:
            yield

        except (pyodbc.ProgrammingError, Exception) as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            logger.debug(e)
            cls.release_connection(profile, connection_name)
            raise dbt.exceptions.RuntimeException(str(e))

    @classmethod
    def type(cls):
        return 'sql_server'

    @classmethod
    def date_function(cls):
        return 'CURRENT_TIMESTAMP()'

    @classmethod
    def get_odbc_connection_string(cls, credentials):
        return 'DRIVER={driver};SERVER={host};PORT={port};DATABASE={database};UID={user};PWD={password}'.format(**credentials)  # noqa

    @classmethod
    def drop_relation(cls, profile, schema, rel_name, rel_type, model_name):
        relation = cls.quote_schema_and_table(profile, schema, rel_name)
        sql = 'drop {} if exists {}'.format(rel_type, relation)

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def rename(cls, profile, schema, from_name, to_name, model_name=None):
        from_relation = cls.quote_schema_and_table(profile, schema, from_name)
        to_relation = to_name
        sql = "sp_rename '{}', '{}'".format(from_relation, to_relation)

        connection, cursor = cls.add_query(profile, sql, model_name)

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
        if not isinstance(schemas, (list, tuple)):
            schemas = [schemas]

        schema_list = ",".join(["'{}'".format(schema) for schema in schemas])

        sql = """
select table_name as name, 'table' as type from information_schema.tables
where table_schema in ({schema_list})
union all
select table_name as name, 'view' as type from information_schema.views
where table_schema in ({schema_list})
""".format(schema_list=schema_list).strip()  # noqa

        connection, cursor = cls.add_query(profile, sql, model_name,
                                           auto_begin=False)

        results = cursor.fetchall()

        existing = [(name, relation_type) for (name, relation_type) in results]

        return dict(existing)

    @classmethod
    def get_existing_schemas(cls, profile, model_name=None):
        sql = "select distinct Name from sys.schemas"

        connection, cursor = cls.add_query(profile, sql, model_name,
                                           auto_begin=False)
        results = cursor.fetchall()

        return [row[0] for row in results]

    @classmethod
    def check_schema_exists(cls, profile, schema, model_name=None):
        return (schema in cls.get_existing_schemas(profile, model_name))

    @classmethod
    def create_schema(cls, profile, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)

        sql = cls.get_create_schema_sql(schema)
        res = cls.add_query(profile, sql, model_name, auto_begin=False)

        return res

    @classmethod
    def get_create_schema_sql(cls, schema):
        return 'create schema {}'.format(schema)

    @classmethod
    def drop_schema(cls, profile, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)

        existing = cls.query_for_existing(profile, [schema], model_name)

        for name, relation_type in existing.items():
            cls.drop_relation(
                profile, schema, name, relation_type, model_name)

        sql = 'drop schema if exists {}'.format(schema)
        return cls.add_query(profile, sql, model_name)

    @classmethod
    def quote(cls, identifier):
        if re.match('^\[.*\]$', identifier):
            return identifier

        return '[{}]'.format(identifier)

    @classmethod
    def add_begin_query(cls, profile, name):
        pass
