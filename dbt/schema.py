
from dbt.logger import GLOBAL_LOGGER as logger

import psycopg2
import logging
import time
import re

SCHEMA_PERMISSION_DENIED_MESSAGE = """
The user '{user}' does not have sufficient permissions to create the schema
'{schema}'. Either create the schema manually, or adjust the permissions of
the '{user}' user."""

RELATION_PERMISSION_DENIED_MESSAGE = """
The user '{user}' does not have sufficient permissions to create the model
'{model}' in the schema '{schema}'. Please adjust the permissions of the
'{user}' user on the '{schema}' schema. With a superuser account, execute the
following commands, then re-run dbt.

grant usage, create on schema "{schema}" to "{user}";
grant select, insert, delete on all tables in schema "{schema}" to "{user}";"""

RELATION_NOT_OWNER_MESSAGE = """
The user '{user}' does not have sufficient permissions to drop the model
'{model}' in the schema '{schema}'. This is likely because the relation was
created by a different user. Either delete the model "{schema}"."{model}"
manually, or adjust the permissions of the '{user}' user in the '{schema}'
schema."""

READ_PERMISSION_DENIED_ERROR = """
Encountered an error while executing model '{model}'.
> {error}
Check that the user '{user}' has sufficient permissions to read from all
necessary source tables"""


class Column(object):
    def __init__(self, column, dtype, char_size):
        self.column = column
        self.dtype = dtype
        self.char_size = char_size

    @property
    def name(self):
        return self.column

    @property
    def quoted(self):
        return '"{}"'.format(self.column)

    @property
    def data_type(self):
        if self.is_string():
            return Column.string_type(self.string_size())
        else:
            return self.dtype

    def is_string(self):
        return self.dtype in ['text', 'character varying']

    def string_size(self):
        if not self.is_string():
            raise RuntimeError("Called string_size() on non-string field!")

        if self.dtype == 'text' or self.char_size is None:
            # char_size should never be None. Handle it reasonably just in case
            return 255
        else:
            return int(self.char_size)

    def can_expand_to(self, other_column):
        """returns True if this column can be expanded to the size of the
        other column"""
        if not self.is_string() or not other_column.is_string():
            return False

        return other_column.string_size() > self.string_size()

    @classmethod
    def string_type(cls, size):
        return "character varying({})".format(size)

    def __repr__(self):
        return "<Column {} ({})>".format(self.name, self.data_type)


class Schema(object):
    def __init__(self, project, target):
        self.project = project
        self.target = target

        self.schema_cache = {}

    # used internally
    def cache_table_columns(self, schema, table, columns):
        tid = (schema, table)

        if tid not in self.schema_cache:
            self.schema_cache[tid] = columns

        return tid

    # used internally
    def get_table_columns_if_cached(self, schema, table):
        tid = (schema, table)
        return self.schema_cache.get(tid, None)

    # archival
    def create_schema(self, schema_name):
        target_cfg = self.project.run_environment()
        user = target_cfg['user']

        try:
            self.execute(
                'create schema if not exists "{}"'.format(schema_name))
        except psycopg2.ProgrammingError as e:
            if "permission denied for" in e.diag.message_primary:
                raise RuntimeError(
                    SCHEMA_PERMISSION_DENIED_MESSAGE.format(
                        schema=schema_name, user=user))
            else:
                raise e

    # used internally
    def execute(self, sql):
        with self.target.get_handle() as handle:
            with handle.cursor() as cursor:
                try:
                    logger.debug("SQL: %s", sql)
                    pre = time.time()
                    cursor.execute(sql)
                    post = time.time()
                    logger.debug(
                        "SQL status: %s in %0.2f seconds",
                        cursor.statusmessage, post-pre)
                    return cursor.statusmessage
                except Exception as e:
                    self.target.rollback()
                    logger.exception("Error running SQL: %s", sql)
                    logger.debug("rolling back connection")
                    raise e

    # testrunner
    def execute_and_fetch(self, sql):
        with self.target.get_handle() as handle:
            with handle.cursor() as cursor:
                try:
                    logger.debug("SQL: %s", sql)
                    pre = time.time()
                    cursor.execute(sql)
                    post = time.time()
                    logger.debug(
                        "SQL status: %s in %0.2f seconds",
                        cursor.statusmessage, post-pre)
                    data = cursor.fetchall()
                    logger.debug("SQL response: %s", data)
                    return data
                except Exception as e:
                    self.target.rollback()
                    logger.exception("Error running SQL: %s", sql)
                    logger.debug("rolling back connection")
                    raise e

    # used internally
    def execute_and_handle_permissions(self, query, model_name):
        try:
            return self.execute(query)
        except psycopg2.ProgrammingError as e:
            error_data = {"model": model_name,
                          "schema": self.target.schema,
                          "user": self.target.user}
            if 'must be owner of relation' in e.diag.message_primary:
                raise RuntimeError(
                    RELATION_NOT_OWNER_MESSAGE.format(**error_data))
            elif "permission denied for" in e.diag.message_primary:
                raise RuntimeError(
                    RELATION_PERMISSION_DENIED_MESSAGE.format(**error_data))
            else:
                raise e

    # archival via get_columns_in_table
    def sql_columns_in_table(self, schema_name, table_name):
        sql = ("""
                select column_name, data_type, character_maximum_length
                from information_schema.columns
                where table_name = '{table_name}'"""
               .format(table_name=table_name).strip())

        if schema_name is not None:
            sql += (" AND table_schema = '{schema_name}'"
                    .format(schema_name=schema_name))

        return sql

    # archival
    def get_columns_in_table(self, schema_name, table_name, use_cached=True):
        logger.debug("getting columns in table %s.%s", schema_name, table_name)

        columns = self.get_table_columns_if_cached(schema_name, table_name)
        if columns is not None and use_cached:
            logger.debug("Found columns (in cache): %s", columns)
            return columns

        sql = self.sql_columns_in_table(schema_name, table_name)
        results = self.execute_and_fetch(sql)

        columns = []
        for result in results:
            column, data_type, char_size = result
            col = Column(column, data_type, char_size)
            columns.append(col)

        self.cache_table_columns(schema_name, table_name, columns)

        logger.debug("Found columns: %s", columns)
        return columns

    # archival
    def create_table(self, schema, table, columns, sort, dist):
        fields = ['"{field}" {data_type}'.format(
            field=column.name, data_type=column.data_type
        ) for column in columns]
        fields_csv = ",\n  ".join(fields)
        dist = self.target.dist_qualifier(dist)
        sort = self.target.sort_qualifier('compound', sort)
        sql = 'create table if not exists "{schema}"."{table}" (\n  {fields}\n) {dist} {sort};'.format(schema=schema, table=table, fields=fields_csv, sort=sort, dist=dist)  # noqa
        logger.debug('creating table "%s"."%s"'.format(schema, table))
        self.execute_and_handle_permissions(sql, table)
