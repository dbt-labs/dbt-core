
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
        self.runtime_existing = self.query_for_existing(self.target.schema)

    def cache_table_columns(self, schema, table, columns):
        tid = (schema, table)

        if tid not in self.schema_cache:
            self.schema_cache[tid] = columns

        return tid

    def get_table_columns_if_cached(self, schema, table):
        tid = (schema, table)
        return self.schema_cache.get(tid, None)

    def get_schemas(self):
        existing = []
        results = self.execute_and_fetch(
            'select nspname from pg_catalog.pg_namespace')
        return [name for (name,) in results]

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

    def query_for_existing(self, schema):
        sql = """
            select tablename as name, 'table' as type from pg_tables where schemaname = '{schema}'
                union all
            select viewname as name, 'view' as type from pg_views where schemaname = '{schema}' """.format(schema=schema)  # noqa

        results = self.execute_and_fetch(sql)
        existing = [(name, relation_type) for (name, relation_type) in results]

        return dict(existing)

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
                    logger.debug("Error running SQL: %s", sql)
                    logger.debug("rolling back connection")
                    raise e

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
                    logger.debug("Error running SQL: %s", sql)
                    logger.debug("rolling back connection")
                    raise e

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

    def execute_without_auto_commit(self, sql, handle=None):
        if handle is None:
            handle = self.target.get_handle()

        cursor = handle.cursor()

        try:
            logger.debug("SQL: %s", sql)
            pre = time.time()
            cursor.execute(sql)
            post = time.time()
            logger.debug(
                "SQL status: %s in %0.2f seconds",
                cursor.statusmessage, post-pre)
            return handle, cursor.statusmessage
        except Exception as e:
            self.target.rollback()
            logger.debug("Error running SQL: %s", sql)
            logger.debug("rolling back connection")
            raise e
        finally:
            cursor.close()

    def truncate(self, schema, relation):
        sql = ('truncate table "{schema}"."{relation}"'
               .format(schema=schema, relation=relation))
        logger.debug("dropping table %s.%s", schema, relation)
        self.execute_and_handle_permissions(sql, relation)
        logger.debug("dropped %s.%s", schema, relation)

    def drop(self, schema, relation_type, relation):
        sql = ('drop {relation_type} if exists "{schema}"."{relation}" cascade'
               .format(
                   schema=schema,
                   relation_type=relation_type,
                   relation=relation))
        logger.debug("dropping %s %s.%s", relation_type, schema, relation)
        self.execute_and_handle_permissions(sql, relation)
        logger.debug("dropped %s %s.%s", relation_type, schema, relation)

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

    def rename(self, schema, from_name, to_name):
        rename_query = 'alter table "{schema}"."{from_name}" rename to "{to_name}"'.format(schema=schema, from_name=from_name, to_name=to_name)  # noqa
        logger.debug(
            "renaming model %s.%s --> %s.%s",
            schema, from_name, schema, to_name)
        self.execute_and_handle_permissions(rename_query, from_name)
        logger.debug(
            "renamed model %s.%s --> %s.%s",
            schema, from_name, schema, to_name)

    def get_missing_columns(self, from_schema, from_table, to_schema,
                            to_table):
        """Returns dict of {column:type} for columns in from_table that are
        missing from to_table"""
        from_columns = {col.name: col for col in
                        self.get_columns_in_table(from_schema, from_table)}
        to_columns = {col.name: col for col in
                      self.get_columns_in_table(to_schema, to_table)}

        missing_columns = set(from_columns.keys()) - set(to_columns.keys())

        return [col for (col_name, col) in from_columns.items()
                if col_name in missing_columns]

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

    def create_schema_if_not_exists(self, schema_name):
        schemas = self.get_schemas()

        if schema_name not in schemas:
            self.create_schema(schema_name)

    def alter_column_type(self, schema, table, column_name, new_column_type):
        """
        1. Create a new column (w/ temp name and correct type)
        2. Copy data over to it
        3. Drop the existing column (cascade!)
        4. Rename the new column to existing column
        """

        opts = {
            "schema": schema,
            "table": table,
            "old_column": column_name,
            "tmp_column": "{}__dbt_alter".format(column_name),
            "dtype": new_column_type
        }

        sql = """
        alter table "{schema}"."{table}" add column "{tmp_column}" {dtype};
        update "{schema}"."{table}" set "{tmp_column}" = "{old_column}";
        alter table "{schema}"."{table}" drop column "{old_column}" cascade;
        alter table "{schema}"."{table}" rename column "{tmp_column}" to "{old_column}";
        """.format(**opts)  # noqa

        status = self.execute(sql)
        return status

    def expand_column_types_if_needed(self, temp_table, to_schema, to_table):
        source_columns = {col.name: col for col in
                          self.get_columns_in_table(None, temp_table)}
        dest_columns = {col.name: col for col in
                        self.get_columns_in_table(to_schema, to_table)}

        for column_name, source_column in source_columns.items():
            dest_column = dest_columns.get(column_name)

            if dest_column is not None and \
               dest_column.can_expand_to(source_column):
                new_type = Column.string_type(source_column.string_size())
                logger.debug("Changing col type from %s to %s in table %s.%s",
                             dest_column.data_type,
                             new_type,
                             to_schema,
                             to_table)
                self.alter_column_type(
                    to_schema, to_table, column_name, new_type)

        # update these cols in the cache! This is a hack to fix broken
        # incremental models for type expansion. TODO
        self.cache_table_columns(to_schema, to_table, source_columns)

    def table_exists(self, schema, table):
        if schema == self.target.schema:
            exists = self.runtime_existing.get(table) is not None
            return exists
        else:
            tables = self.query_for_existing(schema)
            exists = tables.get(table) is not None
            return exists
