import abc
import time

import agate
import six

import dbt.clients.agate_helper
import dbt.exceptions
import dbt.flags
from dbt.adapters.base import BaseAdapter, available
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.compat import abstractclassmethod


class SQLAdapter(BaseAdapter):
    """The default adapter with the common agate conversions and some SQL
    methods implemented. This adapter has a different (much shorter) list of
    methods to implement, but it may not be possible to implement all of them
    on all databases.

    Methods to implement:
        - exception_handler
        - type
        - date_function
        - list_schemas
        - list_relations_without_caching
        - get_columns_in_relation_sql

    """
    @available
    def add_query(self, sql, model_name=None, auto_begin=True, bindings=None,
                  abridge_sql_log=False):
        """Add a query to the current transaction. A thin wrapper around
        ConnectionManager.add_query.

        :param str sql: The SQL query to add
        :param Optional[str] model_name: The name of the connection the
            transaction is on
        :param bool auto_begin: If set and there is no transaction in progress,
            begin a new one.
        :param Optional[List[object]]: An optional list of bindings for the
            query.
        :param bool abridge_sql_log: If set, limit the raw sql logged to 512
            characters
        """
        return self.connections.add_query(sql, model_name, auto_begin,
                                          bindings, abridge_sql_log)

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "text"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float8" if decimals else "integer"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "boolean"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "timestamp without time zone"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"

    @classmethod
    def is_cancelable(cls):
        return True

    def expand_column_types(self, goal, current, model_name=None):
        reference_columns = {
            c.name: c for c in
            self.get_columns_in_relation(goal, model_name)
        }

        target_columns = {
            c.name: c for c
            in self.get_columns_in_relation(current, model_name)
        }

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)

            if target_column is not None and \
               target_column.can_expand_to(reference_column):
                col_string_size = reference_column.string_size()
                new_type = self.Column.string_type(col_string_size)
                logger.debug("Changing col type from %s to %s in table %s",
                             target_column.data_type, new_type, current)

                self.alter_column_type(current, column_name,
                                       new_type, model_name)

    def drop_relation(self, relation, model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.drop(relation)
        if relation.type is None:
            dbt.exceptions.raise_compiler_error(
                'Tried to drop relation {}, but its type is null.'
                .format(relation))

        sql = 'drop {} if exists {} cascade'.format(relation.type, relation)

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)

    def alter_column_type(self, relation, column_name, new_column_type,
                          model_name=None):
        """
        1. Create a new column (w/ temp name and correct type)
        2. Copy data over to it
        3. Drop the existing column (cascade!)
        4. Rename the new column to existing column
        """

        opts = {
            "relation": relation,
            "old_column": column_name,
            "tmp_column": "{}__dbt_alter".format(column_name),
            "dtype": new_column_type
        }

        sql = """
        alter table {relation} add column "{tmp_column}" {dtype};
        update {relation} set "{tmp_column}" = "{old_column}";
        alter table {relation} drop column "{old_column}" cascade;
        alter table {relation} rename column "{tmp_column}" to "{old_column}";
        """.format(**opts).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name)

        return connection, cursor

    def truncate_relation(self, relation, model_name=None):
        sql = 'truncate table {}'.format(relation)

        connection, cursor = self.add_query(sql, model_name)

    def rename_relation(self, from_relation, to_relation, model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.rename(from_relation, to_relation)
        sql = 'alter table {} rename to {}'.format(
            from_relation, to_relation.include(database=False, schema=False))

        connection, cursor = self.add_query(sql, model_name)

    @abstractclassmethod
    def get_columns_in_relation_sql(cls, relation):
        """Return the sql string to execute on this adapter that will return
        information about the columns in this relation. The query should result
        in a table with the following type information:

            column_name: text
            data_type: text
            character_maximum_length: number
            numeric_precision: text

        numeric_precision should be two integers separated by a comma,
        representing the precision and the scale, respectively.

        :param self.Relation relation: The relation to get columns for.
        :return: The column information query
        :rtype: str
        """
        raise dbt.exceptions.NotImplementedException(
            '`get_columns_in_relation_sql` is not implemented for this '
            'adapter!'
        )

    def get_columns_in_relation(self, relation, model_name=None):
        sql = self.get_columns_in_relation_sql(relation)
        connection, cursor = self.add_query(sql, model_name)

        data = cursor.fetchall()
        columns = []

        for row in data:
            name, data_type, char_size, numeric_size = row
            column = self.Column(name, data_type, char_size, numeric_size)
            columns.append(column)

        return columns

    def _create_schema_sql(self, database, schema):
        schema = self.quote_as_configured(schema, 'schema')
        database = self.quote_as_configured(database, 'database')
        return 'create schema if not exists {database}.{schema}'.format(
            database=database, schema=schema
        )

    def _drop_schema_sql(self, database, schema):
        schema = self.quote_as_configured(schema, 'schema')
        database = self.quote_as_configured(database, 'database')
        return 'drop schema if exists {database}.{schema} cascade'.format(
            database=database, schema=schema
        )

    def create_schema(self, database, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)

        sql = self._create_schema_sql(database, schema)
        res = self.add_query(sql, model_name)

        self.commit_if_has_connection(model_name)

        return res

    def drop_schema(self, database, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)

        sql = self._drop_schema_sql(database, schema)

        return self.add_query(sql, model_name)

    def quote(cls, identifier):
        return '"{}"'.format(identifier)
