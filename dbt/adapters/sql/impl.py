import abc
import time

import six

from dbt.adapters.base import BaseAdapter
# a temporary evil, until connection cleanup
from dbt.adapters.base.impl import connections_in_use
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.compat import abstractclassmethod

import dbt.clients.agate_helper
import dbt.exceptions
import dbt.flags


class SQLAdapter(BaseAdapter):
    """The default adapter with the common agate conversions and some SQL
    methods implemented. This adapter has a different (much shorter) list of
    methods to implement, but it may not be possible to implement all of them
    on all databases.

    Methods to implement:
        - exception_handler
        - type
        - date_function
        - get_existing_schemas
        - list_relations_without_caching
        - cancel_connection
        - get_status
        - get_columns_in_relation_sql

    """
    config_functions = BaseAdapter.config_functions[:] + [
        'add_query',
    ]

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
    def get_result_from_cursor(cls, cursor):
        data = []
        column_names = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            raw_results = cursor.fetchall()
            data = [dict(zip(column_names, row))
                    for row in raw_results]

        return dbt.clients.agate_helper.table_from_data(data, column_names)

    @classmethod
    def is_cancelable(cls):
        return True

    @abc.abstractmethod
    def cancel_connection(connection):
        """Cancel the given connection.

        :param Connection connection: The connection to cancel.
        """
        raise dbt.exceptions.NotImplementedException(
            '`cancel_connection` is not implemented for this adapter!'
        )

    def cancel_open_connections(self):
        global connections_in_use

        for name, connection in connections_in_use.items():
            if name == 'master':
                continue

            self.cancel_connection(connection)
            yield name

    def add_query(self, sql, model_name=None, auto_begin=True,
                  bindings=None, abridge_sql_log=False):
        connection = self.get_connection(model_name)
        connection_name = connection.name

        if auto_begin and connection.transaction_open is False:
            self.begin(connection_name)

        logger.debug('Using {} connection "{}".'
                     .format(self.type(), connection_name))

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

    def execute(self, sql, model_name=None, auto_begin=False,
                fetch=False):
        self.get_connection(model_name)
        _, cursor = self.add_query(sql, model_name, auto_begin)
        status = self.get_status(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return status, table

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
            from_relation, to_relation.include(schema=False))

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

    def create_schema(self, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)
        schema = self.quote_as_configured(schema, 'schema')

        sql = 'create schema if not exists {schema}'.format(schema=schema)
        res = self.add_query(sql, model_name)

        self.commit_if_has_connection(model_name)

        return res

    def drop_schema(self, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)
        schema = self.quote_as_configured(schema, 'schema')

        sql = 'drop schema if exists {schema} cascade'.format(schema=schema)
        return self.add_query(sql, model_name)

    def quote(cls, identifier):
        return '"{}"'.format(identifier)

    def add_begin_query(self, name):
        return self.add_query('BEGIN', name, auto_begin=False)

    def add_commit_query(self, name):
        return self.add_query('COMMIT', name, auto_begin=False)

    def begin(self, name):
        global connections_in_use
        connection = self.get_connection(name)

        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        if connection.transaction_open is True:
            raise dbt.exceptions.InternalException(
                'Tried to begin a new transaction on connection "{}", but '
                'it already had one open!'.format(connection.get('name')))

        self.add_begin_query(name)

        connection.transaction_open = True
        connections_in_use[name] = connection

        return connection

    def commit(self, connection):
        global connections_in_use

        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        connection = self.reload(connection)

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to commit transaction on connection "{}", but '
                'it does not have one open!'.format(connection.name))

        logger.debug('On {}: COMMIT'.format(connection.name))
        self.add_commit_query(connection.name)

        connection.transaction_open = False
        connections_in_use[connection.name] = connection

        return connection
