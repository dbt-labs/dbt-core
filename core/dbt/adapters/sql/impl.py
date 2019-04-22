import agate

import dbt.clients.agate_helper
import dbt.exceptions
import dbt.flags
from dbt.adapters.base import BaseAdapter, available
from dbt.logger import GLOBAL_LOGGER as logger


LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
GET_COLUMNS_IN_RELATION_MACRO_NAME = 'get_columns_in_relation'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
CHECK_SCHEMA_EXISTS_MACRO_NAME = 'check_schema_exists'
CREATE_SCHEMA_MACRO_NAME = 'create_schema'
DROP_SCHEMA_MACRO_NAME = 'drop_schema'
RENAME_RELATION_MACRO_NAME = 'rename_relation'
TRUNCATE_RELATION_MACRO_NAME = 'truncate_relation'
DROP_RELATION_MACRO_NAME = 'drop_relation'
ALTER_COLUMN_TYPE_MACRO_NAME = 'alter_column_type'


class SQLAdapter(BaseAdapter):
    """The default adapter with the common agate conversions and some SQL
    methods implemented. This adapter has a different much shorter list of
    methods to implement, but some more macros that must be implemented.

    To implement a macro, implement "${adapter_type}__${macro_name}". in the
    adapter's internal project.

    Methods to implement:
        - date_function

    Macros to implement:
        - get_catalog
        - list_relations_without_caching
        - get_columns_in_relation
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
            self.get_columns_in_relation(goal, model_name=model_name)
        }

        target_columns = {
            c.name: c for c
            in self.get_columns_in_relation(current, model_name=model_name)
        }

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)

            if target_column is not None and \
               target_column.can_expand_to(reference_column):
                col_string_size = reference_column.string_size()
                new_type = self.Column.string_type(col_string_size)
                logger.debug("Changing col type from %s to %s in table %s",
                             target_column.data_type, new_type, current)

                self.alter_column_type(current, column_name, new_type,
                                       model_name=model_name)

        if model_name is None:
            self.release_connection('master')

    def alter_column_type(self, relation, column_name, new_column_type,
                          model_name=None):
        """
        1. Create a new column (w/ temp name and correct type)
        2. Copy data over to it
        3. Drop the existing column (cascade!)
        4. Rename the new column to existing column
        """
        kwargs = {
            'relation': relation,
            'column_name': column_name,
            'new_column_type': new_column_type,
        }
        self.execute_macro(
            ALTER_COLUMN_TYPE_MACRO_NAME,
            kwargs=kwargs,
            connection_name=model_name
        )

    def drop_relation(self, relation, model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.drop(relation)
        if relation.type is None:
            dbt.exceptions.raise_compiler_error(
                'Tried to drop relation {}, but its type is null.'
                .format(relation))

        self.execute_macro(
            DROP_RELATION_MACRO_NAME,
            kwargs={'relation': relation},
            connection_name=model_name
        )

    def truncate_relation(self, relation, model_name=None):
        self.execute_macro(
            TRUNCATE_RELATION_MACRO_NAME,
            kwargs={'relation': relation},
            connection_name=model_name
        )

    def rename_relation(self, from_relation, to_relation, model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.rename(from_relation, to_relation)

        kwargs = {'from_relation': from_relation, 'to_relation': to_relation}
        self.execute_macro(
            RENAME_RELATION_MACRO_NAME,
            kwargs=kwargs,
            connection_name=model_name
        )

    def get_columns_in_relation(self, relation, model_name=None):
        return self.execute_macro(
            GET_COLUMNS_IN_RELATION_MACRO_NAME,
            kwargs={'relation': relation},
            connection_name=model_name
        )

    def create_schema(self, database, schema, model_name=None):
        logger.debug('Creating schema "%s"."%s".', database, schema)
        if model_name is None:
            model_name = 'master'
        kwargs = {
            'database_name': self.quote_as_configured(database, 'database'),
            'schema_name': self.quote_as_configured(schema, 'schema'),
        }
        self.execute_macro(CREATE_SCHEMA_MACRO_NAME,
                           kwargs=kwargs,
                           connection_name=model_name)
        self.commit_if_has_connection(model_name)

    def drop_schema(self, database, schema, model_name=None):
        logger.debug('Dropping schema "%s"."%s".', database, schema)
        kwargs = {
            'database_name': self.quote_as_configured(database, 'database'),
            'schema_name': self.quote_as_configured(schema, 'schema'),
        }
        self.execute_macro(DROP_SCHEMA_MACRO_NAME,
                           kwargs=kwargs,
                           connection_name=model_name)

    def list_relations_without_caching(self, information_schema, schema,
                                       model_name=None):
        kwargs = {'information_schema': information_schema, 'schema': schema}
        results = self.execute_macro(
            LIST_RELATIONS_MACRO_NAME,
            kwargs=kwargs,
            connection_name=model_name,
            release=True
        )

        relations = []
        quote_policy = {
            'database': True,
            'schema': True,
            'identifier': True
        }
        for _database, name, _schema, _type in results:
            relations.append(self.Relation.create(
                database=_database,
                schema=_schema,
                identifier=name,
                quote_policy=quote_policy,
                type=_type
            ))
        return relations

    def quote(cls, identifier):
        return '"{}"'.format(identifier)

    def list_schemas(self, database, model_name=None):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME,
            kwargs={'database': database},
            connection_name=model_name,
            # release when the model_name is none, as that implies we were
            # called by node_runners.py.
            release=(model_name is None)
        )

        return [row[0] for row in results]

    def check_schema_exists(self, database, schema, model_name=None):
        information_schema = self.Relation.create(
            database=database, schema=schema,
            quote_policy=self.config.quoting
        ).information_schema()

        kwargs = {'information_schema': information_schema, 'schema': schema}
        results = self.execute_macro(
            CHECK_SCHEMA_EXISTS_MACRO_NAME,
            kwargs=kwargs,
            connection_name=model_name
        )
        return results[0][0] > 0
