import copy
import multiprocessing
import time
import agate

from contextlib import contextmanager

import dbt.exceptions
import dbt.flags
import dbt.schema
import dbt.clients.agate_helper

from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.schema import Column
from dbt.utils import filter_null_values

from dbt.adapters.default.relation import DefaultRelation
from dbt.adapters.cache import RelationsCache

GET_CATALOG_OPERATION_NAME = 'get_catalog_data'

lock = multiprocessing.Lock()
connections_in_use = {}
connections_available = []


def _expect_row_value(key, row):
    if key not in row.keys():
        raise dbt.exceptions.InternalException(
            'Got a row without "{}" column, columns: {}'
            .format(key, row.keys())
        )
    return row[key]


def _relations_filter_schemas(schemas):
    def test(row):
        referenced_schema = _expect_row_value('referenced_schema', row)
        dependent_schema = _expect_row_value('dependent_schema', row)
        # handle the null schema
        if referenced_schema is not None:
            referenced_schema = referenced_schema.lower()
        if dependent_schema is not None:
            dependent_schema = dependent_schema.lower()
        return referenced_schema in schemas or dependent_schema in schemas
    return test


def _catalog_filter_schemas(manifest):
    """Return a function that takes a row and decides if the row should be
    included in the catalog output.
    """
    schemas = frozenset(s.lower() for s in manifest.get_used_schemas())

    def test(row):
        table_schema = _expect_row_value('table_schema', row)
        # the schema may be present but None, which is not an error and should
        # be filtered out
        if table_schema is None:
            return False
        return table_schema.lower() in schemas
    return test


class DefaultAdapter(object):
    requires = {}

    config_functions = [
        "get_columns_in_table",
        "get_missing_columns",
        "expand_target_column_types",
        "create_schema",
        "quote_as_configured",
        "cache_new_relation",

        # deprecated -- use versions that take relations instead
        "already_exists",
        "query_for_existing",
        "rename",
        "drop",
        "truncate",

        # just deprecated. going away in a future release
        "quote_schema_and_table",

        # versions of adapter functions that take / return Relations
        "get_relation",
        "drop_relation",
        "rename_relation",
        "truncate_relation",

        # formerly profile functions
        "execute",
        "add_query",
    ]

    raw_functions = [
        "get_status",
        "get_result_from_cursor",
        "quote",
        "convert_type",
    ]
    Relation = DefaultRelation
    Column = Column

    def __init__(self, config):
        self.config = config
        self.cache = RelationsCache()

    ###
    # ADAPTER-SPECIFIC FUNCTIONS -- each of these must be overridden in
    #                               every adapter
    ###
    @contextmanager
    def exception_handler(self, sql, model_name=None,
                          connection_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`exception_handler` is not implemented for this adapter!')

    @classmethod
    def type(cls):
        raise dbt.exceptions.NotImplementedException(
            '`type` is not implemented for this adapter!')

    @classmethod
    def date_function(cls):
        raise dbt.exceptions.NotImplementedException(
            '`date_function` is not implemented for this adapter!')

    @classmethod
    def get_status(cls, cursor):
        raise dbt.exceptions.NotImplementedException(
            '`get_status` is not implemented for this adapter!')

    def alter_column_type(self, schema, table, column_name, new_column_type,
                          model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`alter_column_type` is not implemented for this adapter!')

    def query_for_existing(self, schemas, model_name=None):
        if not isinstance(schemas, (list, tuple)):
            schemas = [schemas]

        all_relations = []

        for schema in schemas:
            all_relations.extend(self.list_relations(schema, model_name))

        return {relation.identifier: relation.type
                for relation in all_relations}

    def get_existing_schemas(self, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`get_existing_schemas` is not implemented for this adapter!')

    def check_schema_exists(self, schema):
        raise dbt.exceptions.NotImplementedException(
            '`check_schema_exists` is not implemented for this adapter!')

    def cancel_connection(self, connection):
        raise dbt.exceptions.NotImplementedException(
            '`cancel_connection` is not implemented for this adapter!')

    ###
    # FUNCTIONS THAT SHOULD BE ABSTRACT
    ###
    def cache_new_relation(self, relation, model_name=None):
        """Cache a new relation in dbt. It will show up in `list relations`."""
        if relation is None:
            dbt.exceptions.raise_compiler_error(
                'Attempted to cache a null relation for {}'.format(model_name)
            )
        if dbt.flags.USE_CACHE:
            self.cache.add(relation)
        # so jinja doesn't render things
        return ''

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

    def drop(self, schema, relation, relation_type, model_name=None):
        identifier = relation
        relation = self.Relation.create(
            schema=schema,
            identifier=identifier,
            type=relation_type,
            quote_policy=self.config.quoting)

        return self.drop_relation(relation, model_name)

    def drop_relation(self, relation, model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.drop(relation)
        if relation.type is None:
            dbt.exceptions.raise_compiler_error(
                'Tried to drop relation {}, but its type is null.'
                .format(relation))

        sql = 'drop {} if exists {} cascade'.format(relation.type, relation)

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)

    def truncate(self, schema, table, model_name=None):
        relation = self.Relation.create(
            schema=schema,
            identifier=table,
            type='table',
            quote_policy=self.config.quoting)

        return self.truncate_relation(relation, model_name)

    def truncate_relation(self, relation, model_name=None):
        sql = 'truncate table {}'.format(relation)

        connection, cursor = self.add_query(sql, model_name)

    def rename(self, schema, from_name, to_name, model_name=None):
        quote_policy = self.config.quoting
        from_relation = self.Relation.create(
            schema=schema,
            identifier=from_name,
            quote_policy=quote_policy
        )
        to_relation = self.Relation.create(
            identifier=to_name,
            quote_policy=quote_policy
        )
        return self.rename_relation(
            from_relation=from_relation,
            to_relation=to_relation,
            model_name=model_name)

    def rename_relation(self, from_relation, to_relation,
                        model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.rename(from_relation, to_relation)
        sql = 'alter table {} rename to {}'.format(
            from_relation, to_relation.include(schema=False))

        connection, cursor = self.add_query(sql, model_name)

    @classmethod
    def is_cancelable(cls):
        return True

    def get_missing_columns(self, from_schema, from_table,
                            to_schema, to_table, model_name=None):
        """Returns dict of {column:type} for columns in from_table that are
        missing from to_table"""
        from_columns = {
            col.name: col for col in
            self.get_columns_in_table(
                from_schema, from_table,
                model_name=model_name)
        }
        to_columns = {
            col.name: col for col in
            self.get_columns_in_table(
                to_schema, to_table,
                model_name=model_name)
        }

        missing_columns = set(from_columns.keys()) - set(to_columns.keys())

        return [col for (col_name, col) in from_columns.items()
                if col_name in missing_columns]

    @classmethod
    def _get_columns_in_table_sql(cls, schema_name, table_name, database):
        schema_filter = '1=1'
        if schema_name is not None:
            schema_filter = "table_schema = '{}'".format(schema_name)

        db_prefix = '' if database is None else '{}.'.format(database)

        sql = """
        select
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision || ',' || numeric_scale as numeric_size

        from {db_prefix}information_schema.columns
        where table_name = '{table_name}'
          and {schema_filter}
        order by ordinal_position
        """.format(db_prefix=db_prefix,
                   table_name=table_name,
                   schema_filter=schema_filter).strip()

        return sql

    def get_columns_in_table(self, schema_name,
                             table_name, database=None, model_name=None):
        sql = self._get_columns_in_table_sql(schema_name, table_name, database)
        connection, cursor = self.add_query(sql, model_name)

        data = cursor.fetchall()
        columns = []

        for row in data:
            name, data_type, char_size, numeric_size = row
            column = self.Column(name, data_type, char_size, numeric_size)
            columns.append(column)

        return columns

    @classmethod
    def _table_columns_to_dict(cls, columns):
        return {col.name: col for col in columns}

    def expand_target_column_types(self,
                                   temp_table,
                                   to_schema, to_table,
                                   model_name=None):

        reference_columns = self._table_columns_to_dict(
            self.get_columns_in_table(None, temp_table, model_name=model_name)
        )

        target_columns = self._table_columns_to_dict(
            self.get_columns_in_table(to_schema, to_table,
                                      model_name=model_name)
        )

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)

            if target_column is not None and \
               target_column.can_expand_to(reference_column):
                col_string_size = reference_column.string_size()
                new_type = self.Column.string_type(col_string_size)
                logger.debug("Changing col type from %s to %s in table %s.%s",
                             target_column.data_type,
                             new_type,
                             to_schema,
                             to_table)

                self.alter_column_type(to_schema, to_table, column_name,
                                       new_type, model_name)

    ###
    # RELATIONS
    ###
    def _schema_is_cached(self, schema, model_name=None,
                          debug_on_missing=True):
        """Check if the schema is cached, and by default logs if it is not."""
        if dbt.flags.USE_CACHE is False:
            return False
        elif schema not in self.cache:
            if debug_on_missing:
                logger.debug(
                    'On "{}": cache miss for schema "{}", this is inefficient'
                    .format(model_name or '<None>', schema)
                )
            return False
        else:
            return True

    def _list_relations(self, schema, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`list_relations` is not implemented for this adapter!')

    def list_relations(self, schema, model_name=None):
        if self._schema_is_cached(schema, model_name):
            return self.cache.get_relations(schema)

        # we can't build the relations cache because we don't have a
        # manifest so we can't run any operations.
        relations = self._list_relations(schema, model_name=model_name)

        logger.debug('with schema={}, model_name={}, relations={}'
                     .format(schema, model_name, relations))
        return relations

    def _make_match_kwargs(self, schema, identifier):
        quoting = self.config.quoting
        if identifier is not None and quoting['identifier'] is False:
            identifier = identifier.lower()

        if schema is not None and quoting['schema'] is False:
            schema = schema.lower()

        return filter_null_values({'identifier': identifier,
                                   'schema': schema})

    def _make_match(self, relations_list, schema, identifier):

        matches = []

        search = self._make_match_kwargs(schema, identifier)

        for relation in relations_list:
            if relation.matches(**search):
                matches.append(relation)

        return matches

    def get_relation(self, schema, identifier, model_name=None):
        relations_list = self.list_relations(schema, model_name)

        matches = self._make_match(relations_list, schema, identifier)

        if len(matches) > 1:
            dbt.exceptions.get_relation_returned_multiple_results(
                {'identifier': identifier, 'schema': schema}, matches)

        elif matches:
            return matches[0]

        return None

    ###
    # SANE ANSI SQL DEFAULTS
    ###
    def get_create_schema_sql(self, schema):
        schema = self.quote_as_configured(schema, 'schema')

        return ('create schema if not exists {schema}'
                .format(schema=schema))

    def get_drop_schema_sql(self, schema):
        schema = self.quote_as_configured(schema, 'schema')

        return ('drop schema if exists {schema} cascade'
                .format(schema=schema))

    ###
    # ODBC FUNCTIONS -- these should not need to change for every adapter,
    #                   although some adapters may override them
    ###
    def get_default_schema(self):
        return self.config.credentials.schema

    def get_connection(self, name=None, recache_if_missing=True):
        global connections_in_use

        if name is None:
            # if a name isn't specified, we'll re-use a single handle
            # named 'master'
            name = 'master'

        if connections_in_use.get(name):
            return connections_in_use.get(name)

        if not recache_if_missing:
            raise dbt.exceptions.InternalException(
                'Tried to get a connection "{}" which does not exist '
                '(recache_if_missing is off).'.format(name))

        logger.debug('Acquiring new {} connection "{}".'
                     .format(self.type(), name))

        connection = self.acquire_connection(name)
        connections_in_use[name] = connection

        return self.get_connection(name)

    def cancel_open_connections(self):
        global connections_in_use

        for name, connection in connections_in_use.items():
            if name == 'master':
                continue

            self.cancel_connection(connection)
            yield name

    @classmethod
    def total_connections_allocated(cls):
        global connections_in_use, connections_available

        return len(connections_in_use) + len(connections_available)

    def acquire_connection(self, name):
        global connections_available, lock

        # we add a magic number, 2 because there are overhead connections,
        # one for pre- and post-run hooks and other misc operations that occur
        # before the run starts, and one for integration tests.
        max_connections = self.config.threads + 2

        with lock:
            num_allocated = self.total_connections_allocated()

            if len(connections_available) > 0:
                logger.debug('Re-using an available connection from the pool.')
                to_return = connections_available.pop()
                to_return.name = name
                return to_return

            elif num_allocated >= max_connections:
                raise dbt.exceptions.InternalException(
                    'Tried to request a new connection "{}" but '
                    'the maximum number of connections are already '
                    'allocated!'.format(name))

            logger.debug('Opening a new connection ({} currently allocated)'
                         .format(num_allocated))

            result = Connection(
                type=self.type(),
                name=name,
                state='init',
                transaction_open=False,
                handle=None,
                credentials=self.config.credentials
            )

            return self.open_connection(result)

    def release_connection(self, name):
        global connections_in_use, connections_available, lock

        with lock:

            if name not in connections_in_use:
                return

            to_release = self.get_connection(name, recache_if_missing=False)

            if to_release.state == 'open':

                if to_release.transaction_open is True:
                    self.rollback(to_release)

                to_release.name = None
                connections_available.append(to_release)
            else:
                self.close(to_release)

            del connections_in_use[name]

    @classmethod
    def cleanup_connections(cls):
        global connections_in_use, connections_available, lock

        with lock:
            for name, connection in connections_in_use.items():
                if connection.get('state') != 'closed':
                    logger.debug("Connection '{}' was left open."
                                 .format(name))
                else:
                    logger.debug("Connection '{}' was properly closed."
                                 .format(name))

            conns_in_use = list(connections_in_use.values())
            for conn in conns_in_use + connections_available:
                cls.close(conn)

            # garbage collect these connections
            connections_in_use = {}
            connections_available = []

    def reload(self, connection):
        return self.get_connection(connection.name)

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

    def commit_if_has_connection(self, name):
        global connections_in_use

        if name is None:
            name = 'master'

        if connections_in_use.get(name) is None:
            return

        connection = self.get_connection(name, False)

        return self.commit(connection)

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

    def rollback(self, connection):
        if dbt.flags.STRICT_MODE:
            Connection(**connection)

        connection = self.reload(connection)

        if connection.transaction_open is False:
            raise dbt.exceptions.InternalException(
                'Tried to rollback transaction on connection "{}", but '
                'it does not have one open!'.format(connection.name))

        logger.debug('On {}: ROLLBACK'.format(connection.name))
        connection.handle.rollback()

        connection.transaction_open = False
        connections_in_use[connection.name] = connection

        return connection

    @classmethod
    def close(cls, connection):
        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        # On windows, sometimes connection handles don't have a close() attr.
        if hasattr(connection.handle, 'close'):
            connection.handle.close()

        connection.state = 'closed'

        return connection

    def add_query(self, sql, model_name=None, auto_begin=True,
                  bindings=None, abridge_sql_log=False):
        connection = self.get_connection(model_name)
        connection_name = connection.name

        if auto_begin and connection.transaction_open is False:
            self.begin(connection_name)

        logger.debug('Using {} connection "{}".'
                     .format(self.type(), connection_name))

        with self.exception_handler(sql, model_name, connection_name):
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

    def clear_transaction(self, conn_name='master'):
        conn = self.begin(conn_name)
        self.commit(conn)
        return conn_name

    def execute_one(self, sql, model_name=None, auto_begin=False):
        self.get_connection(model_name)

        return self.add_query(sql, model_name, auto_begin)

    def execute_and_fetch(self, sql, model_name=None,
                          auto_begin=False):
        _, cursor = self.execute_one(sql, model_name, auto_begin)

        status = self.get_status(cursor)
        table = self.get_result_from_cursor(cursor)
        return status, table

    def execute(self, sql, model_name=None, auto_begin=False,
                fetch=False):
        if fetch:
            return self.execute_and_fetch(sql, model_name, auto_begin)
        else:
            _, cursor = self.execute_one(sql, model_name, auto_begin)
            status = self.get_status(cursor)
            return status, dbt.clients.agate_helper.empty_table()

    def execute_all(self, sqls, model_name=None):
        connection = self.get_connection(model_name)

        if len(sqls) == 0:
            return connection

        for i, sql in enumerate(sqls):
            connection, _ = self.add_query(sql, model_name)

        return connection

    def create_schema(self, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)
        sql = self.get_create_schema_sql(schema)
        res = self.add_query(sql, model_name)

        self.commit_if_has_connection(model_name)

        return res

    def drop_schema(self, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)
        sql = self.get_drop_schema_sql(schema)
        return self.add_query(sql, model_name)

    def already_exists(self, schema, table, model_name=None):
        relation = self.get_relation(schema=schema, identifier=table)
        return relation is not None

    @classmethod
    def quote(cls, identifier):
        return '"{}"'.format(identifier)

    def quote_as_configured(self, identifier, quote_key, model_name=None):
        """Quote or do not quote the given identifer as configured in the
        project config for the quote key.

        The quote key should be one of 'database' (on bigquery, 'profile'),
        'identifier', or 'schema', or it will be treated as if you set `True`.
        """
        default = self.Relation.DEFAULTS['quote_policy'].get(quote_key)
        if self.config.quoting.get(quote_key, default):
            return self.quote(identifier)
        else:
            return identifier

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_text_type` is not implemented for this adapter!')

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_number_type` is not implemented for this adapter!')

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_boolean_type` is not implemented for this adapter!')

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_datetime_type` is not implemented for this adapter!')

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_date_type` is not implemented for this adapter!')

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        raise dbt.exceptions.NotImplementedException(
            '`convert_time_type` is not implemented for this adapter!')

    @classmethod
    def convert_type(cls, agate_table, col_idx):
        return cls.convert_agate_type(agate_table, col_idx)

    @classmethod
    def convert_agate_type(cls, agate_table, col_idx):
        agate_type = agate_table.column_types[col_idx]
        conversions = [
            (agate.Text, cls.convert_text_type),
            (agate.Number, cls.convert_number_type),
            (agate.Boolean, cls.convert_boolean_type),
            (agate.DateTime, cls.convert_datetime_type),
            (agate.Date, cls.convert_date_type),
            (agate.TimeDelta, cls.convert_time_type),
        ]
        for agate_cls, func in conversions:
            if isinstance(agate_type, agate_cls):
                return func(agate_table, col_idx)

    ###
    # Operations involving the manifest
    ###
    def run_operation(self, manifest, operation_name):
        """Look the operation identified by operation_name up in the manifest
        and run it.

        Return an an AttrDict with three attributes: 'table', 'data', and
            'status'. 'table' is an agate.Table.
        """
        operation = manifest.find_operation_by_name(operation_name, 'dbt')

        # This causes a reference cycle, as dbt.context.runtime.generate()
        # ends up calling get_adapter, so the import has to be here.
        import dbt.context.runtime
        context = dbt.context.runtime.generate(
            operation,
            self.config,
            manifest,
        )

        result = operation.generator(context)()
        return result

    ###
    # Abstract methods involving the manifest
    ###
    @classmethod
    def _catalog_filter_table(cls, table, manifest):
        return table.where(_catalog_filter_schemas(manifest))

    def get_catalog(self, manifest):
        try:
            table = self.run_operation(manifest, GET_CATALOG_OPERATION_NAME)
        finally:
            self.release_connection(GET_CATALOG_OPERATION_NAME)

        results = self._catalog_filter_table(table, manifest)
        return results

    @classmethod
    def _relations_filter_table(cls, table, schemas):
        return table.where(_relations_filter_schemas(schemas))

    def _link_cached_relations(self, manifest, schemas):
        """This method has to exist because BigQueryAdapter and SnowflakeAdapter
        inherit from the PostgresAdapter, so they need something to override
        in order to disable linking.
        """
        pass

    def _relations_cache_for_schemas(self, manifest, schemas=None):
        if not dbt.flags.USE_CACHE:
            return

        if schemas is None:
            schemas = manifest.get_used_schemas()

        relations = []
        # add all relations
        for schema in schemas:
            # bypass the cache, of course!
            for relation in self._list_relations(schema):
                self.cache.add(relation)
        self._link_cached_relations(manifest, schemas)
        # it's possible that there were no relations in some schemas. We want
        # to insert the schemas we query into the cache's `.schemas` attribute
        # so we can check it later
        self.cache.update_schemas(schemas)

    def set_relations_cache(self, manifest, clear=False):
        """Run a query that gets a populated cache of the relations in the
        database and set the cache on this adapter.
        """
        if not dbt.flags.USE_CACHE:
            return

        with self.cache.lock:
            if clear:
                self.cache.clear()
            self._relations_cache_for_schemas(manifest)
