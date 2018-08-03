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

lock = multiprocessing.Lock()
connections_in_use = {}
connections_available = []


class DefaultAdapter(object):

    requires = {}

    context_functions = [
        "get_columns_in_table",
        "get_missing_columns",
        "expand_target_column_types",
        "create_schema",

        # deprecated -- use versions that take relations instead
        "already_exists",
        "query_for_existing",
        "rename",
        "drop",
        "truncate",

        # just deprecated. going away in a future release
        "quote_schema_and_table",

        # versions of adapter functions that take / return Relations
        "list_relations",
        "get_relation",
        "drop_relation",
        "rename_relation",
        "truncate_relation",
    ]

    profile_functions = [
        "execute",
        "add_query",
    ]

    raw_functions = [
        "get_status",
        "get_result_from_cursor",
        "quote",
        "convert_type"
    ]

    Relation = DefaultRelation
    Column = Column

    ###
    # ADAPTER-SPECIFIC FUNCTIONS -- each of these must be overridden in
    #                               every adapter
    ###
    @classmethod
    @contextmanager
    def exception_handler(cls, profile, sql, model_name=None,
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

    @classmethod
    def alter_column_type(cls, profile, project_cfg, schema, table,
                          column_name, new_column_type, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`alter_column_type` is not implemented for this adapter!')

    @classmethod
    def query_for_existing(cls, profile, project_cfg, schemas,
                           model_name=None):
        if not isinstance(schemas, (list, tuple)):
            schemas = [schemas]

        all_relations = []

        for schema in schemas:
            all_relations.extend(
                cls.list_relations(profile, project_cfg, schema, model_name))

        return {relation.identifier: relation.type
                for relation in all_relations}

    @classmethod
    def get_existing_schemas(cls, profile, project_cfg, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`get_existing_schemas` is not implemented for this adapter!')

    @classmethod
    def check_schema_exists(cls, profile, project_cfg, schema):
        raise dbt.exceptions.NotImplementedException(
            '`check_schema_exists` is not implemented for this adapter!')

    @classmethod
    def cancel_connection(cls, project_cfg, connection):
        raise dbt.exceptions.NotImplementedException(
            '`cancel_connection` is not implemented for this adapter!')

    ###
    # FUNCTIONS THAT SHOULD BE ABSTRACT
    ###
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
    def drop(cls, profile, project_cfg, schema,
             relation, relation_type, model_name=None):
        identifier = relation
        relation = cls.Relation.create(
            schema=schema,
            identifier=identifier,
            type=relation_type)

        return cls.drop_relation(profile, project_cfg, relation, model_name)

    @classmethod
    def drop_relation(cls, profile, project_cfg, relation, model_name=None):
        if relation.type is None:
            dbt.exceptions.raise_compiler_error(
                'Tried to drop relation {}, but its type is null.'
                .format(relation))

        sql = 'drop {} if exists {} cascade'.format(relation.type, relation)

        connection, cursor = cls.add_query(profile, sql, model_name,
                                           auto_begin=False)

    @classmethod
    def truncate(cls, profile, project_cfg, schema, table, model_name=None):
        relation = cls.Relation.create(
            schema=schema,
            identifier=table,
            type='table')

        return cls.truncate_relation(profile, project_cfg,
                                     relation, model_name)

    @classmethod
    def truncate_relation(cls, profile, project_cfg,
                          relation, model_name=None):
        sql = 'truncate table {}'.format(relation)

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def rename(cls, profile, project_cfg, schema,
               from_name, to_name, model_name=None):
        return cls.rename_relation(
            profile, project_cfg,
            from_relation=cls.Relation.create(
                schema=schema, identifier=from_name),
            to_relation=cls.Relation.create(
                identifier=to_name),
            model_name=model_name)

    @classmethod
    def rename_relation(cls, profile, project_cfg, from_relation,
                        to_relation, model_name=None):
        sql = 'alter table {} rename to {}'.format(
            from_relation, to_relation.include(schema=False))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def is_cancelable(cls):
        return True

    @classmethod
    def get_missing_columns(cls, profile, project_cfg,
                            from_schema, from_table,
                            to_schema, to_table,
                            model_name=None):
        """Returns dict of {column:type} for columns in from_table that are
        missing from to_table"""
        from_columns = {col.name: col for col in
                        cls.get_columns_in_table(
                            profile, project_cfg, from_schema, from_table,
                            model_name=model_name)}
        to_columns = {col.name: col for col in
                      cls.get_columns_in_table(
                          profile, project_cfg, to_schema, to_table,
                          model_name=model_name)}

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

    @classmethod
    def get_columns_in_table(cls, profile, project_cfg, schema_name,
                             table_name, database=None, model_name=None):
        sql = cls._get_columns_in_table_sql(schema_name, table_name, database)
        connection, cursor = cls.add_query(
            profile, sql, model_name)

        data = cursor.fetchall()
        columns = []

        for row in data:
            name, data_type, char_size, numeric_size = row
            column = cls.Column(name, data_type, char_size, numeric_size)
            columns.append(column)

        return columns

    @classmethod
    def _table_columns_to_dict(cls, columns):
        return {col.name: col for col in columns}

    @classmethod
    def expand_target_column_types(cls, profile, project_cfg,
                                   temp_table,
                                   to_schema, to_table,
                                   model_name=None):

        reference_columns = cls._table_columns_to_dict(
            cls.get_columns_in_table(
                profile, project_cfg, None, temp_table, model_name=model_name))

        target_columns = cls._table_columns_to_dict(
            cls.get_columns_in_table(
                profile, project_cfg, to_schema, to_table,
                model_name=model_name))

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)

            if target_column is not None and \
               target_column.can_expand_to(reference_column):
                col_string_size = reference_column.string_size()
                new_type = cls.Column.string_type(col_string_size)
                logger.debug("Changing col type from %s to %s in table %s.%s",
                             target_column.data_type,
                             new_type,
                             to_schema,
                             to_table)

                cls.alter_column_type(profile, project_cfg, to_schema,
                                      to_table, column_name, new_type,
                                      model_name)

    ###
    # RELATIONS
    ###
    @classmethod
    def list_relations(cls, profile, project_cfg, schema, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`list_relations` is not implemented for this adapter!')

    @classmethod
    def _make_match_kwargs(cls, project_cfg, schema, identifier):
        if identifier is not None and \
           project_cfg.get('quoting', {}).get('identifier') is False:
            identifier = identifier.lower()

        if schema is not None and \
           project_cfg.get('quoting', {}).get('schema') is False:
            schema = schema.lower()

        return filter_null_values({'identifier': identifier,
                                   'schema': schema})

    @classmethod
    def get_relation(cls, profile, project_cfg, schema=None, identifier=None,
                     relations_list=None, model_name=None):
        if schema is None and relations_list is None:
            raise dbt.exceptions.RuntimeException(
                'get_relation needs either a schema to query, or a list '
                'of relations to use')

        if relations_list is None:
            relations_list = cls.list_relations(
                profile, project_cfg, schema, model_name)

        matches = []

        search = cls._make_match_kwargs(project_cfg, schema, identifier)

        for relation in relations_list:
            if relation.matches(**search):
                matches.append(relation)

        if len(matches) > 1:
            dbt.exceptions.get_relation_returned_multiple_results(
                {'identifier': identifier, 'schema': schema}, matches)

        elif matches:
            return matches[0]

        return None

    ###
    # SANE ANSI SQL DEFAULTS
    ###
    @classmethod
    def get_create_schema_sql(cls, project_cfg, schema):
        if project_cfg.get('quoting', {}).get('schema', True):
            schema = cls.quote(schema)

        return ('create schema if not exists {schema}'
                .format(schema=schema))

    @classmethod
    def get_drop_schema_sql(cls, project_cfg, schema):
        if project_cfg.get('quoting', {}).get('schema', True):
            schema = cls.quote(schema)

        return ('drop schema if exists {schema} cascade'
                .format(schema=schema))

    ###
    # ODBC FUNCTIONS -- these should not need to change for every adapter,
    #                   although some adapters may override them
    ###
    @classmethod
    def get_default_schema(cls, profile, project_cfg):
        return profile.get('schema')

    @classmethod
    def get_connection(cls, profile, name=None, recache_if_missing=True):
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
                     .format(cls.type(), name))

        connection = cls.acquire_connection(profile, name)
        connections_in_use[name] = connection

        return cls.get_connection(profile, name)

    @classmethod
    def cancel_open_connections(cls, profile):
        global connections_in_use

        for name, connection in connections_in_use.items():
            if name == 'master':
                continue

            cls.cancel_connection(profile, connection)
            yield name

    @classmethod
    def total_connections_allocated(cls):
        global connections_in_use, connections_available

        return len(connections_in_use) + len(connections_available)

    @classmethod
    def acquire_connection(cls, profile, name):
        global connections_available, lock

        # we add a magic number, 2 because there are overhead connections,
        # one for pre- and post-run hooks and other misc operations that occur
        # before the run starts, and one for integration tests.
        max_connections = profile.get('threads', 1) + 2

        try:
            lock.acquire()
            num_allocated = cls.total_connections_allocated()

            if len(connections_available) > 0:
                logger.debug('Re-using an available connection from the pool.')
                to_return = connections_available.pop()
                to_return['name'] = name
                return to_return

            elif num_allocated >= max_connections:
                raise dbt.exceptions.InternalException(
                    'Tried to request a new connection "{}" but '
                    'the maximum number of connections are already '
                    'allocated!'.format(name))

            logger.debug('Opening a new connection ({} currently allocated)'
                         .format(num_allocated))

            credentials = copy.deepcopy(profile)

            credentials.pop('type', None)
            credentials.pop('threads', None)

            result = {
                'type': cls.type(),
                'name': name,
                'state': 'init',
                'transaction_open': False,
                'handle': None,
                'credentials': credentials
            }

            if dbt.flags.STRICT_MODE:
                Connection(**result)

            return cls.open_connection(result)
        finally:
            lock.release()

    @classmethod
    def release_connection(cls, profile, name='master'):
        global connections_in_use, connections_available, lock

        if connections_in_use.get(name) is None:
            return

        to_release = cls.get_connection(profile, name,
                                        recache_if_missing=False)

        try:
            lock.acquire()

            if to_release.get('state') == 'open':

                if to_release.get('transaction_open') is True:
                    cls.rollback(to_release)

                to_release['name'] = None
                connections_available.append(to_release)
            else:
                cls.close(to_release)

            del connections_in_use[name]
        finally:
            lock.release()

    @classmethod
    def cleanup_connections(cls):
        global connections_in_use, connections_available, lock

        try:
            lock.acquire()

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

        finally:
            lock.release()

    @classmethod
    def reload(cls, connection):
        return cls.get_connection(connection.get('credentials'),
                                  connection.get('name'))

    @classmethod
    def add_begin_query(cls, profile, name):
        return cls.add_query(profile, 'BEGIN', name, auto_begin=False)

    @classmethod
    def add_commit_query(cls, profile, name):
        return cls.add_query(profile, 'COMMIT', name, auto_begin=False)

    @classmethod
    def begin(cls, profile, name='master'):
        global connections_in_use
        connection = cls.get_connection(profile, name)

        if dbt.flags.STRICT_MODE:
            Connection(**connection)

        if connection['transaction_open'] is True:
            raise dbt.exceptions.InternalException(
                'Tried to begin a new transaction on connection "{}", but '
                'it already had one open!'.format(connection.get('name')))

        cls.add_begin_query(profile, name)

        connection['transaction_open'] = True
        connections_in_use[name] = connection

        return connection

    @classmethod
    def commit_if_has_connection(cls, profile, name):
        global connections_in_use

        if name is None:
            name = 'master'

        if connections_in_use.get(name) is None:
            return

        connection = cls.get_connection(profile, name, False)

        return cls.commit(profile, connection)

    @classmethod
    def commit(cls, profile, connection):
        global connections_in_use

        if dbt.flags.STRICT_MODE:
            Connection(**connection)

        connection = cls.reload(connection)

        if connection['transaction_open'] is False:
            raise dbt.exceptions.InternalException(
                'Tried to commit transaction on connection "{}", but '
                'it does not have one open!'.format(connection.get('name')))

        logger.debug('On {}: COMMIT'.format(connection.get('name')))
        cls.add_commit_query(profile, connection.get('name'))

        connection['transaction_open'] = False
        connections_in_use[connection.get('name')] = connection

        return connection

    @classmethod
    def rollback(cls, connection):
        if dbt.flags.STRICT_MODE:
            Connection(**connection)

        connection = cls.reload(connection)

        if connection['transaction_open'] is False:
            raise dbt.exceptions.InternalException(
                'Tried to rollback transaction on connection "{}", but '
                'it does not have one open!'.format(connection.get('name')))

        logger.debug('On {}: ROLLBACK'.format(connection.get('name')))
        connection.get('handle').rollback()

        connection['transaction_open'] = False
        connections_in_use[connection.get('name')] = connection

        return connection

    @classmethod
    def close(cls, connection):
        if dbt.flags.STRICT_MODE:
            Connection(**connection)

        connection.get('handle').close()
        connection['state'] = 'closed'

        return connection

    @classmethod
    def add_query(cls, profile, sql, model_name=None, auto_begin=True,
                  bindings=None, abridge_sql_log=False):
        connection = cls.get_connection(profile, model_name)
        connection_name = connection.get('name')

        if auto_begin and connection['transaction_open'] is False:
            cls.begin(profile, connection_name)

        logger.debug('Using {} connection "{}".'
                     .format(cls.type(), connection_name))

        with cls.exception_handler(profile, sql, model_name, connection_name):
            if abridge_sql_log:
                logger.debug('On %s: %s....', connection_name, sql[0:512])
            else:
                logger.debug('On %s: %s', connection_name, sql)
            pre = time.time()

            cursor = connection.get('handle').cursor()
            cursor.execute(sql, bindings)

            logger.debug("SQL status: %s in %0.2f seconds",
                         cls.get_status(cursor), (time.time() - pre))

            return connection, cursor

    @classmethod
    def clear_transaction(cls, profile, conn_name='master'):
        conn = cls.begin(profile, conn_name)
        cls.commit(profile, conn)
        return conn_name

    @classmethod
    def execute_one(cls, profile, sql, model_name=None, auto_begin=False):
        cls.get_connection(profile, model_name)

        return cls.add_query(profile, sql, model_name, auto_begin)

    @classmethod
    def execute_and_fetch(cls, profile, sql, model_name=None,
                          auto_begin=False):
        _, cursor = cls.execute_one(profile, sql, model_name, auto_begin)

        status = cls.get_status(cursor)
        table = cls.get_result_from_cursor(cursor)
        return status, table

    @classmethod
    def execute(cls, profile, sql, model_name=None, auto_begin=False,
                fetch=False):
        if fetch:
            return cls.execute_and_fetch(profile, sql, model_name, auto_begin)
        else:
            _, cursor = cls.execute_one(profile, sql, model_name, auto_begin)
            status = cls.get_status(cursor)
            return status, dbt.clients.agate_helper.empty_table()

    @classmethod
    def execute_all(cls, profile, sqls, model_name=None):
        connection = cls.get_connection(profile, model_name)

        if len(sqls) == 0:
            return connection

        for i, sql in enumerate(sqls):
            connection, _ = cls.add_query(profile, sql, model_name)

        return connection

    @classmethod
    def create_schema(cls, profile, project_cfg, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)
        sql = cls.get_create_schema_sql(project_cfg, schema)
        res = cls.add_query(profile, sql, model_name)

        cls.commit_if_has_connection(profile, model_name)

        return res

    @classmethod
    def drop_schema(cls, profile, project_cfg, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)
        sql = cls.get_drop_schema_sql(project_cfg, schema)
        return cls.add_query(profile, sql, model_name)

    @classmethod
    def already_exists(cls, profile, project_cfg,
                       schema, table, model_name=None):
        relation = cls.get_relation(
            profile, project_cfg, schema=schema, identifier=table)
        return relation is not None

    @classmethod
    def quote(cls, identifier):
        return '"{}"'.format(identifier)

    @classmethod
    def quote_schema_and_table(cls, profile, project_cfg,
                               schema, table, model_name=None):
        return '{}.{}'.format(cls.quote(schema),
                              cls.quote(table))

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
    @classmethod
    def run_operation(cls, profile, project_cfg, manifest, operation_name,
                      result_key):
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
            project_cfg,
            manifest.to_flat_graph(),
        )

        operation.generator(context)()

        result = context['load_result'](result_key)
        return result

    ###
    # Abstract methods involving the manifest
    ###
    @classmethod
    def get_catalog(cls, profile, project_cfg, manifest):
        raise dbt.exceptions.NotImplementedException(
            '`get_catalog` is not implemented for this adapter!')
