import copy
import multiprocessing
import re
import time
import yaml

from contextlib import contextmanager

import dbt.exceptions
import dbt.flags
import dbt.materializers

from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.schema import Column


lock = multiprocessing.Lock()
connections_in_use = {}
connections_available = []


class DefaultAdapter(object):

    requires = {}

    @classmethod
    def get_materializer(cls, model, existing):
        return dbt.materializers.get_materializer(cls, model, existing)

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
    def dist_qualifier(cls):
        raise dbt.exceptions.NotImplementedException(
            '`dist_qualifier` is not implemented for this adapter!')

    @classmethod
    def sort_qualifier(cls):
        raise dbt.exceptions.NotImplementedException(
            '`sort_qualifier` is not implemented for this adapter!')

    @classmethod
    def get_status(cls, cursor):
        raise dbt.exceptions.NotImplementedException(
            '`get_status` is not implemented for this adapter!')

    @classmethod
    def alter_column_type(cls, profile, schema, table, column_name,
                          new_column_type, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`alter_column_type` is not implemented for this adapter!')

    @classmethod
    def query_for_existing(cls, profile, schema, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`query_for_existing` is not implemented for this adapter!')

    @classmethod
    def check_schema_exists(cls, profile, schema):
        raise dbt.exceptions.NotImplementedException(
            '`check_schema_exists` is not implemented for this adapter!')

    @classmethod
    def cancel_connection(cls, project, connection):
        raise dbt.exceptions.NotImplementedException(
            '`cancel_connection` is not implemented for this adapter!')

    ###
    # FUNCTIONS THAT SHOULD BE ABSTRACT
    ###
    @classmethod
    def drop(cls, profile, relation, relation_type, model_name=None):
        if relation_type == 'view':
            return cls.drop_view(profile, relation, model_name)
        elif relation_type == 'table':
            return cls.drop_table(profile, relation, model_name)
        else:
            raise RuntimeError(
                "Invalid relation_type '{}'"
                .format(relation_type))

    @classmethod
    def drop_view(cls, profile, view, model_name):
        schema = cls.get_default_schema(profile)

        sql = ('drop view if exists "{schema}"."{view}" cascade'
               .format(schema=schema,
                       view=view))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def drop_table(cls, profile, table, model_name):
        schema = cls.get_default_schema(profile)

        sql = ('drop table if exists "{schema}"."{table}" cascade'
               .format(schema=schema,
                       table=table))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def truncate(cls, profile, table, model_name=None):
        schema = cls.get_default_schema(profile)

        sql = ('truncate table "{schema}"."{table}"'
               .format(schema=schema,
                       table=table))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def rename(cls, profile, from_name, to_name, model_name=None):
        schema = cls.get_default_schema(profile)

        sql = ('alter table "{schema}"."{from_name}" rename to "{to_name}"'
               .format(schema=schema,
                       from_name=from_name,
                       to_name=to_name))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def is_cancelable(cls):
        return True

    @classmethod
    def execute_model(cls, profile, model):
        parts = re.split(r'-- (DBT_OPERATION .*)', model.get('wrapped_sql'))

        for i, part in enumerate(parts):
            matches = re.match(r'^DBT_OPERATION ({.*})$', part)
            if matches is not None:
                instruction_string = matches.groups()[0]
                instruction = yaml.safe_load(instruction_string)
                function = instruction['function']
                kwargs = instruction['args']

                def call_expand_target_column_types(kwargs):
                    kwargs.update({'profile': profile,
                                   'model_name': model.get('name')})
                    return cls.expand_target_column_types(**kwargs)

                func_map = {
                    'expand_column_types_if_needed':
                    call_expand_target_column_types
                }

                func_map[function](kwargs)
            else:
                connection, cursor = cls.add_query(
                    profile, part, model.get('name'))

        return cls.get_status(cursor)

    @classmethod
    def get_missing_columns(cls, profile,
                            from_schema, from_table,
                            to_schema, to_table,
                            model_name=None):
        """Returns dict of {column:type} for columns in from_table that are
        missing from to_table"""
        from_columns = {col.name: col for col in
                        cls.get_columns_in_table(
                            profile, from_schema, from_table, model_name)}
        to_columns = {col.name: col for col in
                      cls.get_columns_in_table(
                          profile, to_schema, to_table, model_name)}

        missing_columns = set(from_columns.keys()) - set(to_columns.keys())

        return [col for (col_name, col) in from_columns.items()
                if col_name in missing_columns]

    @classmethod
    def get_columns_in_table(cls, profile, schema_name, table_name,
                             model_name=None):
        sql = """
        select column_name, data_type, character_maximum_length
        from information_schema.columns
        where table_name = '{table_name}'
        """.format(table_name=table_name).strip()

        if schema_name is not None:
            sql += (" AND table_schema = '{schema_name}'"
                    .format(schema_name=schema_name))

        connection, cursor = cls.add_query(
            profile, sql, model_name)

        data = cursor.fetchall()
        columns = []

        for row in data:
            name, data_type, char_size = row
            column = Column(name, data_type, char_size)
            columns.append(column)

        return columns

    @classmethod
    def expand_target_column_types(cls, profile,
                                   temp_table,
                                   to_schema, to_table,
                                   model_name=None):

        reference_columns = {col.name: col for col in
                             cls.get_columns_in_table(
                                 profile, None, temp_table, model_name)}
        target_columns = {col.name: col for col in
                          cls.get_columns_in_table(
                              profile, to_schema, to_table, model_name)}

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)

            if target_column is not None and \
               target_column.can_expand_to(reference_column):
                new_type = Column.string_type(reference_column.string_size())
                logger.debug("Changing col type from %s to %s in table %s.%s",
                             target_column.data_type,
                             new_type,
                             to_schema,
                             to_table)

                cls.alter_column_type(profile, to_schema, to_table,
                                      column_name, new_type, model_name)

    ###
    # SANE ANSI SQL DEFAULTS
    ###
    @classmethod
    def get_create_schema_sql(cls, schema):
        return ('create schema if not exists "{schema}"'
                .format(schema=schema))

    @classmethod
    def get_drop_schema_sql(cls, schema):
        return ('drop schema if exists "{schema} cascade"'
                .format(schema=schema))

    @classmethod
    def get_create_table_sql(cls, schema, table, columns, sort, dist):
        fields = ['"{field}" {data_type}'.format(
            field=column.name, data_type=column.data_type
        ) for column in columns]
        fields_csv = ",\n  ".join(fields)
        dist = cls.dist_qualifier(dist)
        sort = cls.sort_qualifier('compound', sort)
        sql = """
        create table if not exists "{schema}"."{table}" (
        {fields}
        )
        {dist} {sort}
        """.format(
            schema=schema,
            table=table,
            fields=fields_csv,
            sort=sort,
            dist=dist).strip()
        return sql

    ###
    # ODBC FUNCTIONS -- these should not need to change for every adapter,
    #                   although some adapters may override them
    ###
    @classmethod
    def get_default_schema(cls, profile):
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
                validate_connection(result)

            return cls.open_connection(result)
        finally:
            lock.release()

    @classmethod
    def release_connection(cls, profile, name):
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
        global connections_in_use, connections_available

        try:
            lock.acquire()

            for name, connection in connections_in_use.items():
                if connection.get('state') != 'closed':
                    logger.debug("Connection '{}' was left open."
                                 .format(name))
                else:
                    logger.debug("Connection '{}' was properly closed."
                                 .format(name))

            # garbage collect, but don't close them in case someone
            # still has a handle
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
    def begin(cls, profile, name='master'):
        global connections_in_use
        connection = cls.get_connection(profile, name)

        if dbt.flags.STRICT_MODE:
            validate_connection(connection)

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

        return cls.commit(connection)

    @classmethod
    def commit(cls, connection):
        global connections_in_use

        if dbt.flags.STRICT_MODE:
            validate_connection(connection)

        connection = cls.reload(connection)

        if connection['transaction_open'] is False:
            raise dbt.exceptions.InternalException(
                'Tried to commit transaction on connection "{}", but '
                'it does not have one open!'.format(connection.get('name')))

        logger.debug('On {}: COMMIT'.format(connection.get('name')))
        connection.get('handle').commit()

        connection['transaction_open'] = False
        connections_in_use[connection.get('name')] = connection

        return connection

    @classmethod
    def rollback(cls, connection):
        if dbt.flags.STRICT_MODE:
            validate_connection(connection)

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
            validate_connection(connection)

        connection = cls.reload(connection)

        if connection.get('state') == 'closed':
            return connection

        connection.get('handle').close()

        connection['state'] = 'closed'
        connections_in_use[connection.get('name')] = connection

        return connection

    @classmethod
    def add_query(cls, profile, sql, model_name=None, auto_begin=True):
        connection = cls.get_connection(profile, model_name)
        connection_name = connection.get('name')

        if auto_begin and connection['transaction_open'] is False:
            cls.begin(profile, connection_name)

        logger.debug('Using {} connection "{}".'
                     .format(cls.type(), connection_name))

        with cls.exception_handler(profile, sql, model_name, connection_name):
            logger.debug('On %s: %s', connection_name, sql)
            pre = time.time()

            cursor = connection.get('handle').cursor()
            cursor.execute(sql)

            logger.debug("SQL status: %s in %0.2f seconds",
                         cls.get_status(cursor), (time.time() - pre))

            return connection, cursor

    @classmethod
    def execute_one(cls, profile, sql, model_name=None, auto_begin=False):
        cls.get_connection(profile, model_name)

        return cls.add_query(profile, sql, model_name, auto_begin)

    @classmethod
    def execute_and_fetch(cls, profile, sql, model_name=None,
                          auto_begin=False):
        _, cursor = cls.execute_one(profile, sql, model_name, auto_begin)

        return cursor.fetchall()

    @classmethod
    def execute_all(cls, profile, sqls, model_name=None):
        connection = cls.get_connection(profile, model_name)

        if len(sqls) == 0:
            return connection

        for i, sql in enumerate(sqls):
            connection, _ = cls.add_query(profile, sql, model_name)

        return connection

    @classmethod
    def create_schema(cls, profile, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)
        sql = cls.get_create_schema_sql(schema)
        res = cls.add_query(profile, sql, model_name)

        cls.commit_if_has_connection(profile, model_name)

        return res

    @classmethod
    def drop_schema(cls, profile, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)
        sql = cls.get_drop_schema_sql(schema)
        return cls.add_query(profile, sql, model_name)

    @classmethod
    def create_table(cls, profile, schema, table, columns, sort, dist,
                     model_name=None):
        logger.debug('Creating table "%s"."%s".', schema, table)
        sql = cls.get_create_table_sql(schema, table, columns, sort, dist)
        return cls.add_query(profile, sql, model_name)

    @classmethod
    def table_exists(cls, profile, schema, table, model_name=None):
        tables = cls.query_for_existing(profile, schema, model_name)
        exists = tables.get(table) is not None
        return exists

    @classmethod
    def already_exists(cls, profile, schema, table, model_name=None):
        """
        Alias for `table_exists`.
        """
        return cls.table_exists(profile, schema, table, model_name)

    @classmethod
    def quote_schema_and_table(cls, profile, schema, table):
        return '"{}"."{}"'.format(schema, table)
