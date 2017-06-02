from __future__ import absolute_import

import re


from contextlib import contextmanager

import dbt.exceptions
import dbt.flags as flags

from dbt.adapters.postgres import PostgresAdapter
from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger


class BigQueryAdapter(PostgresAdapter):

    requires = {'bigquery': 'google-cloud-bigquery==0.24.0'}

    @classmethod
    def initialize(cls):
        import importlib
        globals()['bigquery'] = importlib.import_module('google.cloud.bigquery')

    @classmethod
    @contextmanager
    def exception_handler(cls, profile, sql, model_name=None,
                          connection_name='master'):
        connection = cls.get_connection(profile, connection_name)

        try:
            yield
        #except snowflake.connector.errors.ProgrammingError as e:
        #    logger.debug('Snowflake error: {}'.format(str(e)))

        #    if 'Empty SQL statement' in e.msg:
        #        logger.debug("got empty sql statement, moving on")
        #    elif 'This session does not have a current database' in e.msg:
        #        cls.rollback(connection)
        #        raise dbt.exceptions.FailedToConnectException(
        #            ('{}\n\nThis error sometimes occurs when invalid '
        #             'credentials are provided, or when your default role '
        #             'does not have access to use the specified database. '
        #             'Please double check your profile and try again.')
        #            .format(str(e)))
        #    else:
        #        cls.rollback(connection)
        #        raise dbt.exceptions.ProgrammingException(str(e))
        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            cls.rollback(connection)
            raise e

    @classmethod
    def type(cls):
        return 'bigquery'

    @classmethod
    def date_function(cls):
        return 'CURRENT_TIMESTAMP()'

    @classmethod
    def begin(cls, profile, name='master'):
        pass

    @classmethod
    def commit(cls, connection):
        pass

    @classmethod
    def get_status(cls, cursor):
        raise Exception("Not implemented")
        state = cursor.sqlstate

        if state is None:
            state = 'SUCCESS'

        return "{} {}".format(state, cursor.rowcount)

    @classmethod
    def open_connection(cls, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()

        try:
            credentials = connection.get('credentials', {})
            handle = bigquery.Client(
                project = credentials.get('project', None),
            )

            result['handle'] = handle
            result['state'] = 'open'
        except Exception as e: # TODO
            logger.debug("Got an error when attempting to create a bigquery "
                         "client: '{}'"
                         .format(e))

            result['handle'] = None
            result['state'] = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return result

    @classmethod
    def query_for_existing(cls, profile, schema, model_name=None):
        dataset = cls.get_dataset(profile, schema, model_name)
        tables = dataset.list_tables()

        relation_type_lookup = {
            'TABLE': 'table',
            'VIEW': 'view'
        }

        existing = [(table.name, relation_type_lookup.get(table.table_type))
                    for table in tables]

        return dict(existing)

    @classmethod
    def drop_view(cls, profile, view_name, model_name):
        schema = cls.get_default_schema(profile)
        dataset = cls.get_dataset(profile, schema, model_name)
        view = dataset.table(view_name)
        view.delete()

    @classmethod
    def rename(cls, profile, from_name, to_name, model_name=None):
        return # TODO
        schema = cls.get_default_schema(profile)

        sql = (('alter table "{schema}"."{from_name}" '
                'rename to "{schema}"."{to_name}"')
               .format(schema=schema,
                       from_name=from_name,
                       to_name=to_name))

        connection, cursor = cls.add_query(profile, sql, model_name)

    @classmethod
    def execute_model(cls, profile, model):
        connection = cls.get_connection(profile, model.get('name'))

        if flags.STRICT_MODE:
            validate_connection(connection)

        model_name = model.get('name')

        # TODO: injected_sql?
        model_sql = "#standardSQL\n{}".format(model.get('compiled_sql'))

        schema = cls.get_default_schema(profile)
        dataset = cls.get_dataset(profile, schema, model_name)

        view = dataset.table(model_name)
        view.view_query = model_sql

        try:
            view.create()
        except Exception as err:
            errors = "\n".join([e['message'] for e in err.errors])
            raise RuntimeError(errors)

        if view.created is None:
            raise RuntimeError("Error creating view {}".format(model_name))

        return "CREATE VIEW"

    @classmethod
    def add_begin_query(cls, profile, name):
        raise Exception("not implemented")
        return cls.add_query(profile, 'BEGIN', name, auto_begin=False,
                             select_schema=False)

    @classmethod
    def create_schema(cls, profile, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)
        dataset = cls.get_dataset(profile, schema, model_name)
        dataset.create()

    @classmethod
    def check_schema_exists(cls, profile, schema, model_name=None):
        conn = cls.get_connection(profile, model_name)

        client = conn.get('handle')
        all_datasets = client.list_datasets()
        return any([ds.name == schema for ds in all_datasets])

    @classmethod
    def get_dataset(cls, profile, dataset_name, model_name=None):
        conn = cls.get_connection(profile, model_name)

        client = conn.get('handle')
        dataset = client.dataset(dataset_name)
        return dataset

    @classmethod
    def add_query(cls, profile, sql, model_name=None, auto_begin=True):
        raise Exception("Not implemented")

    @classmethod
    def cancel_connection(cls, profile, connection):
        raise Exception("Not implemented")
        handle = connection['handle']
        sid = handle.session_id

        connection_name = connection.get('name')

        sql = 'select system$abort_session({})'.format(sid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, sid))

        _, cursor = cls.add_query(profile, sql, 'master')
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))

    @classmethod
    def quote_schema_and_table(cls, profile, schema, table):
        connection = cls.get_connection(profile)
        credentials = connection.get('credentials', {})
        project = credentials.get('project')
        return '`{}`.`{}`.`{}`'.format(project, schema, table)
