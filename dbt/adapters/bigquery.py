from __future__ import absolute_import

import re

from google.cloud import bigquery as bigquery_client

from contextlib import contextmanager

import dbt.exceptions
import dbt.flags as flags

from dbt.adapters.postgres import PostgresAdapter
from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger


class BigQueryAdapter(PostgresAdapter):

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
            handle = bigquery_client.Client(
                project=credentials.get('Project', None),
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
                    for table in results]

        return dict(existing)

    @classmethod
    def rename(cls, profile, from_name, to_name, model_name=None):
        raise Exception("Not implemented")
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

        # WTF do i do here??

        return super(PostgresAdapter, cls).execute_model(
            profile, model)

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
        all_datasets = bigquery_client.list_datasets()
        return any([ds.name == schema for ds in all_datasets])

    @classmethod
    def get_dataset(cls, profile, dataset_name, model_name=None):
        conn = cls.get_connection(profile, model_name)

        client = conn.get('handle')
        dataset = client.dataset(dataset_name)
        return dataset

    @classmethod
    def get_connection(cls, profile, model_name=None, auto_begin=True):
        connection = cls.get_connection(profile, model_name)
        connection_name = connection.get('name')

        logger.debug('Using {} connection "{}".'
                     .format(cls.type(), connection_name))

        return connection

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
