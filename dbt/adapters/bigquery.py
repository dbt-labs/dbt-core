from __future__ import absolute_import

from contextlib import contextmanager

import dbt.exceptions
import dbt.flags as flags
import dbt.materializers

from dbt.adapters.postgres import PostgresAdapter
from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger


class BigQueryAdapter(PostgresAdapter):

    QUERY_TIMEOUT = 60 * 1000
    requires = {'bigquery': 'google-cloud-bigquery==0.24.0'}

    @classmethod
    def initialize(cls):
        import importlib

        google = importlib.import_module('google')
        google.cloud = importlib.import_module('google.cloud')
        google.cloud.bigquery = importlib.import_module('google.cloud.bigquery')
        google.cloud.exceptions = importlib.import_module('google.cloud.exceptions')

        globals()['google'] = google

    @classmethod
    def get_materializer(cls, node, existing):
        # use InPlaceMaterializer b/c BigQuery doesn't have transactions
        # and can't rename views
        materializer = dbt.materializers.NonDDLMaterializer
        return dbt.materializers.make_materializer(materializer,
                                                   cls,
                                                   node,
                                                   existing)

    @classmethod
    @contextmanager
    def exception_handler(cls, profile, sql, model_name=None,
                          connection_name='master'):
        connection = cls.get_connection(profile, connection_name)

        try:
            yield
        except google.cloud.exceptions.BadRequest as e:
            logger.debug("Bad request while running:\n{}".format(sql))
            logger.debug(e)
            error_msg = "\n".join([error['message'] for error in e.errors])
            raise dbt.exceptions.RuntimeException(error_msg)

        except Exception as e:
            logger.debug("Unhandled error while running:\n{}".format(sql))
            logger.debug(e)
            raise dbt.exceptions.RuntimeException(e)

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
            handle = google.cloud.bigquery.Client(
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

    # hack because of current API limitations
    @classmethod
    def format_sql_for_bigquery(cls, sql):
        return "#standardSQL\n{}".format(sql)

    @classmethod
    def execute_model(cls, profile, model):
        connection = cls.get_connection(profile, model.get('name'))

        if flags.STRICT_MODE:
            validate_connection(connection)

        model_name = model.get('name')
        model_sql = cls.format_sql_for_bigquery(model.get('injected_sql'))

        schema = cls.get_default_schema(profile)
        dataset = cls.get_dataset(profile, schema, model_name)

        view = dataset.table(model_name)
        view.view_query = model_sql

        logger.debug("Model SQL ({}):\n{}".format(model_name, model_sql))

        with cls.exception_handler(profile, model_sql, model_name, model_name):
            view.create()

        if view.created is None:
            raise RuntimeError("Error creating view {}".format(model_name))

        return "CREATE VIEW"

    @classmethod
    def fetch_query_results(cls, query):
        all_rows = []

        rows = query.rows
        token = query.page_token

        while True:
            all_rows.extend(rows)
            if token is None:
                break
            rows, total_count, token = query.fetch_data(page_token=token)
        return rows

    @classmethod
    def execute_and_fetch(cls, profile, sql, model_name=None, **kwargs):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        formatted_sql = cls.format_sql_for_bigquery(sql)
        query = client.run_sync_query(formatted_sql)
        query.timeout_ms = cls.QUERY_TIMEOUT
        logger.debug("Fetching data for query {}:\n{}".format(model_name, formatted_sql))
        query.run()

        return cls.fetch_query_results(query)

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
