from __future__ import absolute_import

from contextlib import contextmanager

import dbt.exceptions
import dbt.flags as flags
import dbt.materializers
import dbt.clients.gcloud

from dbt.adapters.postgres import PostgresAdapter
from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger


class BigQueryAdapter(PostgresAdapter):

    QUERY_TIMEOUT = 60 * 1000
    requires = {'bigquery': 'google-cloud-bigquery==0.24.0'}

    @classmethod
    def initialize(cls):
        google = cls._import('google')
        google.auth = cls._import('google.auth')
        google.oauth2 = cls._import('google.oauth2')

        google.cloud = cls._import('google.cloud')
        google.cloud.bigquery = cls._import('google.cloud.bigquery')
        google.cloud.exceptions = cls._import('google.cloud.exceptions')

        globals()['google'] = google

    @classmethod
    def get_materializer(cls, node, existing):
        materializer = dbt.materializers.NonDDLMaterializer
        return dbt.materializers.make_materializer(materializer,
                                                   cls,
                                                   node,
                                                   existing)

    @classmethod
    def handle_error(cls, error, message, sql):
        logger.debug(message.format(sql=sql))
        logger.debug(error)
        error_msg = "\n".join([error['message'] for error in error.errors])
        raise dbt.exceptions.RuntimeException(error_msg)

    @classmethod
    @contextmanager
    def exception_handler(cls, profile, sql, model_name=None,
                          connection_name='master'):
        try:
            yield

        except google.cloud.exceptions.BadRequest as e:
            message = "Bad request while running:\n{sql}"
            cls.handle_error(e, message, sql)

        except google.cloud.exceptions.Forbidden as e:
            message = "Access denied while running:\n{sql}"
            cls.handle_error(e, message, sql)

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
    def get_bigquery_credentials(cls, config):
        method = config.get('method')
        creds = google.oauth2.service_account.Credentials

        if method == 'oauth':
            return None

        elif method == 'service-account':
            keyfile = config.get('keyfile')
            return creds.from_service_account_file(keyfile)

        elif method == 'service-account-json':
            details = config.get('config')
            return creds.from_service_account_info(details)

        error = ('Invalid `method` in profile: "{}"'.format(method))
        raise dbt.exceptions.FailedToConnectException(error)

    @classmethod
    def get_bigquery_client(cls, config):
        project_name = config.get('project')
        creds = cls.get_bigquery_credentials(config)

        return google.cloud.bigquery.Client(project=project_name,
                                            credentials=creds)

    @classmethod
    def open_connection(cls, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()
        credentials = connection.get('credentials', {})

        try:
            handle = cls.get_bigquery_client(credentials)

        except google.auth.exceptions.DefaultCredentialsError as e:
            logger.info("Please log into GCP to continue")
            dbt.clients.gcloud.setup_default_credentials()

            handle = cls.get_bigquery_client(credentials)

        except Exception as e:
            logger.debug("Got an error when attempting to create a bigquery "
                         "client: '{}'".format(e))

            result['handle'] = None
            result['state'] = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        result['handle'] = handle
        result['state'] = 'open'
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
        message = 'Cannot rename bigquery relation {} to {}'.format(
                  from_name, to_name)
        raise dbt.exceptions.NotImplementedException(message)

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

        debug_message = "Fetching data for query {}:\n{}"
        logger.debug(debug_message.format(model_name, formatted_sql))

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

        with cls.exception_handler(profile, 'create dataset', model_name):
            dataset.create()

    @classmethod
    def check_schema_exists(cls, profile, schema, model_name=None):
        conn = cls.get_connection(profile, model_name)

        client = conn.get('handle')

        with cls.exception_handler(profile, 'create dataset', model_name):
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
        raise dbt.exceptions.NotImplementedException(
            '`add_query` is not implemented for this adapter!')

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
