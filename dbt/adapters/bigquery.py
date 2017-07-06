from __future__ import absolute_import

from contextlib import contextmanager

import dbt.exceptions
import dbt.flags as flags
import dbt.materializers
import dbt.clients.gcloud

from dbt.adapters.postgres import PostgresAdapter
from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger

import google.auth
import google.oauth2
import google.cloud.exceptions
import google.cloud.bigquery


class BigQueryAdapter(PostgresAdapter):

    QUERY_TIMEOUT = 60 * 1000

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
        raise dbt.exceptions.NotImplementedException(
            '`get_status` is not implemented for this adapter!')

    @classmethod
    def get_bigquery_credentials(cls, config):
        method = config.get('method')
        creds = google.oauth2.service_account.Credentials

        if method == 'oauth':
            credentials, project_id = google.auth.default()
            return credentials

        elif method == 'service-account':
            keyfile = config.get('keyfile')
            return creds.from_service_account_file(keyfile)

        elif method == 'service-account-json':
            details = config.get('keyfile_json')
            return creds.from_service_account_info(details)

        error = ('Invalid `method` in profile: "{}"'.format(method))
        raise dbt.exceptions.FailedToConnectException(error)

    @classmethod
    def get_bigquery_client(cls, config):
        project_name = config.get('project')
        creds = cls.get_bigquery_credentials(config)

        return google.cloud.bigquery.Client(project_name, creds)

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
            raise
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
        raise dbt.exceptions.NotImplementedException(
            '`rename` is not implemented for this adapter!')

    # Hack because of current API limitations. We should set a flag on the
    # Table object indicating StandardSQL when it's implemented
    # https://github.com/GoogleCloudPlatform/google-cloud-python/issues/3388
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

        materialization = dbt.utils.get_materialization(model)
        allowed_materializations = ['view', 'ephemeral']

        if materialization not in allowed_materializations:
            msg = "Unsupported materialization: {}".format(materialization)
            raise dbt.exceptions.RuntimeException(msg)

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
        raise dbt.exceptions.NotImplementedException(
            '`add_begin_query` is not implemented for this adapter!')

    @classmethod
    def create_schema(cls, profile, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)

        dataset = cls.get_dataset(profile, schema, model_name)

        with cls.exception_handler(profile, 'create dataset', model_name):
            dataset.create()

    @classmethod
    def drop_tables_in_schema(cls, dataset):
        for table in dataset.list_tables():
            table.delete()

    @classmethod
    def drop_schema(cls, profile, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)

        if not cls.check_schema_exists(profile, schema, model_name):
            return

        dataset = cls.get_dataset(profile, schema, model_name)

        with cls.exception_handler(profile, 'drop dataset', model_name):
            cls.drop_tables_in_schema(dataset)
            dataset.delete()

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
    def warning_on_hooks(cls, hook_type):
        msg = "{} is not supported in bigquery and will be ignored"
        dbt.ui.printer.print_timestamped_line(msg.format(hook_type),
                                              dbt.ui.printer.COLOR_FG_YELLOW)

    @classmethod
    def add_query(cls, profile, sql, model_name=None, auto_begin=True):
        if model_name in ['on-run-start', 'on-run-end']:
            cls.warning_on_hooks(model_name)
        else:
            raise dbt.exceptions.NotImplementedException(
                '`add_query` is not implemented for this adapter!')

    @classmethod
    def is_cancelable(cls):
        return False

    @classmethod
    def quote_schema_and_table(cls, profile, schema, table):
        connection = cls.get_connection(profile)
        credentials = connection.get('credentials', {})
        project = credentials.get('project')
        return '`{}`.`{}`.`{}`'.format(project, schema, table)
