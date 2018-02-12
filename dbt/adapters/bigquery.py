from __future__ import absolute_import

from contextlib import contextmanager

import dbt.compat
import dbt.exceptions
import dbt.flags as flags
import dbt.clients.gcloud

from dbt.adapters.postgres import PostgresAdapter
from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger

import google.auth
import google.oauth2
import google.cloud.exceptions
import google.cloud.bigquery

import time


class BigQueryAdapter(PostgresAdapter):

    context_functions = [
        "query_for_existing",
        "execute_model",
        "drop",
        "execute",
        "quote_schema_and_table",
        "make_date_partitioned_table"
    ]

    SCOPE = ('https://www.googleapis.com/auth/bigquery',
             'https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/drive')

    QUERY_TIMEOUT = 300

    @classmethod
    def handle_error(cls, error, message, sql):
        logger.debug(message.format(sql=sql))
        logger.debug(error)
        error_msg = "\n".join(
            [item['message'] for item in error.errors])

        raise dbt.exceptions.DatabaseException(error_msg)

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
            raise dbt.exceptions.RuntimeException(dbt.compat.to_string(e))

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
    def commit(cls, profile, connection):
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
            credentials, project_id = google.auth.default(scopes=cls.SCOPE)
            return credentials

        elif method == 'service-account':
            keyfile = config.get('keyfile')
            return creds.from_service_account_file(keyfile, scopes=cls.SCOPE)

        elif method == 'service-account-json':
            details = config.get('keyfile_json')
            return creds.from_service_account_info(details, scopes=cls.SCOPE)

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
    def query_for_existing(cls, profile, schemas, model_name=None):
        if not isinstance(schemas, (list, tuple)):
            schemas = [schemas]

        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        all_tables = []
        for schema in schemas:
            dataset = cls.get_dataset(profile, schema, model_name)
            all_tables.extend(client.list_tables(dataset))

        relation_types = {
            'TABLE': 'table',
            'VIEW': 'view',
            'EXTERNAL': 'external'
        }

        existing = [(table.table_id, relation_types.get(table.table_type))
                    for table in all_tables]

        return dict(existing)

    @classmethod
    def drop(cls, profile, schema, relation, relation_type, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset = cls.get_dataset(profile, schema, model_name)
        relation_object = dataset.table(relation)
        client.delete_table(relation_object)

    @classmethod
    def rename(cls, profile, schema, from_name, to_name, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`rename` is not implemented for this adapter!')

    @classmethod
    def get_timeout(cls, conn):
        credentials = conn['credentials']
        return credentials.get('timeout_seconds', cls.QUERY_TIMEOUT)

    @classmethod
    def materialize_as_view(cls, profile, dataset, model):
        model_name = model.get('name')
        model_sql = model.get('injected_sql')

        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        view_ref = dataset.table(model_name)
        view = google.cloud.bigquery.Table(view_ref)
        view.view_query = model_sql
        view.view_use_legacy_sql = False

        logger.debug("Model SQL ({}):\n{}".format(model_name, model_sql))

        with cls.exception_handler(profile, model_sql, model_name, model_name):
            client.create_table(view)

        return "CREATE VIEW"

    @classmethod
    def poll_until_job_completes(cls, job, timeout):
        retry_count = timeout

        while retry_count > 0 and job.state != 'DONE':
            retry_count -= 1
            time.sleep(1)
            job.reload()

        if job.state != 'DONE':
            raise dbt.exceptions.RuntimeException("BigQuery Timeout Exceeded")

        elif job.error_result:
            raise job.exception()

    @classmethod
    def make_date_partitioned_table(cls, profile, dataset_name, identifier,
                                    model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset = cls.get_dataset(profile, dataset_name, identifier)
        table_ref = dataset.table(identifier)
        table = google.cloud.bigquery.Table(table_ref)
        table.partitioning_type = 'DAY'

        return client.create_table(table)

    @classmethod
    def materialize_as_table(cls, profile, dataset, model, model_sql,
                             decorator=None):
        model_name = model.get('name')

        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        if decorator is None:
            table_name = model_name
        else:
            table_name = "{}${}".format(model_name, decorator)

        table_ref = dataset.table(table_name)
        job_config = google.cloud.bigquery.QueryJobConfig()
        job_config.destination = table_ref
        job_config.write_disposition = 'WRITE_TRUNCATE'

        logger.debug("Model SQL ({}):\n{}".format(table_name, model_sql))
        query_job = client.query(model_sql, job_config=job_config)

        # this waits for the job to complete
        with cls.exception_handler(profile, model_sql, model_name, model_name):
            query_job.result(timeout=cls.get_timeout(conn))

        return "CREATE TABLE"

    @classmethod
    def execute_model(cls, profile, model, materialization, sql_override=None,
                      decorator=None, model_name=None):

        if sql_override is None:
            sql_override = model.get('injected_sql')

        if flags.STRICT_MODE:
            connection = cls.get_connection(profile, model.get('name'))
            validate_connection(connection)

        model_name = model.get('name')
        model_schema = model.get('schema')

        dataset = cls.get_dataset(profile, model_schema, model_name)

        if materialization == 'view':
            res = cls.materialize_as_view(profile, dataset, model)
        elif materialization == 'table':
            res = cls.materialize_as_table(profile, dataset, model,
                                           sql_override, decorator)
        else:
            msg = "Invalid relation type: '{}'".format(materialization)
            raise dbt.exceptions.RuntimeException(msg, model)

        return res

    @classmethod
    def execute(cls, profile, sql, model_name=None, fetch=False, **kwargs):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        debug_message = "Fetching data for query {}:\n{}"
        logger.debug(debug_message.format(model_name, sql))

        job_config = google.cloud.bigquery.QueryJobConfig()
        job_config.use_legacy_sql = False
        query_job = client.query(sql, job_config)

        # this blocks until the query has completed
        with cls.exception_handler(profile, 'create dataset', model_name):
            iterator = query_job.result()

        res = []
        if fetch:
            res = list(iterator)

        # If we get here, the query succeeded
        status = 'OK'
        return status, res

    @classmethod
    def execute_and_fetch(cls, profile, sql, model_name, auto_begin=None):
        return cls.execute(profile, sql, model_name, fetch=True)

    @classmethod
    def add_begin_query(cls, profile, name):
        raise dbt.exceptions.NotImplementedException(
            '`add_begin_query` is not implemented for this adapter!')

    @classmethod
    def create_schema(cls, profile, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)

        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset = cls.get_dataset(profile, schema, model_name)
        with cls.exception_handler(profile, 'create dataset', model_name):
            client.create_dataset(dataset)

    @classmethod
    def drop_tables_in_schema(cls, profile, dataset):
        conn = cls.get_connection(profile)
        client = conn.get('handle')

        for table in client.list_tables(dataset):
            client.delete_table(table.reference)

    @classmethod
    def drop_schema(cls, profile, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)

        if not cls.check_schema_exists(profile, schema, model_name):
            return

        conn = cls.get_connection(profile)
        client = conn.get('handle')

        dataset = cls.get_dataset(profile, schema, model_name)
        with cls.exception_handler(profile, 'drop dataset', model_name):
            cls.drop_tables_in_schema(profile, dataset)
            client.delete_dataset(dataset)

    @classmethod
    def get_existing_schemas(cls, profile, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        with cls.exception_handler(profile, 'list dataset', model_name):
            all_datasets = client.list_datasets()
            return [ds.dataset_id for ds in all_datasets]

    @classmethod
    def get_columns_in_table(cls, profile, schema_name, table_name,
                             model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`get_columns_in_table` is not implemented for this adapter!')

    @classmethod
    def check_schema_exists(cls, profile, schema, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        with cls.exception_handler(profile, 'get dataset', model_name):
            all_datasets = client.list_datasets()
            return any([ds.dataset_id == schema for ds in all_datasets])

    @classmethod
    def get_dataset(cls, profile, dataset_name, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset_ref = client.dataset(dataset_name)
        return google.cloud.bigquery.Dataset(dataset_ref)

    @classmethod
    def warning_on_hooks(cls, hook_type):
        msg = "{} is not supported in bigquery and will be ignored"
        dbt.ui.printer.print_timestamped_line(msg.format(hook_type),
                                              dbt.ui.printer.COLOR_FG_YELLOW)

    @classmethod
    def add_query(cls, profile, sql, model_name=None, auto_begin=True,
                  bindings=None):
        if model_name in ['on-run-start', 'on-run-end']:
            cls.warning_on_hooks(model_name)
        else:
            raise dbt.exceptions.NotImplementedException(
                '`add_query` is not implemented for this adapter!')

    @classmethod
    def is_cancelable(cls):
        return False

    @classmethod
    def quote(cls, identifier):
        return '`{}`'.format(identifier)

    @classmethod
    def quote_schema_and_table(cls, profile, schema, table, model_name=None):
        connection = cls.get_connection(profile)
        credentials = connection.get('credentials', {})
        project = credentials.get('project')
        return '{}.{}.{}'.format(cls.quote(project),
                                 cls.quote(schema),
                                 cls.quote(table))

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        import agate
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float64" if decimals else "int64"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "bool"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "datetime"

    @classmethod
    def create_csv_table(cls, profile, schema, table_name, agate_table):
        pass

    @classmethod
    def reset_csv_table(cls, profile, schema, table_name, agate_table,
                        full_refresh=False):
        cls.drop(profile, schema, table_name, "table")

    @classmethod
    def _agate_to_schema(cls, agate_table):
        bq_schema = []
        for idx, col_name in enumerate(agate_table.column_names):
            type_ = cls.convert_agate_type(agate_table, idx)
            bq_schema.append(
                google.cloud.bigquery.SchemaField(col_name, type_))
        return bq_schema

    @classmethod
    def load_csv_rows(cls, profile, schema, table_name, agate_table):
        bq_schema = cls._agate_to_schema(agate_table)
        dataset = cls.get_dataset(profile, schema, None)
        table = dataset.table(table_name, schema=bq_schema)
        conn = cls.get_connection(profile, None)
        client = conn.get('handle')
        with open(agate_table.original_abspath, "rb") as f:
            job = table.upload_from_file(f, "CSV", rewind=True,
                                         client=client, skip_leading_rows=1)
        with cls.exception_handler(profile, "LOAD TABLE"):
            cls.poll_until_job_completes(job, cls.get_timeout(conn))
