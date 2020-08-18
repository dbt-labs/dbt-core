from contextlib import contextmanager
from dataclasses import dataclass
from requests.exceptions import ConnectionError
from typing import Optional, Any, Dict

import google.auth
import google.auth.exceptions
import google.cloud.bigquery
import google.cloud.exceptions
from google.api_core import retry, client_info
from google.auth import impersonated_credentials
from google.oauth2 import service_account

from dbt.utils import format_bytes, format_rows_number
from dbt.clients import agate_helper, gcloud
from dbt.contracts.connection import ConnectionState
from dbt.exceptions import (
    FailedToConnectException, RuntimeException, DatabaseException
)
from dbt.adapters.base import BaseConnectionManager, Credentials
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.version import __version__ as dbt_version

from hologram.helpers import StrEnum


BQ_QUERY_JOB_SPLIT = '-----Query Job SQL Follows-----'

WRITE_TRUNCATE = google.cloud.bigquery.job.WriteDisposition.WRITE_TRUNCATE

REOPENABLE_ERRORS = (
    ConnectionResetError,
    ConnectionError,
)

RETRYABLE_ERRORS = (
    google.cloud.exceptions.ServerError,
    google.cloud.exceptions.BadRequest,
    ConnectionResetError,
    ConnectionError,
)


class Priority(StrEnum):
    Interactive = 'interactive'
    Batch = 'batch'


class BigQueryConnectionMethod(StrEnum):
    OAUTH = 'oauth'
    SERVICE_ACCOUNT = 'service-account'
    SERVICE_ACCOUNT_JSON = 'service-account-json'


@dataclass
class BigQueryCredentials(Credentials):
    method: BigQueryConnectionMethod
    keyfile: Optional[str] = None
    keyfile_json: Optional[Dict[str, Any]] = None
    timeout_seconds: Optional[int] = 300
    location: Optional[str] = None
    priority: Optional[Priority] = None
    retries: Optional[int] = 1
    maximum_bytes_billed: Optional[int] = None
    impersonate_service_account: Optional[str] = None
    _ALIASES = {
        'project': 'database',
        'dataset': 'schema',
    }

    @property
    def type(self):
        return 'bigquery'

    def _connection_keys(self):
        return ('method', 'database', 'schema', 'location', 'priority',
                'timeout_seconds', 'maximum_bytes_billed')


class BigQueryConnectionManager(BaseConnectionManager):
    TYPE = 'bigquery'

    SCOPE = ('https://www.googleapis.com/auth/bigquery',
             'https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/drive')

    QUERY_TIMEOUT = 300
    RETRIES = 1
    DEFAULT_INITIAL_DELAY = 1.0  # Seconds
    DEFAULT_MAXIMUM_DELAY = 1.0  # Seconds

    @classmethod
    def handle_error(cls, error, message):
        error_msg = "\n".join([item['message'] for item in error.errors])
        raise DatabaseException(error_msg)

    def clear_transaction(self):
        pass

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except google.cloud.exceptions.BadRequest as e:
            message = "Bad request while running query"
            self.handle_error(e, message)

        except google.cloud.exceptions.Forbidden as e:
            message = "Access denied while running query"
            self.handle_error(e, message)

        except google.auth.exceptions.RefreshError:
            message = "Unable to generate access token, if you're using " \
                      "impersonate_service_account, make sure your " \
                      'initial account has the "roles/' \
                      'iam.serviceAccountTokenCreator" role on the ' \
                      'account you are trying to impersonate.'
            raise RuntimeException(message)

        except Exception as e:
            logger.debug("Unhandled error while running:\n{}".format(sql))
            logger.debug(e)
            if isinstance(e, RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            exc_message = str(e)
            # the google bigquery library likes to add the query log, which we
            # don't want to log. Hopefully they never change this!
            if BQ_QUERY_JOB_SPLIT in exc_message:
                exc_message = exc_message.split(BQ_QUERY_JOB_SPLIT)[0].strip()
            raise RuntimeException(exc_message)

    def cancel_open(self) -> None:
        pass

    @classmethod
    def close(cls, connection):
        connection.state = ConnectionState.CLOSED

        return connection

    def begin(self):
        pass

    def commit(self):
        pass

    @classmethod
    def get_bigquery_credentials(cls, profile_credentials):
        method = profile_credentials.method
        creds = service_account.Credentials

        if method == BigQueryConnectionMethod.OAUTH:
            credentials, project_id = google.auth.default(scopes=cls.SCOPE)
            return credentials

        elif method == BigQueryConnectionMethod.SERVICE_ACCOUNT:
            keyfile = profile_credentials.keyfile
            return creds.from_service_account_file(keyfile, scopes=cls.SCOPE)

        elif method == BigQueryConnectionMethod.SERVICE_ACCOUNT_JSON:
            details = profile_credentials.keyfile_json
            return creds.from_service_account_info(details, scopes=cls.SCOPE)

        error = ('Invalid `method` in profile: "{}"'.format(method))
        raise FailedToConnectException(error)

    @classmethod
    def get_impersonated_bigquery_credentials(cls, profile_credentials):
        source_credentials = cls.get_bigquery_credentials(profile_credentials)
        return impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=profile_credentials.impersonate_service_account,
            target_scopes=list(cls.SCOPE),
            lifetime=profile_credentials.timeout_seconds,
        )

    @classmethod
    def get_bigquery_client(cls, profile_credentials):
        if profile_credentials.impersonate_service_account:
            creds =\
                cls.get_impersonated_bigquery_credentials(profile_credentials)
        else:
            creds = cls.get_bigquery_credentials(profile_credentials)
        database = profile_credentials.database
        location = getattr(profile_credentials, 'location', None)

        info = client_info.ClientInfo(user_agent=f'dbt-{dbt_version}')
        return google.cloud.bigquery.Client(
            database,
            creds,
            location=location,
            client_info=info,
        )

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        try:
            handle = cls.get_bigquery_client(connection.credentials)

        except google.auth.exceptions.DefaultCredentialsError:
            logger.info("Please log into GCP to continue")
            gcloud.setup_default_credentials()

            handle = cls.get_bigquery_client(connection.credentials)

        except Exception as e:
            raise
            logger.debug("Got an error when attempting to create a bigquery "
                         "client: '{}'".format(e))

            connection.handle = None
            connection.state = 'fail'

            raise FailedToConnectException(str(e))

        connection.handle = handle
        connection.state = 'open'
        return connection

    @classmethod
    def get_timeout(cls, conn):
        credentials = conn.credentials
        return credentials.timeout_seconds

    @classmethod
    def get_retries(cls, conn) -> int:
        credentials = conn.credentials
        if credentials.retries is not None:
            return credentials.retries
        else:
            return 1

    @classmethod
    def get_table_from_response(cls, resp):
        column_names = [field.name for field in resp.schema]
        return agate_helper.table_from_data_flat(resp, column_names)

    def raw_execute(self, sql, fetch=False):
        conn = self.get_thread_connection()
        client = conn.handle

        logger.debug('On {}: {}', conn.name, sql)

        job_params = {'use_legacy_sql': False}

        priority = conn.credentials.priority
        if priority == Priority.Batch:
            job_params['priority'] = google.cloud.bigquery.QueryPriority.BATCH
        else:
            job_params[
                'priority'] = google.cloud.bigquery.QueryPriority.INTERACTIVE

        maximum_bytes_billed = conn.credentials.maximum_bytes_billed
        if maximum_bytes_billed is not None and maximum_bytes_billed != 0:
            job_params['maximum_bytes_billed'] = maximum_bytes_billed

        def fn():
            return self._query_and_results(client, sql, conn, job_params)

        query_job, iterator = self._retry_and_handle(msg=sql, conn=conn, fn=fn)

        return query_job, iterator

    def execute(self, sql, auto_begin=False, fetch=None):
        sql = self._add_query_comment(sql)
        # auto_begin is ignored on bigquery, and only included for consistency
        query_job, iterator = self.raw_execute(sql, fetch=fetch)

        if fetch:
            res = self.get_table_from_response(iterator)
        else:
            res = agate_helper.empty_table()

        if query_job.statement_type == 'CREATE_VIEW':
            status = 'CREATE VIEW'

        elif query_job.statement_type == 'CREATE_TABLE_AS_SELECT':
            conn = self.get_thread_connection()
            client = conn.handle
            table = client.get_table(query_job.destination)
            processed = format_bytes(query_job.total_bytes_processed)
            status = 'CREATE TABLE ({} rows, {} processed)'.format(
                format_rows_number(table.num_rows),
                format_bytes(query_job.total_bytes_processed),
            )

        elif query_job.statement_type == 'SCRIPT':
            processed = format_bytes(query_job.total_bytes_processed)
            status = f'SCRIPT ({processed} processed)'

        elif query_job.statement_type in ['INSERT', 'DELETE', 'MERGE']:
            status = '{} ({} rows, {} processed)'.format(
                query_job.statement_type,
                format_rows_number(query_job.num_dml_affected_rows),
                format_bytes(query_job.total_bytes_processed),
            )

        else:
            status = 'OK'

        return status, res

    def create_bigquery_table(self, database, schema, table_name, callback,
                              sql):
        """Create a bigquery table. The caller must supply a callback
        that takes one argument, a `google.cloud.bigquery.Table`, and mutates
        it.
        """
        conn = self.get_thread_connection()
        client = conn.handle

        view_ref = self.table_ref(database, schema, table_name, conn)
        view = google.cloud.bigquery.Table(view_ref)
        callback(view)

        def fn():
            return client.create_table(view)
        self._retry_and_handle(msg=sql, conn=conn, fn=fn)

    def create_view(self, database, schema, table_name, sql):
        def callback(table):
            table.view_query = sql
            table.view_use_legacy_sql = False

        self.create_bigquery_table(database, schema, table_name, callback, sql)

    def create_table(self, database, schema, table_name, sql):
        conn = self.get_thread_connection()
        client = conn.handle

        table_ref = self.table_ref(database, schema, table_name, conn)
        job_params = {'destination': table_ref,
                      'write_disposition': WRITE_TRUNCATE}

        timeout = self.get_timeout(conn)

        def fn():
            return self._query_and_results(client, sql, conn, job_params,
                                           timeout=timeout)
        self._retry_and_handle(msg=sql, conn=conn, fn=fn)

    def create_date_partitioned_table(self, database, schema, table_name):
        def callback(table):
            table.partitioning_type = 'DAY'

        self.create_bigquery_table(database, schema, table_name, callback,
                                   'CREATE DAY PARTITIONED TABLE')

    def copy_bq_table(self, source, destination, write_disposition):
        conn = self.get_thread_connection()
        client = conn.handle

        source_ref = self.table_ref(
            source.database, source.schema, source.table, conn)
        destination_ref = self.table_ref(
            destination.database, destination.schema, destination.table, conn)

        logger.debug(
            'Copying table "{}" to "{}" with disposition: "{}"',
            source_ref.path, destination_ref.path, write_disposition)

        def copy_and_results():
            job_config = google.cloud.bigquery.CopyJobConfig(
                write_disposition=write_disposition)
            copy_job = client.copy_table(
                source_ref, destination_ref, job_config=job_config)
            iterator = copy_job.result(timeout=self.get_timeout(conn))
            return copy_job, iterator

        self._retry_and_handle(
            msg='copy table "{}" to "{}"'.format(
                source_ref.path, destination_ref.path),
            conn=conn, fn=copy_and_results)

    @staticmethod
    def dataset(database, schema, conn):
        dataset_ref = conn.handle.dataset(schema, database)
        return google.cloud.bigquery.Dataset(dataset_ref)

    @staticmethod
    def dataset_from_id(dataset_id):
        return google.cloud.bigquery.Dataset.from_string(dataset_id)

    def table_ref(self, database, schema, table_name, conn):
        dataset = self.dataset(database, schema, conn)
        return dataset.table(table_name)

    def get_bq_table(self, database, schema, identifier):
        """Get a bigquery table for a schema/model."""
        conn = self.get_thread_connection()
        table_ref = self.table_ref(database, schema, identifier, conn)
        return conn.handle.get_table(table_ref)

    def drop_dataset(self, database, schema):
        conn = self.get_thread_connection()
        dataset = self.dataset(database, schema, conn)
        client = conn.handle

        def fn():
            return client.delete_dataset(
                dataset, delete_contents=True, not_found_ok=True)

        self._retry_and_handle(
            msg='drop dataset', conn=conn, fn=fn)

    def create_dataset(self, database, schema):
        conn = self.get_thread_connection()
        client = conn.handle
        dataset = self.dataset(database, schema, conn)

        def fn():
            return client.create_dataset(dataset, exists_ok=True)
        self._retry_and_handle(msg='create dataset', conn=conn, fn=fn)

    def _query_and_results(self, client, sql, conn, job_params, timeout=None):
        """Query the client and wait for results."""
        # Cannot reuse job_config if destination is set and ddl is used
        job_config = google.cloud.bigquery.QueryJobConfig(**job_params)
        query_job = client.query(sql, job_config=job_config)
        iterator = query_job.result(timeout=timeout)

        return query_job, iterator

    def _retry_and_handle(self, msg, conn, fn):
        """retry a function call within the context of exception_handler."""
        def reopen_conn_on_error(error):
            if isinstance(error, REOPENABLE_ERRORS):
                logger.warning('Reopening connection after {!r}', error)
                self.close(conn)
                self.open(conn)
                return

        with self.exception_handler(msg):
            return retry.retry_target(
                target=fn,
                predicate=_ErrorCounter(self.get_retries(conn)).count_error,
                sleep_generator=self._retry_generator(),
                deadline=None,
                on_error=reopen_conn_on_error)

    def _retry_generator(self):
        """Generates retry intervals that exponentially back off."""
        return retry.exponential_sleep_generator(
            initial=self.DEFAULT_INITIAL_DELAY,
            maximum=self.DEFAULT_MAXIMUM_DELAY)


class _ErrorCounter(object):
    """Counts errors seen up to a threshold then raises the next error."""

    def __init__(self, retries):
        self.retries = retries
        self.error_count = 0

    def count_error(self, error):
        if self.retries == 0:
            return False  # Don't log
        self.error_count += 1
        if _is_retryable(error) and self.error_count <= self.retries:
            logger.debug(
                'Retry attempt {} of {} after error: {}',
                self.error_count, self.retries, repr(error))
            return True
        else:
            return False


def _is_retryable(error):
    """Return true for errors that are unlikely to occur again if retried."""
    if isinstance(error, RETRYABLE_ERRORS):
        return True
    return False
