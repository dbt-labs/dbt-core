from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Any, Dict

import google.auth
import google.api_core
import google.oauth2
import google.cloud.exceptions
import google.cloud.bigquery
from google.api_core import retry

import dbt.clients.agate_helper
import dbt.exceptions
from dbt.adapters.base import BaseConnectionManager, Credentials
from dbt.logger import GLOBAL_LOGGER as logger

from hologram.helpers import StrEnum


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
    _ALIASES = {
        'project': 'database',
        'dataset': 'schema',
    }

    @property
    def type(self):
        return 'bigquery'

    def _connection_keys(self):
        return ('method', 'database', 'schema', 'location', 'priority',
                'timeout_seconds')


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
    def handle_error(cls, error, message, sql):
        logger.debug(message.format(sql=sql))
        logger.debug(str(error))
        error_msg = "\n".join(
            [item['message'] for item in error.errors])

        raise dbt.exceptions.DatabaseException(error_msg) from error

    def clear_transaction(self):
        pass

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except google.cloud.exceptions.BadRequest as e:
            message = "Bad request while running:\n{sql}"
            self.handle_error(e, message, sql)

        except google.cloud.exceptions.Forbidden as e:
            message = "Access denied while running:\n{sql}"
            self.handle_error(e, message, sql)

        except Exception as e:
            logger.debug("Unhandled error while running:\n{}".format(sql))
            logger.debug(e)
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise dbt.exceptions.RuntimeException(str(e)) from e

    def cancel_open(self) -> None:
        pass

    @classmethod
    def close(cls, connection):
        connection.state = 'closed'

        return connection

    def begin(self):
        pass

    def commit(self):
        pass

    @classmethod
    def get_bigquery_credentials(cls, profile_credentials):
        method = profile_credentials.method
        creds = google.oauth2.service_account.Credentials

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
        raise dbt.exceptions.FailedToConnectException(error)

    @classmethod
    def get_bigquery_client(cls, profile_credentials):
        database = profile_credentials.database
        creds = cls.get_bigquery_credentials(profile_credentials)
        location = getattr(profile_credentials, 'location', None)
        return google.cloud.bigquery.Client(database, creds,
                                            location=location)

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        try:
            handle = cls.get_bigquery_client(connection.credentials)

        except google.auth.exceptions.DefaultCredentialsError:
            logger.info("Please log into GCP to continue")
            dbt.clients.gcloud.setup_default_credentials()

            handle = cls.get_bigquery_client(connection.credentials)

        except Exception as e:
            raise
            logger.debug("Got an error when attempting to create a bigquery "
                         "client: '{}'".format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

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
        return dbt.clients.agate_helper.table_from_data_flat(resp,
                                                             column_names)

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
            res = dbt.clients.agate_helper.empty_table()

        if query_job.statement_type == 'CREATE_VIEW':
            status = 'CREATE VIEW'

        elif query_job.statement_type == 'CREATE_TABLE_AS_SELECT':
            conn = self.get_thread_connection()
            client = conn.handle
            table = client.get_table(query_job.destination)
            status = 'CREATE TABLE ({})'.format(table.num_rows)

        elif query_job.statement_type in ['INSERT', 'DELETE', 'MERGE']:
            status = '{} ({})'.format(
                query_job.statement_type,
                query_job.num_dml_affected_rows
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
                      'write_disposition': 'WRITE_TRUNCATE'}

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

    @staticmethod
    def dataset(database, schema, conn):
        dataset_ref = conn.handle.dataset(schema, database)
        return google.cloud.bigquery.Dataset(dataset_ref)

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
        with self.exception_handler(msg):
            return retry.retry_target(
                target=fn,
                predicate=_ErrorCounter(self.get_retries(conn)).count_error,
                sleep_generator=self._retry_generator(),
                deadline=None)

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
            logger.debug(
                'Not Retrying after {} previous attempts. Error: {}',
                self.error_count - 1, repr(error))
            return False


def _is_retryable(error):
    """Return true for 500 level (retryable) errors."""
    return isinstance(error, google.cloud.exceptions.ServerError)
