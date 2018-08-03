from __future__ import absolute_import

from contextlib import contextmanager

import dbt.compat
import dbt.deprecations
import dbt.exceptions
import dbt.schema
import dbt.flags as flags
import dbt.clients.gcloud
import dbt.clients.agate_helper

from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger

import google.auth
import google.api_core
import google.oauth2
import google.cloud.exceptions
import google.cloud.bigquery

import time
import agate


class BigQueryAdapter(PostgresAdapter):

    context_functions = [
        # deprecated -- use versions that take relations instead
        "query_for_existing",
        "execute_model",
        "create_temporary_table",
        "drop",
        "execute",
        "quote_schema_and_table",
        "make_date_partitioned_table",
        "already_exists",
        "expand_target_column_types",
        "load_dataframe",
        "get_missing_columns",

        "create_schema",
        "alter_table_add_columns",

        # versions of adapter functions that take / return Relations
        "list_relations",
        "get_relation",
        "drop_relation",
        "rename_relation",

        "get_columns_in_table"
    ]

    Relation = BigQueryRelation
    Column = dbt.schema.BigQueryColumn

    SCOPE = ('https://www.googleapis.com/auth/bigquery',
             'https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/drive')

    RELATION_TYPES = {
        'TABLE': BigQueryRelation.Table,
        'VIEW': BigQueryRelation.View,
        'EXTERNAL': BigQueryRelation.External
    }

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
    def close(cls, connection):
        if dbt.flags.STRICT_MODE:
            Connection(**connection)

        connection['state'] = 'closed'

        return connection

    @classmethod
    def list_relations(cls, profile, project_cfg, schema, model_name=None):
        connection = cls.get_connection(profile, model_name)
        client = connection.get('handle')

        bigquery_dataset = cls.get_dataset(
            profile, project_cfg, schema, model_name)

        all_tables = client.list_tables(
            bigquery_dataset,
            # BigQuery paginates tables by alphabetizing them, and using
            # the name of the last table on a page as the key for the
            # next page. If that key table gets dropped before we run
            # list_relations, then this will 404. So, we avoid this
            # situation by making the page size sufficiently large.
            # see: https://github.com/fishtown-analytics/dbt/issues/726
            # TODO: cache the list of relations up front, and then we
            #       won't need to do this
            max_results=100000)

        # This will 404 if the dataset does not exist. This behavior mirrors
        # the implementation of list_relations for other adapters
        try:
            return [cls.bq_table_to_relation(table) for table in all_tables]
        except google.api_core.exceptions.NotFound as e:
            return []

    @classmethod
    def get_relation(cls, profile, project_cfg, schema=None, identifier=None,
                     relations_list=None, model_name=None):
        if schema is None and relations_list is None:
            raise dbt.exceptions.RuntimeException(
                'get_relation needs either a schema to query, or a list '
                'of relations to use')

        if relations_list is None and identifier is not None:
            table = cls.get_bq_table(profile, project_cfg, schema, identifier)

            return cls.bq_table_to_relation(table)

        return super(BigQueryAdapter, cls).get_relation(
            profile, project_cfg, schema, identifier, relations_list,
            model_name)

    @classmethod
    def drop_relation(cls, profile, project_cfg, relation, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset = cls.get_dataset(
            profile, project_cfg, relation.schema, model_name)
        relation_object = dataset.table(relation.identifier)
        client.delete_table(relation_object)

    @classmethod
    def rename(cls, profile, project_cfg, schema,
               from_name, to_name, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`rename` is not implemented for this adapter!')

    @classmethod
    def rename_relation(cls, profile, project_cfg, from_relation, to_relation,
                        model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`rename_relation` is not implemented for this adapter!')

    @classmethod
    def get_timeout(cls, conn):
        credentials = conn['credentials']
        return credentials.get('timeout_seconds', cls.QUERY_TIMEOUT)

    @classmethod
    def materialize_as_view(cls, profile, project_cfg, dataset, model):
        model_name = model.get('name')
        model_alias = model.get('alias')
        model_sql = model.get('injected_sql')

        conn = cls.get_connection(profile, project_cfg, model_name)
        client = conn.get('handle')

        view_ref = dataset.table(model_alias)
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
    def make_date_partitioned_table(cls, profile, project_cfg, dataset_name,
                                    identifier, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset = cls.get_dataset(profile, project_cfg,
                                  dataset_name, identifier)
        table_ref = dataset.table(identifier)
        table = google.cloud.bigquery.Table(table_ref)
        table.partitioning_type = 'DAY'

        return client.create_table(table)

    @classmethod
    def materialize_as_table(cls, profile, project_cfg, dataset,
                             model, model_sql, decorator=None):
        model_name = model.get('name')
        model_alias = model.get('alias')

        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        if decorator is None:
            table_name = model_alias
        else:
            table_name = "{}${}".format(model_alias, decorator)

        table_ref = dataset.table(table_name)
        job_config = google.cloud.bigquery.QueryJobConfig()
        job_config.destination = table_ref
        job_config.write_disposition = 'WRITE_TRUNCATE'

        logger.debug("Model SQL ({}):\n{}".format(table_name, model_sql))
        query_job = client.query(model_sql, job_config=job_config)

        # this waits for the job to complete
        with cls.exception_handler(profile, model_sql, model_alias,
                                   model_name):
            query_job.result(timeout=cls.get_timeout(conn))

        return "CREATE TABLE"

    @classmethod
    def execute_model(cls, profile, project_cfg, model,
                      materialization, sql_override=None,
                      decorator=None, model_name=None):

        if sql_override is None:
            sql_override = model.get('injected_sql')

        if flags.STRICT_MODE:
            connection = cls.get_connection(profile, model.get('name'))
            Connection(**connection)

        model_name = model.get('name')
        model_schema = model.get('schema')

        dataset = cls.get_dataset(profile, project_cfg,
                                  model_schema, model_name)

        if materialization == 'view':
            res = cls.materialize_as_view(profile, project_cfg, dataset, model)
        elif materialization == 'table':
            res = cls.materialize_as_table(
                profile, project_cfg, dataset, model,
                sql_override, decorator)
        else:
            msg = "Invalid relation type: '{}'".format(materialization)
            raise dbt.exceptions.RuntimeException(msg, model)

        return res

    @classmethod
    def raw_execute(cls, profile, sql, model_name=None, fetch=False, **kwargs):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        logger.debug('On %s: %s', model_name, sql)

        job_config = google.cloud.bigquery.QueryJobConfig()
        job_config.use_legacy_sql = False
        query_job = client.query(sql, job_config)

        # this blocks until the query has completed
        with cls.exception_handler(profile, sql, model_name):
            iterator = query_job.result()

        return query_job, iterator

    @classmethod
    def create_temporary_table(cls, profile, project, sql, model_name=None,
                               **kwargs):

        # BQ queries always return a temp table with their results
        query_job, _ = cls.raw_execute(profile, sql, model_name)
        bq_table = query_job.destination

        return cls.Relation.create(
            project=bq_table.project,
            schema=bq_table.dataset_id,
            identifier=bq_table.table_id,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=BigQueryRelation.Table)

    @classmethod
    def alter_table_add_columns(cls, profile, project, relation, columns,
                                model_name=None):

        logger.debug('Adding columns ({}) to table {}".'.format(
                     columns, relation))

        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset = cls.get_dataset(profile, project, relation.schema,
                                  model_name)

        table_ref = dataset.table(relation.name)
        table = client.get_table(table_ref)

        new_columns = [col.to_bq_schema_object() for col in columns]
        new_schema = table.schema + new_columns

        new_table = google.cloud.bigquery.Table(table_ref, schema=new_schema)
        client.update_table(new_table, ['schema'])

    @classmethod
    def execute(cls, profile, sql, model_name=None, fetch=None, **kwargs):
        _, iterator = cls.raw_execute(profile, sql, model_name, fetch,
                                      **kwargs)

        if fetch:
            res = cls.get_table_from_response(iterator)
        else:
            res = dbt.clients.agate_helper.empty_table()

        # If we get here, the query succeeded
        status = 'OK'
        return status, res

    @classmethod
    def execute_and_fetch(cls, profile, sql, model_name, auto_begin=None):
        status, table = cls.execute(profile, sql, model_name, fetch=True)
        return status, table

    @classmethod
    def get_table_from_response(cls, resp):
        column_names = [field.name for field in resp.schema]
        rows = [dict(row.items()) for row in resp]
        return dbt.clients.agate_helper.table_from_data(rows, column_names)

    # BigQuery doesn't support BEGIN/COMMIT, so stub these out.

    @classmethod
    def add_begin_query(cls, profile, name):
        pass

    @classmethod
    def add_commit_query(cls, profile, name):
        pass

    @classmethod
    def create_schema(cls, profile, project_cfg, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)

        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset = cls.get_dataset(profile, project_cfg, schema, model_name)

        # Emulate 'create schema if not exists ...'
        try:
            client.get_dataset(dataset)
        except google.api_core.exceptions.NotFound:
            with cls.exception_handler(profile, 'create dataset', model_name):
                client.create_dataset(dataset)

    @classmethod
    def drop_tables_in_schema(cls, profile, project_cfg, dataset):
        conn = cls.get_connection(profile)
        client = conn.get('handle')

        for table in client.list_tables(dataset):
            client.delete_table(table.reference)

    @classmethod
    def drop_schema(cls, profile, project_cfg, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)

        if not cls.check_schema_exists(profile, project_cfg,
                                       schema, model_name):
            return

        conn = cls.get_connection(profile)
        client = conn.get('handle')

        dataset = cls.get_dataset(profile, project_cfg, schema, model_name)
        with cls.exception_handler(profile, 'drop dataset', model_name):
            cls.drop_tables_in_schema(profile, project_cfg, dataset)
            client.delete_dataset(dataset)

    @classmethod
    def get_existing_schemas(cls, profile, project_cfg, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        with cls.exception_handler(profile, 'list dataset', model_name):
            all_datasets = client.list_datasets()
            return [ds.dataset_id for ds in all_datasets]

    @classmethod
    def get_columns_in_table(cls, profile, project_cfg,
                             schema_name, table_name,
                             database=None, model_name=None):

        # BigQuery does not have databases -- the database parameter is here
        # for consistency with the base implementation

        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        try:
            dataset_ref = client.dataset(schema_name)
            table_ref = dataset_ref.table(table_name)
            table = client.get_table(table_ref)
            table_schema = table.schema
        except (ValueError, google.cloud.exceptions.NotFound) as e:
            logger.debug("get_columns_in_table error: {}".format(e))
            table_schema = []

        columns = []
        for col in table_schema:
            # BigQuery returns type labels that are not valid type specifiers
            dtype = cls.Column.translate_type(col.field_type)
            column = cls.Column(
                col.name, dtype, col.fields, col.mode)
            columns.append(column)

        return columns

    @classmethod
    def check_schema_exists(cls, profile, project_cfg,
                            schema, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        with cls.exception_handler(profile, 'get dataset', model_name):
            all_datasets = client.list_datasets()
            return any([ds.dataset_id == schema for ds in all_datasets])

    @classmethod
    def get_dataset(cls, profile, project_cfg, dataset_name, model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset_ref = client.dataset(dataset_name)
        return google.cloud.bigquery.Dataset(dataset_ref)

    @classmethod
    def bq_table_to_relation(cls, bq_table):
        if bq_table is None:
            return None

        return cls.Relation.create(
            project=bq_table.project,
            schema=bq_table.dataset_id,
            identifier=bq_table.table_id,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=cls.RELATION_TYPES.get(bq_table.table_type))

    @classmethod
    def get_bq_table(cls, profile, project_cfg, dataset_name, identifier,
                     model_name=None):
        conn = cls.get_connection(profile, model_name)
        client = conn.get('handle')

        dataset = cls.get_dataset(
            profile, project_cfg, dataset_name, model_name)

        table_ref = dataset.table(identifier)

        try:
            return client.get_table(table_ref)
        except google.cloud.exceptions.NotFound:
            return None

    @classmethod
    def warning_on_hooks(cls, hook_type):
        msg = "{} is not supported in bigquery and will be ignored"
        dbt.ui.printer.print_timestamped_line(msg.format(hook_type),
                                              dbt.ui.printer.COLOR_FG_YELLOW)

    @classmethod
    def add_query(cls, profile, sql, model_name=None, auto_begin=True,
                  bindings=None, abridge_sql_log=False):
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
    def quote_schema_and_table(cls, profile, project_cfg, schema,
                               table, model_name=None):
        return cls.render_relation(profile, project_cfg,
                                   cls.quote(schema),
                                   cls.quote(table))

    @classmethod
    def render_relation(cls, profile, project_cfg, schema, table):
        connection = cls.get_connection(profile)
        credentials = connection.get('credentials', {})
        project = credentials.get('project')
        return '{}.{}.{}'.format(cls.quote(project), schema, table)

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float64" if decimals else "int64"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "bool"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "datetime"

    @classmethod
    def _agate_to_schema(cls, agate_table, column_override):
        bq_schema = []
        for idx, col_name in enumerate(agate_table.column_names):
            inferred_type = cls.convert_agate_type(agate_table, idx)
            type_ = column_override.get(col_name, inferred_type)
            bq_schema.append(
                google.cloud.bigquery.SchemaField(col_name, type_))
        return bq_schema

    @classmethod
    def load_dataframe(cls, profile, project_cfg, schema,
                       table_name, agate_table,
                       column_override, model_name=None):
        bq_schema = cls._agate_to_schema(agate_table, column_override)
        dataset = cls.get_dataset(profile, project_cfg, schema, None)
        table = dataset.table(table_name)
        conn = cls.get_connection(profile, None)
        client = conn.get('handle')

        load_config = google.cloud.bigquery.LoadJobConfig()
        load_config.skip_leading_rows = 1
        load_config.schema = bq_schema

        with open(agate_table.original_abspath, "rb") as f:
            job = client.load_table_from_file(f, table, rewind=True,
                                              job_config=load_config)

        with cls.exception_handler(profile, "LOAD TABLE"):
            cls.poll_until_job_completes(job, cls.get_timeout(conn))

    @classmethod
    def expand_target_column_types(cls, profile, project_cfg, temp_table,
                                   to_schema, to_table, model_name=None):
        # This is a no-op on BigQuery
        pass
