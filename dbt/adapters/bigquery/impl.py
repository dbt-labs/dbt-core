from __future__ import absolute_import

from contextlib import contextmanager
import copy

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

    config_functions = [
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
        "cache_new_relation",

        "create_schema",
        "alter_table_add_columns",

        # versions of adapter functions that take / return Relations
        "get_relation",
        "drop_relation",
        "rename_relation",

        "get_columns_in_table",

        # formerly profile functions
        "add_query",
    ]

    SCOPE = ('https://www.googleapis.com/auth/bigquery',
             'https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/drive')

    RELATION_TYPES = {
        'TABLE': BigQueryRelation.Table,
        'VIEW': BigQueryRelation.View,
        'EXTERNAL': BigQueryRelation.External
    }

    QUERY_TIMEOUT = 300
    Relation = BigQueryRelation
    Column = dbt.schema.BigQueryColumn

    @classmethod
    def handle_error(cls, error, message, sql):
        logger.debug(message.format(sql=sql))
        logger.debug(error)
        error_msg = "\n".join(
            [item['message'] for item in error.errors])

        raise dbt.exceptions.DatabaseException(error_msg)

    @contextmanager
    def exception_handler(self, sql, model_name=None,
                          connection_name='master'):
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
            raise dbt.exceptions.RuntimeException(dbt.compat.to_string(e))

    @classmethod
    def type(cls):
        return 'bigquery'

    @classmethod
    def date_function(cls):
        return 'CURRENT_TIMESTAMP()'

    def begin(self, name):
        pass

    def commit(self, connection):
        pass

    @classmethod
    def get_status(cls, cursor):
        raise dbt.exceptions.NotImplementedException(
            '`get_status` is not implemented for this adapter!')

    @classmethod
    def get_bigquery_credentials(cls, profile_credentials):
        method = profile_credentials.method
        creds = google.oauth2.service_account.Credentials

        if method == 'oauth':
            credentials, project_id = google.auth.default(scopes=cls.SCOPE)
            return credentials

        elif method == 'service-account':
            keyfile = profile_credentials.keyfile
            return creds.from_service_account_file(keyfile, scopes=cls.SCOPE)

        elif method == 'service-account-json':
            details = profile_credentials.keyfile_json
            return creds.from_service_account_info(details, scopes=cls.SCOPE)

        error = ('Invalid `method` in profile: "{}"'.format(method))
        raise dbt.exceptions.FailedToConnectException(error)

    @classmethod
    def get_bigquery_client(cls, profile_credentials):
        project_name = profile_credentials.project
        creds = cls.get_bigquery_credentials(profile_credentials)
        location = getattr(profile_credentials, 'location', None)
        return google.cloud.bigquery.Client(project_name, creds,
                                            location=location)

    @classmethod
    def open_connection(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        try:
            handle = cls.get_bigquery_client(connection.credentials)

        except google.auth.exceptions.DefaultCredentialsError as e:
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
    def close(cls, connection):
        connection.state = 'closed'

        return connection

    def _link_cached_relations(self, manifest, schemas):
        pass

    def _list_relations(self, schema, model_name=None):
        connection = self.get_connection(model_name)
        client = connection.handle

        bigquery_dataset = self.get_dataset(schema, model_name)

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
            return [self._bq_table_to_relation(table) for table in all_tables]
        except google.api_core.exceptions.NotFound as e:
            return []

    def get_relation(self, schema, identifier, model_name=None):
        if self._schema_is_cached(schema, model_name):
            # if it's in the cache, use the parent's model of going through
            # the relations cache and picking out the relation
            return super(BigQueryAdapter, self).get_relation(
                schema=schema,
                identifier=identifier,
                model_name=model_name
            )

        table = self._get_bq_table(schema, identifier)
        return self._bq_table_to_relation(table)

    def drop_relation(self, relation, model_name=None):
        if self._schema_is_cached(relation.schema, model_name):
            self.cache.drop(relation)

        conn = self.get_connection(model_name)
        client = conn.handle

        dataset = self.get_dataset(relation.schema, model_name)
        relation_object = dataset.table(relation.identifier)
        client.delete_table(relation_object)

    def rename(self, schema, from_name, to_name, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`rename` is not implemented for this adapter!')

    def rename_relation(self, from_relation, to_relation, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`rename_relation` is not implemented for this adapter!')

    @classmethod
    def get_timeout(cls, conn):
        credentials = conn['credentials']
        return credentials.get('timeout_seconds', cls.QUERY_TIMEOUT)

    def materialize_as_view(self, dataset, model):
        model_name = model.get('name')
        model_alias = model.get('alias')
        model_sql = model.get('injected_sql')

        conn = self.get_connection(model_name)
        client = conn.handle

        view_ref = dataset.table(model_alias)
        view = google.cloud.bigquery.Table(view_ref)
        view.view_query = model_sql
        view.view_use_legacy_sql = False

        logger.debug("Model SQL ({}):\n{}".format(model_name, model_sql))

        with self.exception_handler(model_sql, model_name, model_name):
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
            message = '\n'.join(
                error['message'].strip() for error in job.errors
            )
            raise dbt.exceptions.RuntimeException(message)

    def make_date_partitioned_table(self, dataset_name, identifier,
                                    model_name=None):
        conn = self.get_connection(model_name)
        client = conn.handle

        dataset = self.get_dataset(dataset_name, identifier)
        table_ref = dataset.table(identifier)
        table = google.cloud.bigquery.Table(table_ref)
        table.partitioning_type = 'DAY'

        return client.create_table(table)

    def materialize_as_table(self, dataset, model, model_sql,
                             decorator=None):
        model_name = model.get('name')
        model_alias = model.get('alias')

        conn = self.get_connection(model_name)
        client = conn.handle

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
        with self.exception_handler(model_sql, model_alias,
                                    model_name):
            query_job.result(timeout=self.get_timeout(conn))

        return "CREATE TABLE"

    def execute_model(self, model,
                      materialization, sql_override=None,
                      decorator=None, model_name=None):

        if sql_override is None:
            sql_override = model.get('injected_sql')

        if flags.STRICT_MODE:
            connection = self.get_connection(model.get('name'))
            Connection(**connection)

        model_name = model.get('name')
        model_schema = model.get('schema')

        dataset = self.get_dataset(model_schema, model_name)

        if materialization == 'view':
            res = self.materialize_as_view(dataset, model)
        elif materialization == 'table':
            res = self.materialize_as_table(
                dataset, model,
                sql_override, decorator)
        else:
            msg = "Invalid relation type: '{}'".format(materialization)
            raise dbt.exceptions.RuntimeException(msg, model)

        return res

    def raw_execute(self, sql, model_name=None, fetch=False, **kwargs):
        conn = self.get_connection(model_name)
        client = conn.handle

        logger.debug('On %s: %s', model_name, sql)

        job_config = google.cloud.bigquery.QueryJobConfig()
        job_config.use_legacy_sql = False
        query_job = client.query(sql, job_config)

        # this blocks until the query has completed
        with self.exception_handler(sql, model_name):
            iterator = query_job.result()

        return query_job, iterator

    def create_temporary_table(self, sql, model_name=None, **kwargs):

        # BQ queries always return a temp table with their results
        query_job, _ = self.raw_execute(sql, model_name)
        bq_table = query_job.destination

        return self.Relation.create(
            project=bq_table.project,
            schema=bq_table.dataset_id,
            identifier=bq_table.table_id,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=BigQueryRelation.Table)

    def alter_table_add_columns(self, relation, columns, model_name=None):

        logger.debug('Adding columns ({}) to table {}".'.format(
                     columns, relation))

        conn = self.get_connection(model_name)
        client = conn.handle

        dataset = self.get_dataset(relation.schema, model_name)

        table_ref = dataset.table(relation.name)
        table = client.get_table(table_ref)

        new_columns = [col.to_bq_schema_object() for col in columns]
        new_schema = table.schema + new_columns

        new_table = google.cloud.bigquery.Table(table_ref, schema=new_schema)
        client.update_table(new_table, ['schema'])

    def execute(self, sql, model_name=None, fetch=None, **kwargs):
        _, iterator = self.raw_execute(sql, model_name, fetch, **kwargs)

        if fetch:
            res = self.get_table_from_response(iterator)
        else:
            res = dbt.clients.agate_helper.empty_table()

        # If we get here, the query succeeded
        status = 'OK'
        return status, res

    def execute_and_fetch(self, sql, model_name, auto_begin=None):
        status, table = self.execute(sql, model_name, fetch=True)
        return status, table

    @classmethod
    def get_table_from_response(cls, resp):
        column_names = [field.name for field in resp.schema]
        rows = [dict(row.items()) for row in resp]
        return dbt.clients.agate_helper.table_from_data(rows, column_names)

    # BigQuery doesn't support BEGIN/COMMIT, so stub these out.

    def add_begin_query(self, name):
        pass

    def add_commit_query(self, name):
        pass

    def create_schema(self, schema, model_name=None):
        logger.debug('Creating schema "%s".', schema)

        conn = self.get_connection(model_name)
        client = conn.handle

        dataset = self.get_dataset(schema, model_name)

        # Emulate 'create schema if not exists ...'
        try:
            client.get_dataset(dataset)
        except google.api_core.exceptions.NotFound:
            with self.exception_handler('create dataset', model_name):
                client.create_dataset(dataset)

    def drop_schema(self, schema, model_name=None):
        logger.debug('Dropping schema "%s".', schema)

        if not self.check_schema_exists(schema, model_name):
            return

        conn = self.get_connection(model_name)
        client = conn.handle

        dataset = self.get_dataset(schema, model_name)
        with self.exception_handler('drop dataset', model_name):
            client.delete_dataset(dataset, delete_contents=True)

    def get_existing_schemas(self, model_name=None):
        conn = self.get_connection(model_name)
        client = conn.handle

        with self.exception_handler('list dataset', model_name):
            all_datasets = client.list_datasets(include_all=True)
            return [ds.dataset_id for ds in all_datasets]

    def get_columns_in_table(self, schema_name, table_name,
                             database=None, model_name=None):

        # BigQuery does not have databases -- the database parameter is here
        # for consistency with the base implementation

        conn = self.get_connection(model_name)
        client = conn.handle

        try:
            dataset_ref = client.dataset(schema_name)
            table_ref = dataset_ref.table(table_name)
            table = client.get_table(table_ref)
            return self.get_dbt_columns_from_bq_table(table)

        except (ValueError, google.cloud.exceptions.NotFound) as e:
            logger.debug("get_columns_in_table error: {}".format(e))
            return []

    def get_dbt_columns_from_bq_table(self, table):
        "Translates BQ SchemaField dicts into dbt BigQueryColumn objects"

        columns = []
        for col in table.schema:
            # BigQuery returns type labels that are not valid type specifiers
            dtype = self.Column.translate_type(col.field_type)
            column = self.Column(
                col.name, dtype, col.fields, col.mode)
            columns.append(column)

        return columns

    def check_schema_exists(self, schema, model_name=None):
        conn = self.get_connection(model_name)
        client = conn.handle

        with self.exception_handler('get dataset', model_name):
            all_datasets = client.list_datasets(include_all=True)
            return any([ds.dataset_id == schema for ds in all_datasets])

    def get_dataset(self, dataset_name, model_name=None):
        conn = self.get_connection(model_name)
        dataset_ref = conn.handle.dataset(dataset_name)
        return google.cloud.bigquery.Dataset(dataset_ref)

    def _bq_table_to_relation(self, bq_table):
        if bq_table is None:
            return None

        return self.Relation.create(
            project=bq_table.project,
            schema=bq_table.dataset_id,
            identifier=bq_table.table_id,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=self.RELATION_TYPES.get(bq_table.table_type))

    def _get_bq_table(self, dataset_name, identifier, model_name=None):
        conn = self.get_connection(model_name)

        dataset = self.get_dataset(dataset_name, model_name)

        table_ref = dataset.table(identifier)

        try:
            return conn.handle.get_table(table_ref)
        except google.cloud.exceptions.NotFound:
            return None

    @classmethod
    def warning_on_hooks(hook_type):
        msg = "{} is not supported in bigquery and will be ignored"
        dbt.ui.printer.print_timestamped_line(msg.format(hook_type),
                                              dbt.ui.printer.COLOR_FG_YELLOW)

    def add_query(self, sql, model_name=None, auto_begin=True,
                  bindings=None, abridge_sql_log=False):
        if model_name in ['on-run-start', 'on-run-end']:
            self.warning_on_hooks(model_name)
        else:
            raise dbt.exceptions.NotImplementedException(
                '`add_query` is not implemented for this adapter!')

    @classmethod
    def is_cancelable(cls):
        return False

    @classmethod
    def quote(cls, identifier):
        return '`{}`'.format(identifier)

    def quote_schema_and_table(self, schema, table, model_name=None):
        return self.render_relation(self.quote(schema), self.quote(table))

    def render_relation(cls, schema, table):
        connection = self.get_connection()
        project = connection.credentials.project
        return '{}.{}.{}'.format(self.quote(project), schema, table)

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

    def load_dataframe(self, schema, table_name, agate_table,
                       column_override, model_name=None):
        bq_schema = self._agate_to_schema(agate_table, column_override)
        dataset = self.get_dataset(schema, None)
        table = dataset.table(table_name)
        conn = self.get_connection(None)
        client = conn.handle

        load_config = google.cloud.bigquery.LoadJobConfig()
        load_config.skip_leading_rows = 1
        load_config.schema = bq_schema

        with open(agate_table.original_abspath, "rb") as f:
            job = client.load_table_from_file(f, table, rewind=True,
                                              job_config=load_config)

        with self.exception_handler("LOAD TABLE"):
            self.poll_until_job_completes(job, self.get_timeout(conn))

    def expand_target_column_types(self, temp_table,
                                   to_schema, to_table, model_name=None):
        # This is a no-op on BigQuery
        pass

    def _flat_columns_in_table(self, table):
        """An iterator over the flattened columns for a given schema and table.
        Resolves child columns as having the name "parent.child".
        """
        for col in self.get_dbt_columns_from_bq_table(table):
            flattened = col.flatten()
            for subcol in flattened:
                yield subcol

    @classmethod
    def _get_stats_column_names(cls):
        """Construct a tuple of the column names for stats. Each stat has 4
        columns of data.
        """
        columns = []
        stats = ('num_bytes', 'num_rows', 'location', 'partitioning_type',
                 'clustering_fields')
        stat_components = ('label', 'value', 'description', 'include')
        for stat_id in stats:
            for stat_component in stat_components:
                columns.append('stats:{}:{}'.format(stat_id, stat_component))
        return tuple(columns)

    @classmethod
    def _get_stats_columns(cls, table, relation_type):
        """Given a table, return an iterator of key/value pairs for stats
        column names/values.
        """
        column_names = cls._get_stats_column_names()

        # agate does not handle the array of column names gracefully
        clustering_value = None
        if table.clustering_fields is not None:
            clustering_value = ','.join(table.clustering_fields)
        # cast num_bytes/num_rows to str before they get to agate, or else
        # agate will incorrectly decide they are booleans.
        column_values = (
            'Number of bytes',
            str(table.num_bytes),
            'The number of bytes this table consumes',
            relation_type == 'table',

            'Number of rows',
            str(table.num_rows),
            'The number of rows in this table',
            relation_type == 'table',

            'Location',
            table.location,
            'The geographic location of this table',
            True,

            'Partitioning Type',
            table.partitioning_type,
            'The partitioning type used for this table',
            relation_type == 'table',

            'Clustering Fields',
            clustering_value,
            'The clustering fields for this table',
            relation_type == 'table',
        )
        return zip(column_names, column_values)

    def get_catalog(self, manifest):
        connection = self.get_connection('catalog')
        client = connection.handle

        schemas = {
            node.to_dict()['schema']
            for node in manifest.nodes.values()
        }

        column_names = (
            'table_schema',
            'table_name',
            'table_type',
            'table_comment',
            # does not exist in bigquery, but included for consistency
            'table_owner',
            'column_name',
            'column_index',
            'column_type',
            'column_comment',
        )
        all_names = column_names + self._get_stats_column_names()
        columns = []

        for schema_name in schemas:
            relations = self.list_relations(schema_name)
            for relation in relations:

                # This relation contains a subset of the info we care about.
                # Fetch the full table object here
                dataset_ref = client.dataset(relation.schema)
                table_ref = dataset_ref.table(relation.identifier)
                table = client.get_table(table_ref)

                flattened = self._flat_columns_in_table(table)
                relation_stats = dict(self._get_stats_columns(table,
                                                              relation.type))

                for index, column in enumerate(flattened, start=1):
                    column_data = (
                        relation.schema,
                        relation.name,
                        relation.type,
                        None,
                        None,
                        column.name,
                        index,
                        column.data_type,
                        None,
                    )
                    column_dict = dict(zip(column_names, column_data))
                    column_dict.update(copy.deepcopy(relation_stats))

                    columns.append(column_dict)

        return dbt.clients.agate_helper.table_from_data(columns, all_names)
