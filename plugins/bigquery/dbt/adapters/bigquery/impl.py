from __future__ import absolute_import

import copy

import dbt.compat
import dbt.deprecations
import dbt.exceptions
import dbt.schema
import dbt.flags as flags
import dbt.clients.gcloud
import dbt.clients.agate_helper

from dbt.adapters.base import BaseAdapter, available
from dbt.adapters.bigquery import BigQueryRelation
from dbt.adapters.bigquery import BigQueryConnectionManager
from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger

import google.auth
import google.api_core
import google.oauth2
import google.cloud.exceptions
import google.cloud.bigquery

import time
import agate


def column_to_bq_schema(col):
    """Convert a column to a bigquery schema object. This is here instead of
    in dbt.schema to avoid importing google libraries there.
    """
    kwargs = {}
    if len(col.fields) > 0:
        fields = [column_to_bq_schema(field) for field in col.fields]
        kwargs = {"fields": fields}

    return google.cloud.bigquery.SchemaField(col.name, col.dtype, col.mode,
                                             **kwargs)


class BigQueryAdapter(BaseAdapter):

    RELATION_TYPES = {
        'TABLE': BigQueryRelation.Table,
        'VIEW': BigQueryRelation.View,
        'EXTERNAL': BigQueryRelation.External
    }

    Relation = BigQueryRelation
    Column = dbt.schema.BigQueryColumn
    ConnectionManager = BigQueryConnectionManager

    AdapterSpecificConfigs = frozenset({"cluster_by", "partition_by"})

    ###
    # Implementations of abstract methods
    ###

    @classmethod
    def date_function(cls):
        return 'CURRENT_TIMESTAMP()'

    @classmethod
    def is_cancelable(cls):
        return False

    def drop_relation(self, relation, model_name=None):
        is_cached = self._schema_is_cached(relation.database, relation.schema,
                                           model_name)
        if is_cached:
            self.cache.drop(relation)

        conn = self.connections.get(model_name)
        client = conn.handle

        dataset = self.connections.dataset(relation.database, relation.schema,
                                           conn)
        relation_object = dataset.table(relation.identifier)
        client.delete_table(relation_object)

    def truncate_relation(self, relation, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`truncate` is not implemented for this adapter!'
        )

    def rename_relation(self, from_relation, to_relation, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            '`rename_relation` is not implemented for this adapter!'
        )

    def list_schemas(self, database, model_name=None):
        conn = self.connections.get(model_name)
        client = conn.handle

        with self.connections.exception_handler('list dataset', conn.name):
            all_datasets = client.list_datasets(project=database,
                                                include_all=True)
            return [ds.dataset_id for ds in all_datasets]

    def get_columns_in_relation(self, relation, model_name=None):
        try:
            table = self.connections.get_bq_table(
                database=relation.database,
                schema=relation.schema,
                identifier=relation.table_name,
                conn_name=model_name
            )
            return self._get_dbt_columns_from_bq_table(table)

        except (ValueError, google.cloud.exceptions.NotFound) as e:
            logger.debug("get_columns_in_relation error: {}".format(e))
            return []

    def expand_column_types(self, goal, current, model_name=None):
        # This is a no-op on BigQuery
        pass

    def list_relations_without_caching(self, information_schema, schema,
                                       model_name=None):
        connection = self.connections.get(model_name)
        client = connection.handle

        bigquery_dataset = self.connections.dataset(
            information_schema.database, schema, connection
        )

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

    def get_relation(self, database, schema, identifier, model_name=None):
        if self._schema_is_cached(database, schema, model_name):
            # if it's in the cache, use the parent's model of going through
            # the relations cache and picking out the relation
            return super(BigQueryAdapter, self).get_relation(
                database=database,
                schema=schema,
                identifier=identifier,
                model_name=model_name
            )

        try:
            table = self.connections.get_bq_table(database, schema, identifier,
                                                  conn_name=model_name)
        except google.api_core.exceptions.NotFound:
            table = None
        return self._bq_table_to_relation(table)

    def create_schema(self, database, schema, model_name=None):
        logger.debug('Creating schema "%s.%s".', database, schema)
        self.connections.create_dataset(database, schema, model_name)

    def drop_schema(self, database, schema, model_name=None):
        logger.debug('Dropping schema "%s.%s".', database, schema)

        if not self.check_schema_exists(database, schema, model_name):
            return
        self.connections.drop_dataset(database, schema, model_name)

    @classmethod
    def quote(cls, identifier):
        return '`{}`'.format(identifier)

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
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"

    ###
    # Implementation details
    ###
    def _get_dbt_columns_from_bq_table(self, table):
        "Translates BQ SchemaField dicts into dbt BigQueryColumn objects"

        columns = []
        for col in table.schema:
            # BigQuery returns type labels that are not valid type specifiers
            dtype = self.Column.translate_type(col.field_type)
            column = self.Column(
                col.name, dtype, col.fields, col.mode)
            columns.append(column)

        return columns

    def _agate_to_schema(self, agate_table, column_override):
        """Convert agate.Table with column names to a list of bigquery schemas.
        """
        bq_schema = []
        for idx, col_name in enumerate(agate_table.column_names):
            inferred_type = self.convert_agate_type(agate_table, idx)
            type_ = column_override.get(col_name, inferred_type)
            bq_schema.append(
                google.cloud.bigquery.SchemaField(col_name, type_)
            )
        return bq_schema

    def _materialize_as_view(self, model):
        model_database = model.get('database')
        model_schema = model.get('schema')
        model_name = model.get('name')
        model_alias = model.get('alias')
        model_sql = model.get('injected_sql')

        logger.debug("Model SQL ({}):\n{}".format(model_name, model_sql))
        self.connections.create_view(
            database=model_database,
            schema=model_schema,
            table_name=model_alias,
            conn_name=model_name,
            sql=model_sql
        )
        return "CREATE VIEW"

    def _materialize_as_table(self, model, model_sql, decorator=None):
        model_database = model.get('database')
        model_schema = model.get('schema')
        model_name = model.get('name')
        model_alias = model.get('alias')

        if decorator is None:
            table_name = model_alias
        else:
            table_name = "{}${}".format(model_alias, decorator)

        logger.debug("Model SQL ({}):\n{}".format(table_name, model_sql))
        self.connections.create_table(
            database=model_database,
            schema=model_schema,
            conn_name=model_name,
            table_name=table_name,
            sql=model_sql
        )

        return "CREATE TABLE"

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

    def _bq_table_to_relation(self, bq_table):
        if bq_table is None:
            return None

        return self.Relation.create(
            database=bq_table.project,
            schema=bq_table.dataset_id,
            identifier=bq_table.table_id,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=self.RELATION_TYPES.get(bq_table.table_type))

    @classmethod
    def warning_on_hooks(hook_type):
        msg = "{} is not supported in bigquery and will be ignored"
        dbt.ui.printer.print_timestamped_line(msg.format(hook_type),
                                              dbt.ui.printer.COLOR_FG_YELLOW)

    @available
    def add_query(self, sql, model_name=None, auto_begin=True,
                  bindings=None, abridge_sql_log=False):
        if model_name in ['on-run-start', 'on-run-end']:
            self.warning_on_hooks(model_name)
        else:
            raise dbt.exceptions.NotImplementedException(
                '`add_query` is not implemented for this adapter!')

    ###
    # Special bigquery adapter methods
    ###
    @available
    def make_date_partitioned_table(self, relation, model_name=None):
        return self.connections.create_date_partitioned_table(
            database=relation.database,
            schema=relation.schema,
            table_name=relation.identifier,
            conn_name=model_name
        )

    @available
    def execute_model(self, model, materialization, sql_override=None,
                      decorator=None, model_name=None):

        if sql_override is None:
            sql_override = model.get('injected_sql')

        if flags.STRICT_MODE:
            connection = self.connections.get(model.get('name'))
            assert isinstance(connection, Connection)

        if materialization == 'view':
            res = self._materialize_as_view(model)
        elif materialization == 'table':
            res = self._materialize_as_table(model, sql_override, decorator)
        else:
            msg = "Invalid relation type: '{}'".format(materialization)
            raise dbt.exceptions.RuntimeException(msg, model)

        return res

    @available
    def create_temporary_table(self, sql, model_name=None, **kwargs):

        # BQ queries always return a temp table with their results
        query_job, _ = self.connections.raw_execute(sql, model_name)
        bq_table = query_job.destination

        return self.Relation.create(
            database=bq_table.project,
            schema=bq_table.dataset_id,
            identifier=bq_table.table_id,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=BigQueryRelation.Table)

    @available
    def alter_table_add_columns(self, relation, columns, model_name=None):

        logger.debug('Adding columns ({}) to table {}".'.format(
                     columns, relation))

        conn = self.connections.get(model_name)
        client = conn.handle

        table_ref = self.connections.table_ref(relation.database,
                                               relation.schema,
                                               relation.identifier, conn)
        table = client.get_table(table_ref)

        new_columns = [column_to_bq_schema(col) for col in columns]
        new_schema = table.schema + new_columns

        new_table = google.cloud.bigquery.Table(table_ref, schema=new_schema)
        client.update_table(new_table, ['schema'])

    @available
    def load_dataframe(self, database, schema, table_name, agate_table,
                       column_override, model_name=None):
        bq_schema = self._agate_to_schema(agate_table, column_override)
        conn = self.connections.get(model_name)
        client = conn.handle

        table = self.connections.table_ref(database, schema, table_name, conn)

        load_config = google.cloud.bigquery.LoadJobConfig()
        load_config.skip_leading_rows = 1
        load_config.schema = bq_schema

        with open(agate_table.original_abspath, "rb") as f:
            job = client.load_table_from_file(f, table, rewind=True,
                                              job_config=load_config)

        timeout = self.connections.get_timeout(conn)
        with self.connections.exception_handler("LOAD TABLE", conn.name):
            self.poll_until_job_completes(job, timeout)

    ###
    # The get_catalog implementation for bigquery
    ###
    def _flat_columns_in_table(self, table):
        """An iterator over the flattened columns for a given schema and table.
        Resolves child columns as having the name "parent.child".
        """
        for col in self._get_dbt_columns_from_bq_table(table):
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
        connection = self.connections.get('catalog')
        client = connection.handle

        schemas = manifest.get_used_schemas()

        column_names = (
            'table_database',
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

        for database_name, schema_name in schemas:
            relations = self.list_relations(database_name, schema_name)
            for relation in relations:

                # This relation contains a subset of the info we care about.
                # Fetch the full table object here
                table_ref = self.connections.table_ref(
                    database_name,
                    relation.schema,
                    relation.identifier,
                    connection
                )
                table = client.get_table(table_ref)

                flattened = self._flat_columns_in_table(table)
                relation_stats = dict(self._get_stats_columns(table,
                                                              relation.type))

                for index, column in enumerate(flattened, start=1):
                    column_data = (
                        relation.database,
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
