from typing import Dict, List, Optional, Any

import dbt.deprecations
import dbt.exceptions
import dbt.flags as flags
import dbt.clients.gcloud
import dbt.clients.agate_helper

from dbt.adapters.base import BaseAdapter, available, RelationType
from dbt.adapters.bigquery.relation import (
    BigQueryRelation, BigQueryInformationSchema
)
from dbt.adapters.bigquery import BigQueryColumn
from dbt.adapters.bigquery import BigQueryConnectionManager
from dbt.contracts.connection import Connection
from dbt.contracts.graph.manifest import Manifest
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import filter_null_values

import google.auth
import google.api_core
import google.oauth2
import google.cloud.exceptions
import google.cloud.bigquery

import time
import agate


def _stub_relation(*args, **kwargs):
    return BigQueryRelation.create(
        database='',
        schema='',
        identifier='',
        quote_policy={},
        type=BigQueryRelation.Table
    )


class BigQueryAdapter(BaseAdapter):

    RELATION_TYPES = {
        'TABLE': RelationType.Table,
        'VIEW': RelationType.View,
        'EXTERNAL': RelationType.External
    }

    Relation = BigQueryRelation
    Column = BigQueryColumn
    ConnectionManager = BigQueryConnectionManager

    AdapterSpecificConfigs = frozenset({"cluster_by", "partition_by",
                                        "kms_key_name", "labels"})

    ###
    # Implementations of abstract methods
    ###

    @classmethod
    def date_function(cls) -> str:
        return 'CURRENT_TIMESTAMP()'

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    def drop_relation(self, relation: BigQueryRelation) -> None:
        is_cached = self._schema_is_cached(relation.database, relation.schema)
        if is_cached:
            self.cache_dropped(relation)

        conn = self.connections.get_thread_connection()
        client = conn.handle

        dataset = self.connections.dataset(relation.database, relation.schema,
                                           conn)
        relation_object = dataset.table(relation.identifier)
        client.delete_table(relation_object)

    def truncate_relation(self, relation: BigQueryRelation) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`truncate` is not implemented for this adapter!'
        )

    def rename_relation(
        self, from_relation: BigQueryRelation, to_relation: BigQueryRelation
    ) -> None:
        raise dbt.exceptions.NotImplementedException(
            '`rename_relation` is not implemented for this adapter!'
        )

    @available
    def list_schemas(self, database: str) -> List[str]:
        conn = self.connections.get_thread_connection()
        client = conn.handle

        def query_schemas():
            # this is similar to how we have to deal with listing tables
            all_datasets = client.list_datasets(project=database,
                                                max_results=10000)
            return [ds.dataset_id for ds in all_datasets]

        return self.connections._retry_and_handle(
            msg='list dataset', conn=conn, fn=query_schemas)

    @available.parse(lambda *a, **k: False)
    def check_schema_exists(self, database: str, schema: str) -> bool:
        conn = self.connections.get_thread_connection()
        client = conn.handle

        bigquery_dataset = self.connections.dataset(
            database, schema, conn
        )
        # try to do things with the dataset. If it doesn't exist it will 404.
        # we have to do it this way to handle underscore-prefixed datasets,
        # which appear in neither the information_schema.schemata view nor the
        # list_datasets method.
        try:
            next(iter(client.list_tables(bigquery_dataset, max_results=1)))
        except StopIteration:
            pass
        except google.api_core.exceptions.NotFound:
            # the schema does not exist
            return False
        return True

    def get_columns_in_relation(
        self, relation: BigQueryRelation
    ) -> List[BigQueryColumn]:
        try:
            table = self.connections.get_bq_table(
                database=relation.database,
                schema=relation.schema,
                identifier=relation.identifier
            )
            return self._get_dbt_columns_from_bq_table(table)

        except (ValueError, google.cloud.exceptions.NotFound) as e:
            logger.debug("get_columns_in_relation error: {}".format(e))
            return []

    def expand_column_types(
        self, goal: BigQueryRelation, current: BigQueryRelation
    ) -> None:
        # This is a no-op on BigQuery
        pass

    def expand_target_column_types(
        self, from_relation: BigQueryRelation, to_relation: BigQueryRelation
    ) -> None:
        # This is a no-op on BigQuery
        pass

    def list_relations_without_caching(
        self, information_schema: BigQueryInformationSchema, schema: str
    ) -> List[BigQueryRelation]:
        connection = self.connections.get_thread_connection()
        client = connection.handle

        bigquery_dataset = self.connections.dataset(
            information_schema.database, information_schema.schema, connection
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
        except google.api_core.exceptions.NotFound:
            return []

    def get_relation(
        self, database: str, schema: str, identifier: str
    ) -> BigQueryRelation:
        if self._schema_is_cached(database, schema):
            # if it's in the cache, use the parent's model of going through
            # the relations cache and picking out the relation
            return super().get_relation(
                database=database,
                schema=schema,
                identifier=identifier
            )

        try:
            table = self.connections.get_bq_table(database, schema, identifier)
        except google.api_core.exceptions.NotFound:
            table = None
        return self._bq_table_to_relation(table)

    def create_schema(self, database: str, schema: str) -> None:
        logger.debug('Creating schema "{}.{}".', database, schema)
        self.connections.create_dataset(database, schema)

    def drop_schema(self, database: str, schema: str) -> None:
        logger.debug('Dropping schema "{}.{}".', database, schema)
        self.connections.drop_dataset(database, schema)
        self.cache.drop_schema(database, schema)

    @classmethod
    def quote(cls, identifier: str) -> str:
        return '`{}`'.format(identifier)

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(
        cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float64" if decimals else "int64"

    @classmethod
    def convert_boolean_type(
        cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "bool"

    @classmethod
    def convert_datetime_type(
        cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "datetime"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "time"

    ###
    # Implementation details
    ###
    def _make_match_kwargs(
        self, database: str, schema: str, identifier: str
    ) -> Dict[str, str]:
        return filter_null_values({
            'database': database,
            'identifier': identifier,
            'schema': schema,
        })

    def _get_dbt_columns_from_bq_table(self, table) -> List[BigQueryColumn]:
        "Translates BQ SchemaField dicts into dbt BigQueryColumn objects"

        columns = []
        for col in table.schema:
            # BigQuery returns type labels that are not valid type specifiers
            dtype = self.Column.translate_type(col.field_type)
            column = self.Column(
                col.name, dtype, col.fields, col.mode)
            columns.append(column)

        return columns

    def _agate_to_schema(
        self, agate_table: agate.Table, column_override: Dict[str, str]
    ) -> List[google.cloud.bigquery.SchemaField]:
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

    def _materialize_as_view(self, model: Dict[str, Any]) -> str:
        model_database = model.get('database')
        model_schema = model.get('schema')
        model_alias = model.get('alias')
        model_sql = model.get('injected_sql')

        logger.debug("Model SQL ({}):\n{}".format(model_alias, model_sql))
        self.connections.create_view(
            database=model_database,
            schema=model_schema,
            table_name=model_alias,
            sql=model_sql
        )
        return "CREATE VIEW"

    def _materialize_as_table(
        self,
        model: Dict[str, Any],
        model_sql: str,
        decorator: Optional[str] = None,
    ) -> str:
        model_database = model.get('database')
        model_schema = model.get('schema')
        model_alias = model.get('alias')

        if decorator is None:
            table_name = model_alias
        else:
            table_name = "{}${}".format(model_alias, decorator)

        logger.debug("Model SQL ({}):\n{}".format(table_name, model_sql))
        self.connections.create_table(
            database=model_database,
            schema=model_schema,
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
            type=self.RELATION_TYPES.get(
                bq_table.table_type, RelationType.External
            ),
        )

    @classmethod
    def warning_on_hooks(hook_type):
        msg = "{} is not supported in bigquery and will be ignored"
        dbt.ui.printer.print_timestamped_line(msg.format(hook_type),
                                              dbt.ui.printer.COLOR_FG_YELLOW)

    @available
    def add_query(self, sql, auto_begin=True, bindings=None,
                  abridge_sql_log=False):
        if self.nice_connection_name() in ['on-run-start', 'on-run-end']:
            self.warning_on_hooks(self.nice_connection_name())
        else:
            raise dbt.exceptions.NotImplementedException(
                '`add_query` is not implemented for this adapter!')

    ###
    # Special bigquery adapter methods
    ###
    @available.parse_none
    def make_date_partitioned_table(self, relation):
        return self.connections.create_date_partitioned_table(
            database=relation.database,
            schema=relation.schema,
            table_name=relation.identifier
        )

    @available.parse(lambda *a, **k: '')
    def execute_model(self, model, materialization, sql_override=None,
                      decorator=None):

        if sql_override is None:
            sql_override = model.get('injected_sql')

        if flags.STRICT_MODE:
            connection = self.connections.get_thread_connection()
            if not isinstance(connection, Connection):
                raise dbt.exceptions.CompilerException(
                    f'Got {connection} - not a Connection!'
                )
            model_uid = model.get('unique_id')
            if connection.name != model_uid:
                raise dbt.exceptions.InternalException(
                    f'Connection had name "{connection.name}", expected model '
                    f'unique id of "{model_uid}"'
                )

        if materialization == 'view':
            res = self._materialize_as_view(model)
        elif materialization == 'table':
            res = self._materialize_as_table(model, sql_override, decorator)
        else:
            msg = "Invalid relation type: '{}'".format(materialization)
            raise dbt.exceptions.RuntimeException(msg, model)

        return res

    @available.parse(lambda *a, **k: True)
    def is_replaceable(self, relation, conf_partition, conf_cluster):
        """
        Check if a given partition and clustering column spec for a table
        can replace an existing relation in the database. BigQuery does not
        allow tables to be replaced with another table that has a different
        partitioning spec. This method returns True if the given config spec is
        identical to that of the existing table.
        """
        try:
            table = self.connections.get_bq_table(
                database=relation.database,
                schema=relation.schema,
                identifier=relation.identifier
            )
        except google.cloud.exceptions.NotFound:
            return True

        table_partition = table.time_partitioning
        if table_partition is not None:
            table_partition = table_partition.field

        table_cluster = table.clustering_fields

        if isinstance(conf_cluster, str):
            conf_cluster = [conf_cluster]

        return table_partition == conf_partition \
            and table_cluster == conf_cluster

    @available.parse_none
    def alter_table_add_columns(self, relation, columns):

        logger.debug('Adding columns ({}) to table {}".'.format(
                     columns, relation))

        conn = self.connections.get_thread_connection()
        client = conn.handle

        table_ref = self.connections.table_ref(relation.database,
                                               relation.schema,
                                               relation.identifier, conn)
        table = client.get_table(table_ref)

        new_columns = [col.column_to_bq_schema() for col in columns]
        new_schema = table.schema + new_columns

        new_table = google.cloud.bigquery.Table(table_ref, schema=new_schema)
        client.update_table(new_table, ['schema'])

    @available.parse_none
    def load_dataframe(self, database, schema, table_name, agate_table,
                       column_override):
        bq_schema = self._agate_to_schema(agate_table, column_override)
        conn = self.connections.get_thread_connection()
        client = conn.handle

        table = self.connections.table_ref(database, schema, table_name, conn)

        load_config = google.cloud.bigquery.LoadJobConfig()
        load_config.skip_leading_rows = 1
        load_config.schema = bq_schema

        with open(agate_table.original_abspath, "rb") as f:
            job = client.load_table_from_file(f, table, rewind=True,
                                              job_config=load_config)

        timeout = self.connections.get_timeout(conn)
        with self.connections.exception_handler("LOAD TABLE"):
            self.poll_until_job_completes(job, timeout)

    @classmethod
    def _catalog_filter_table(
        cls, table: agate.Table, manifest: Manifest
    ) -> agate.Table:
        table = table.rename(column_names={
            col.name: col.name.replace('__', ':') for col in table.columns
        })
        return super()._catalog_filter_table(table, manifest)

    def _get_catalog_information_schemas(
        self, manifest: Manifest
    ) -> List[BigQueryInformationSchema]:

        candidates = super()._get_catalog_information_schemas(manifest)
        information_schemas = []
        db_schemas = {}
        for candidate in candidates:
            database = candidate.database
            if database not in db_schemas:
                db_schemas[database] = set(self.list_schemas(database))
            if candidate.schema in db_schemas[database]:
                information_schemas.append(candidate)
            else:
                logger.debug(
                    'Skipping catalog for {}.{} - schema does not exist'
                    .format(database, candidate.schema)
                )

        return information_schemas
