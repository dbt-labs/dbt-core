from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Set, Union
from hologram import JsonSchemaMixin, ValidationError

import dbt.deprecations
import dbt.exceptions
import dbt.flags as flags
import dbt.clients.gcloud
import dbt.clients.agate_helper

from dbt import ui
from dbt.adapters.base import (
    BaseAdapter, available, RelationType, SchemaSearchMap, AdapterConfig
)
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery import BigQueryColumn
from dbt.adapters.bigquery import BigQueryConnectionManager
from dbt.contracts.connection import Connection
from dbt.contracts.graph.manifest import Manifest
from dbt.logger import GLOBAL_LOGGER as logger, print_timestamped_line
from dbt.utils import filter_null_values

import google.auth
import google.api_core
import google.oauth2
import google.cloud.exceptions
import google.cloud.bigquery

from google.cloud.bigquery import AccessEntry, SchemaField

import time
import agate
import json

# Write dispositions for bigquery.
WRITE_APPEND = google.cloud.bigquery.job.WriteDisposition.WRITE_APPEND
WRITE_TRUNCATE = google.cloud.bigquery.job.WriteDisposition.WRITE_TRUNCATE


def sql_escape(string):
    if not isinstance(string, str):
        dbt.exceptions.raise_compiler_exception(
            f'cannot escape a non-string: {string}'
        )

    return json.dumps(string)[1:-1]


@dataclass
class PartitionConfig(JsonSchemaMixin):
    field: str
    data_type: str = 'date'
    range: Optional[Dict[str, Any]] = None

    def render(self, alias: Optional[str] = None):
        column: str = self.field
        if alias:
            column = f'{alias}.{self.field}'

        if self.data_type in ('timestamp', 'datetime'):
            return f'date({column})'
        else:
            return column

    @classmethod
    def parse(cls, raw_partition_by) -> Optional['PartitionConfig']:
        if raw_partition_by is None:
            return None
        try:
            return cls.from_dict(raw_partition_by)
        except ValidationError as exc:
            msg = dbt.exceptions.validator_error_message(exc)
            dbt.exceptions.raise_compiler_error(
                f'Could not parse partition config: {msg}'
            )
        except TypeError:
            dbt.exceptions.raise_compiler_error(
                f'Invalid partition_by config:\n'
                f'  Got: {raw_partition_by}\n'
                f'  Expected a dictionary with "field" and "data_type" keys'
            )


@dataclass
class GrantTarget(JsonSchemaMixin):
    dataset: str
    project: str

    def render(self):
        return f'{self.project}.{self.dataset}'


def _stub_relation(*args, **kwargs):
    return BigQueryRelation.create(
        database='',
        schema='',
        identifier='',
        quote_policy={},
        type=BigQueryRelation.Table
    )


@dataclass
class BigqueryConfig(AdapterConfig):
    cluster_by: Optional[Union[List[str], str]] = None
    partition_by: Optional[Dict[str, Any]] = None
    kms_key_name: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    partitions: Optional[List[str]] = None
    grant_access_to: Optional[List[Dict[str, str]]] = None
    hours_to_expiration: Optional[int] = None


class BigQueryAdapter(BaseAdapter):

    RELATION_TYPES = {
        'TABLE': RelationType.Table,
        'VIEW': RelationType.View,
        'EXTERNAL': RelationType.External
    }

    Relation = BigQueryRelation
    Column = BigQueryColumn
    ConnectionManager = BigQueryConnectionManager

    AdapterSpecificConfigs = BigqueryConfig

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

        conn = self.connections.get_thread_connection()
        client = conn.handle

        from_table_ref = self.connections.table_ref(from_relation.database,
                                                    from_relation.schema,
                                                    from_relation.identifier,
                                                    conn)
        from_table = client.get_table(from_table_ref)
        if from_table.table_type == "VIEW" or \
                from_relation.type == RelationType.View or \
                to_relation.type == RelationType.View:
            raise dbt.exceptions.RuntimeException(
                'Renaming of views is not currently supported in BigQuery'
            )

        to_table_ref = self.connections.table_ref(to_relation.database,
                                                  to_relation.schema,
                                                  to_relation.identifier,
                                                  conn)

        self.cache_renamed(from_relation, to_relation)
        client.copy_table(from_table_ref, to_table_ref)
        client.delete_table(from_table_ref)

    @available
    def list_schemas(self, database: str) -> List[str]:
        # the database string we get here is potentially quoted. Strip that off
        # for the API call.
        database = database.strip('`')
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

    @available.parse_list
    def list_relations_without_caching(
        self, schema_relation: BigQueryRelation
    ) -> List[BigQueryRelation]:
        connection = self.connections.get_thread_connection()
        client = connection.handle

        bigquery_dataset = self.connections.dataset(
            schema_relation.database, schema_relation.schema, connection
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

    def create_schema(self, relation: BigQueryRelation) -> None:
        database = relation.database
        schema = relation.schema
        logger.debug('Creating schema "{}.{}".', database, schema)
        self.connections.create_dataset(database, schema)

    def drop_schema(self, relation: BigQueryRelation) -> None:
        database = relation.database
        schema = relation.schema
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
    ) -> List[SchemaField]:
        """Convert agate.Table with column names to a list of bigquery schemas.
        """
        bq_schema = []
        for idx, col_name in enumerate(agate_table.column_names):
            inferred_type = self.convert_agate_type(agate_table, idx)
            type_ = column_override.get(col_name, inferred_type)
            bq_schema.append(SchemaField(col_name, type_))
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

    @available.parse(lambda *a, **k: '')
    def copy_table(self, source, destination, materialization):
        if materialization == 'incremental':
            write_disposition = WRITE_APPEND
        elif materialization == 'table':
            write_disposition = WRITE_TRUNCATE
        else:
            dbt.exceptions.raise_compiler_error(
                'Copy table materialization must be "copy" or "table", but '
                f"config.get('copy_materialization', 'table') was "
                f'{materialization}')

        self.connections.copy_bq_table(
            source, destination, write_disposition)

        return "COPY TABLE with materialization: {}".format(materialization)

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
        print_timestamped_line(
            msg.format(hook_type), ui.COLOR_FG_YELLOW
        )

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
                dbt.exceptions.raise_compiler_error(
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

    def _partitions_match(
        self, table, conf_partition: Optional[PartitionConfig]
    ) -> bool:
        """
        Check if the actual and configured partitions for a table are a match.
        BigQuery tables can be replaced if:
        - Both tables are not partitioned, OR
        - Both tables are partitioned using the exact same configs

        If there is a mismatch, then the table cannot be replaced directly.
        """
        is_partitioned = (table.range_partitioning or table.time_partitioning)

        if not is_partitioned and not conf_partition:
            return True
        elif conf_partition and table.time_partitioning is not None:
            table_field = table.time_partitioning.field
            return table_field == conf_partition.field
        elif conf_partition and table.range_partitioning is not None:
            dest_part = table.range_partitioning
            conf_part = conf_partition.range or {}

            return dest_part.field == conf_partition.field \
                and dest_part.range_.start == conf_part.get('start') \
                and dest_part.range_.end == conf_part.get('end') \
                and dest_part.range_.interval == conf_part.get('interval')
        else:
            return False

    def _clusters_match(self, table, conf_cluster) -> bool:
        """
        Check if the actual and configured clustering columns for a table
        are a match. BigQuery tables can be replaced if clustering columns
        match exactly.
        """
        if isinstance(conf_cluster, str):
            conf_cluster = [conf_cluster]

        return table.clustering_fields == conf_cluster

    @available.parse(lambda *a, **k: True)
    def is_replaceable(
        self,
        relation,
        conf_partition: Optional[PartitionConfig],
        conf_cluster
    ) -> bool:
        """
        Check if a given partition and clustering column spec for a table
        can replace an existing relation in the database. BigQuery does not
        allow tables to be replaced with another table that has a different
        partitioning spec. This method returns True if the given config spec is
        identical to that of the existing table.
        """
        if not relation:
            return True

        try:
            table = self.connections.get_bq_table(
                database=relation.database,
                schema=relation.schema,
                identifier=relation.identifier
            )
        except google.cloud.exceptions.NotFound:
            return True

        return all((
            self._partitions_match(table, conf_partition),
            self._clusters_match(table, conf_cluster)
        ))

    @available
    def parse_partition_by(
        self, raw_partition_by: Any
    ) -> Optional[PartitionConfig]:
        """
        dbt v0.16.0 expects `partition_by` to be a dictionary where previously
        it was a string. Check the type of `partition_by`, raise error
        or warning if string, and attempt to convert to dict.
        """
        return PartitionConfig.parse(raw_partition_by)

    def get_table_ref_from_relation(self, conn, relation):
        return self.connections.table_ref(relation.database,
                                          relation.schema,
                                          relation.identifier,
                                          conn)

    def _update_column_dict(self, bq_column_dict, dbt_columns, parent=''):
        """
        Helper function to recursively traverse the schema of a table in the
        update_column_descriptions function below.

        bq_column_dict should be a dict as obtained by the to_api_repr()
        function of a SchemaField object.
        """
        if parent:
            dotted_column_name = '{}.{}'.format(parent, bq_column_dict['name'])
        else:
            dotted_column_name = bq_column_dict['name']

        if dotted_column_name in dbt_columns:
            column_config = dbt_columns[dotted_column_name]
            bq_column_dict['description'] = column_config.get('description')
            if column_config.get('policy_tags'):
                bq_column_dict['policyTags'] = {
                    'names': column_config.get('policy_tags')
                }

        new_fields = []
        for child_col_dict in bq_column_dict.get('fields', list()):
            new_child_column_dict = self._update_column_dict(
                child_col_dict,
                dbt_columns,
                parent=dotted_column_name
            )
            new_fields.append(new_child_column_dict)

        bq_column_dict['fields'] = new_fields

        return bq_column_dict

    @available.parse_none
    def update_columns(self, relation, columns):
        if len(columns) == 0:
            return

        conn = self.connections.get_thread_connection()
        table_ref = self.get_table_ref_from_relation(conn, relation)
        table = conn.handle.get_table(table_ref)

        new_schema = []
        for bq_column in table.schema:
            bq_column_dict = bq_column.to_api_repr()
            new_bq_column_dict = self._update_column_dict(
                bq_column_dict,
                columns
            )
            new_schema.append(SchemaField.from_api_repr(new_bq_column_dict))

        new_table = google.cloud.bigquery.Table(table_ref, schema=new_schema)
        conn.handle.update_table(new_table, ['schema'])

    @available.parse_none
    def update_table_description(
        self, database: str, schema: str, identifier: str, description: str
    ):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        table_ref = self.connections.table_ref(
            database,
            schema,
            identifier,
            conn
        )
        table = client.get_table(table_ref)
        table.description = description
        client.update_table(table, ['description'])

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

    def _get_catalog_schemas(self, manifest: Manifest) -> SchemaSearchMap:
        candidates = super()._get_catalog_schemas(manifest)
        db_schemas: Dict[str, Set[str]] = {}
        result = SchemaSearchMap()

        for candidate, schemas in candidates.items():
            database = candidate.database
            if database not in db_schemas:
                db_schemas[database] = set(self.list_schemas(database))
            if candidate.schema in db_schemas[database]:
                result[candidate] = schemas
            else:
                logger.debug(
                    'Skipping catalog for {}.{} - schema does not exist'
                    .format(database, candidate.schema)
                )
        return result

    @available.parse(lambda *a, **k: {})
    def get_table_options(
        self, config: Dict[str, Any], node: Dict[str, Any], temporary: bool
    ) -> Dict[str, Any]:
        opts = {}
        if temporary:
            expiration = 'TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)'
            opts['expiration_timestamp'] = expiration

        if (config.get('hours_to_expiration') is not None) and (not temporary):
            expiration = (
                'TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL '
                '{} hour)').format(config.get('hours_to_expiration'))
            opts['expiration_timestamp'] = expiration

        if config.persist_relation_docs() and 'description' in node:
            description = sql_escape(node['description'])
            opts['description'] = '"""{}"""'.format(description)

        if config.get('kms_key_name') is not None:
            opts['kms_key_name'] = "'{}'".format(config.get('kms_key_name'))

        if config.get('labels'):
            labels = config.get('labels', {})
            opts['labels'] = list(labels.items())

        return opts

    @available.parse_none
    def grant_access_to(self, entity, entity_type, role, grant_target_dict):
        """
        Given an entity, grants it access to a permissioned dataset.
        """
        conn = self.connections.get_thread_connection()
        client = conn.handle

        grant_target = GrantTarget.from_dict(grant_target_dict)
        dataset = client.get_dataset(
            self.connections.dataset_from_id(grant_target.render())
        )

        if entity_type == 'view':
            entity = self.connections.table_ref(
                entity.database,
                entity.schema,
                entity.identifier,
                conn).to_api_repr()

        access_entry = AccessEntry(role, entity_type, entity)
        access_entries = dataset.access_entries

        if access_entry in access_entries:
            logger.debug(f"Access entry {access_entry} "
                         f"already exists in dataset")
            return

        access_entries.append(AccessEntry(role, entity_type, entity))
        dataset.access_entries = access_entries
        client.update_dataset(dataset, ['access_entries'])

    def get_rows_different_sql(
        self,
        relation_a: BigQueryRelation,
        relation_b: BigQueryRelation,
        column_names: Optional[List[str]] = None,
        except_operator='EXCEPT DISTINCT'
    ) -> str:
        return super().get_rows_different_sql(
            relation_a=relation_a,
            relation_b=relation_b,
            column_names=column_names,
            except_operator=except_operator,
        )
