import abc
from concurrent.futures import as_completed, Future
from contextlib import contextmanager
from datetime import datetime
from itertools import chain
from typing import (
    Optional,
    Tuple,
    Callable,
    Iterable,
    Type,
    Dict,
    Any,
    List,
    Mapping,
    Iterator,
    Union,
    Set,
)

import agate
import pytz

from dbt.exceptions import (
    raise_database_error,
    raise_compiler_error,
    invalid_type_error,
    get_relation_returned_multiple_results,
    InternalException,
    NotImplementedException,
    RuntimeException,
)

from dbt.adapters.protocol import (
    AdapterConfig,
    ConnectionManagerProtocol,
)
from dbt.clients.agate_helper import empty_table, merge_tables, table_from_rows
from dbt.clients.jinja import MacroGenerator
from dbt.contracts.graph.compiled import CompileResultNode, CompiledSeedNode
from dbt.contracts.graph.manifest import Manifest, MacroManifest
from dbt.contracts.graph.parsed import ParsedSeedNode
from dbt.exceptions import warn_or_error
from dbt.events.functions import fire_event
from dbt.events.types import CacheMiss, ListRelations
from dbt.utils import filter_null_values, executor

from dbt.adapters.base.connections import Connection, AdapterResponse
from dbt.adapters.base.meta import AdapterMeta, available
from dbt.adapters.base.relation import (
    ComponentName,
    BaseRelation,
    InformationSchema,
    SchemaSearchMap,
)
from dbt.adapters.base import Column as BaseColumn
from dbt.adapters.cache import RelationsCache, _make_key


SeedModel = Union[ParsedSeedNode, CompiledSeedNode]


GET_CATALOG_MACRO_NAME = "get_catalog"
FRESHNESS_MACRO_NAME = "collect_freshness"


def _expect_row_value(key: str, row: agate.Row):
    if key not in row.keys():
        raise InternalException(
            'Got a row without "{}" column, columns: {}'.format(key, row.keys())
        )
    return row[key]


def _catalog_filter_schemas(manifest: Manifest) -> Callable[[agate.Row], bool]:
    """Return a function that takes a row and decides if the row should be
    included in the catalog output.
    """
    schemas = frozenset((d.lower(), s.lower()) for d, s in manifest.get_used_schemas())

    def test(row: agate.Row) -> bool:
        table_database = _expect_row_value("table_database", row)
        table_schema = _expect_row_value("table_schema", row)
        # the schema may be present but None, which is not an error and should
        # be filtered out
        if table_schema is None:
            return False
        return (table_database.lower(), table_schema.lower()) in schemas

    return test


def _utc(dt: Optional[datetime], source: BaseRelation, field_name: str) -> datetime:
    """If dt has a timezone, return a new datetime that's in UTC. Otherwise,
    assume the datetime is already for UTC and add the timezone.
    """
    if dt is None:
        raise raise_database_error(
            "Expected a non-null value when querying field '{}' of table "
            " {} but received value 'null' instead".format(field_name, source)
        )

    elif not hasattr(dt, "tzinfo"):
        raise raise_database_error(
            "Expected a timestamp value when querying field '{}' of table "
            "{} but received value of type '{}' instead".format(
                field_name, source, type(dt).__name__
            )
        )

    elif dt.tzinfo:
        return dt.astimezone(pytz.UTC)
    else:
        return dt.replace(tzinfo=pytz.UTC)


def _relation_name(rel: Optional[BaseRelation]) -> str:
    if rel is None:
        return "null relation"
    else:
        return str(rel)


class BaseAdapter(metaclass=AdapterMeta):
    """The BaseAdapter provides an abstract base class for adapters.

    Adapters must implement the following methods and macros. Some of the
    methods can be safely overridden as a noop, where it makes sense
    (transactions on databases that don't support them, for instance). Those
    methods are marked with a (passable) in their docstrings. Check docstrings
    for type information, etc.

    To implement a macro, implement "${adapter_type}__${macro_name}" in the
    adapter's internal project.

    To invoke a method in an adapter macro, call it on the 'adapter' Jinja
    object using dot syntax.

    To invoke a method in model code, add the @available decorator atop a method
    declaration. Methods are invoked as macros.

    Methods:
        - exception_handler
        - date_function
        - list_schemas
        - drop_relation
        - truncate_relation
        - rename_relation
        - get_columns_in_relation
        - expand_column_types
        - list_relations_without_caching
        - is_cancelable
        - create_schema
        - drop_schema
        - quote
        - convert_text_type
        - convert_number_type
        - convert_boolean_type
        - convert_datetime_type
        - convert_date_type
        - convert_time_type

    Macros:
        - get_catalog
    """

    Relation: Type[BaseRelation] = BaseRelation
    Column: Type[BaseColumn] = BaseColumn
    ConnectionManager: Type[ConnectionManagerProtocol]

    # A set of clobber config fields accepted by this adapter
    # for use in materializations
    AdapterSpecificConfigs: Type[AdapterConfig] = AdapterConfig

    def __init__(self, config):
        self.config = config
        self.cache = RelationsCache()
        self.connections = self.ConnectionManager(config)
        self._macro_manifest_lazy: Optional[MacroManifest] = None

    ###
    # Methods that pass through to the connection manager
    ###
    def acquire_connection(self, name=None) -> Connection:
        return self.connections.set_connection_name(name)

    def release_connection(self) -> None:
        self.connections.release()

    def cleanup_connections(self) -> None:
        self.connections.cleanup_all()

    def clear_transaction(self) -> None:
        self.connections.clear_transaction()

    def commit_if_has_connection(self) -> None:
        self.connections.commit_if_has_connection()

    def debug_query(self) -> None:
        self.execute("select 1 as id")

    def nice_connection_name(self) -> str:
        conn = self.connections.get_if_exists()
        if conn is None or conn.name is None:
            return "<None>"
        return conn.name

    @contextmanager
    def connection_named(
        self, name: str, node: Optional[CompileResultNode] = None
    ) -> Iterator[None]:
        try:
            if self.connections.query_header is not None:
                self.connections.query_header.set(name, node)
            self.acquire_connection(name)
            yield
        finally:
            self.release_connection()
            if self.connections.query_header is not None:
                self.connections.query_header.reset()

    @contextmanager
    def connection_for(self, node: CompileResultNode) -> Iterator[None]:
        with self.connection_named(node.unique_id, node):
            yield

    @available.parse(lambda *a, **k: ("", empty_table()))
    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, agate.Table]:
        """Execute the given SQL. This is a thin wrapper around
        ConnectionManager.execute.

        :param str sql: The sql to execute.
        :param bool auto_begin: If set, and dbt is not currently inside a
            transaction, automatically begin one.
        :param bool fetch: If set, fetch results.
        :return: A tuple of the query status and results (empty if fetch=False).
        :rtype: Tuple[AdapterResponse, agate.Table]
        """
        return self.connections.execute(sql=sql, auto_begin=auto_begin, fetch=fetch)

    @available.parse(lambda *a, **k: ("", empty_table()))
    def get_partitions_metadata(self, table: str) -> Tuple[agate.Table]:
        """Obtain partitions metadata for a BigQuery partitioned table.

        :param str table_id: a partitioned table id, in standard SQL format.
        :return: a partition metadata tuple, as described in
            https://cloud.google.com/bigquery/docs/creating-partitioned-tables#getting_partition_metadata_using_meta_tables.
        :rtype: agate.Table
        """
        return self.connections.get_partitions_metadata(table=table)

    ###
    # Methods that should never be overridden
    ###
    @classmethod
    def type(cls) -> str:
        """Get the type of this adapter. Types must be class-unique and
        consistent.

        :return: The type name
        :rtype: str
        """
        return cls.ConnectionManager.TYPE

    @property
    def _macro_manifest(self) -> MacroManifest:
        if self._macro_manifest_lazy is None:
            return self.load_macro_manifest()
        return self._macro_manifest_lazy

    def check_macro_manifest(self) -> Optional[MacroManifest]:
        """Return the internal manifest (used for executing macros) if it's
        been initialized, otherwise return None.
        """
        return self._macro_manifest_lazy

    def load_macro_manifest(self, base_macros_only=False) -> MacroManifest:
        # base_macros_only is for the test framework
        if self._macro_manifest_lazy is None:
            # avoid a circular import
            from dbt.parser.manifest import ManifestLoader

            manifest = ManifestLoader.load_macros(
                self.config, self.connections.set_query_header, base_macros_only=base_macros_only
            )
            # TODO CT-211
            self._macro_manifest_lazy = manifest  # type: ignore[assignment]
        # TODO CT-211
        return self._macro_manifest_lazy  # type: ignore[return-value]

    def clear_macro_manifest(self):
        if self._macro_manifest_lazy is not None:
            self._macro_manifest_lazy = None

    ###
    # Caching methods
    ###
    def _schema_is_cached(self, database: Optional[str], schema: str) -> bool:
        """Check if the schema is cached, and by default logs if it is not."""

        if (database, schema) not in self.cache:
            fire_event(
                CacheMiss(conn_name=self.nice_connection_name(), database=database, schema=schema)
            )
            return False
        else:
            return True

    def _get_cache_schemas(self, manifest: Manifest) -> Set[BaseRelation]:
        """Get the set of schema relations that the cache logic needs to
        populate. This means only executable nodes are included.
        """
        # the cache only cares about executable nodes
        return {
            self.Relation.create_from(self.config, node).without_identifier()
            for node in manifest.nodes.values()
            if (node.is_relational and not node.is_ephemeral_model)
        }

    def _get_catalog_schemas(self, manifest: Manifest) -> SchemaSearchMap:
        """Get a mapping of each node's "information_schema" relations to a
        set of all schemas expected in that information_schema.

        There may be keys that are technically duplicates on the database side,
        for example all of '"foo", 'foo', '"FOO"' and 'FOO' could coexist as
        databases, and values could overlap as appropriate. All values are
        lowercase strings.
        """
        info_schema_name_map = SchemaSearchMap()
        nodes: Iterator[CompileResultNode] = chain(
            [
                node
                for node in manifest.nodes.values()
                if (node.is_relational and not node.is_ephemeral_model)
            ],
            manifest.sources.values(),
        )
        for node in nodes:
            relation = self.Relation.create_from(self.config, node)
            info_schema_name_map.add(relation)
        # result is a map whose keys are information_schema Relations without
        # identifiers that have appropriate database prefixes, and whose values
        # are sets of lowercase schema names that are valid members of those
        # databases
        return info_schema_name_map

    def _relations_cache_for_schemas(
        self, manifest: Manifest, cache_schemas: Set[BaseRelation] = None
    ) -> None:
        """Populate the relations cache for the given schemas. Returns an
        iterable of the schemas populated, as strings.
        """
        if not cache_schemas:
            cache_schemas = self._get_cache_schemas(manifest)
        with executor(self.config) as tpe:
            futures: List[Future[List[BaseRelation]]] = []
            for cache_schema in cache_schemas:
                fut = tpe.submit_connected(
                    self,
                    f"list_{cache_schema.database}_{cache_schema.schema}",
                    self.list_relations_without_caching,
                    cache_schema,
                )
                futures.append(fut)

            for future in as_completed(futures):
                # if we can't read the relations we need to just raise anyway,
                # so just call future.result() and let that raise on failure
                for relation in future.result():
                    self.cache.add(relation)

        # it's possible that there were no relations in some schemas. We want
        # to insert the schemas we query into the cache's `.schemas` attribute
        # so we can check it later
        cache_update: Set[Tuple[Optional[str], Optional[str]]] = set()
        for relation in cache_schemas:
            cache_update.add((relation.database, relation.schema))
        self.cache.update_schemas(cache_update)

    def set_relations_cache(
        self, manifest: Manifest, clear: bool = False, required_schemas: Set[BaseRelation] = None
    ) -> None:
        """Run a query that gets a populated cache of the relations in the
        database and set the cache on this adapter.
        """
        with self.cache.lock:
            if clear:
                self.cache.clear()
            self._relations_cache_for_schemas(manifest, required_schemas)

    @available
    def cache_added(self, relation: Optional[BaseRelation]) -> str:
        """Cache a new relation in dbt. It will show up in `list relations`."""
        if relation is None:
            name = self.nice_connection_name()
            raise_compiler_error("Attempted to cache a null relation for {}".format(name))
        self.cache.add(relation)
        # so jinja doesn't render things
        return ""

    @available
    def cache_dropped(self, relation: Optional[BaseRelation]) -> str:
        """Drop a relation in dbt. It will no longer show up in
        `list relations`, and any bound views will be dropped from the cache
        """
        if relation is None:
            name = self.nice_connection_name()
            raise_compiler_error("Attempted to drop a null relation for {}".format(name))
        self.cache.drop(relation)
        return ""

    @available
    def cache_renamed(
        self,
        from_relation: Optional[BaseRelation],
        to_relation: Optional[BaseRelation],
    ) -> str:
        """Rename a relation in dbt. It will show up with a new name in
        `list_relations`, but bound views will remain bound.
        """
        if from_relation is None or to_relation is None:
            name = self.nice_connection_name()
            src_name = _relation_name(from_relation)
            dst_name = _relation_name(to_relation)
            raise_compiler_error(
                "Attempted to rename {} to {} for {}".format(src_name, dst_name, name)
            )

        self.cache.rename(from_relation, to_relation)
        return ""

    ###
    # Abstract methods for database-specific values, attributes, and types
    ###
    @abc.abstractclassmethod
    def date_function(cls) -> str:
        """Get the date function used by this adapter's database."""
        raise NotImplementedException("`date_function` is not implemented for this adapter!")

    @abc.abstractclassmethod
    def is_cancelable(cls) -> bool:
        raise NotImplementedException("`is_cancelable` is not implemented for this adapter!")

    ###
    # Abstract methods about schemas
    ###
    @abc.abstractmethod
    def list_schemas(self, database: str) -> List[str]:
        """Get a list of existing schemas in database"""
        raise NotImplementedException("`list_schemas` is not implemented for this adapter!")

    @available.parse(lambda *a, **k: False)
    def check_schema_exists(self, database: str, schema: str) -> bool:
        """Check if a schema exists.

        The default implementation of this is potentially unnecessarily slow,
        and adapters should implement it if there is an optimized path (and
        there probably is)
        """
        search = (s.lower() for s in self.list_schemas(database=database))
        return schema.lower() in search

    ###
    # Abstract methods about relations
    ###
    @abc.abstractmethod
    @available.parse_none
    def drop_relation(self, relation: BaseRelation) -> None:
        """Drop the given relation.

        *Implementors must call self.cache.drop() to preserve cache state!*
        """
        raise NotImplementedException("`drop_relation` is not implemented for this adapter!")

    @abc.abstractmethod
    @available.parse_none
    def truncate_relation(self, relation: BaseRelation) -> None:
        """Truncate the given relation."""
        raise NotImplementedException("`truncate_relation` is not implemented for this adapter!")

    @abc.abstractmethod
    @available.parse_none
    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        """Rename the relation from from_relation to to_relation.

        Implementors must call self.cache.rename() to preserve cache state.
        """
        raise NotImplementedException("`rename_relation` is not implemented for this adapter!")

    @abc.abstractmethod
    @available.parse_list
    def get_columns_in_relation(self, relation: BaseRelation) -> List[BaseColumn]:
        """Get a list of the columns in the given Relation."""
        raise NotImplementedException(
            "`get_columns_in_relation` is not implemented for this adapter!"
        )

    @available.deprecated("get_columns_in_relation", lambda *a, **k: [])
    def get_columns_in_table(self, schema: str, identifier: str) -> List[BaseColumn]:
        """DEPRECATED: Get a list of the columns in the given table."""
        relation = self.Relation.create(
            database=self.config.credentials.database,
            schema=schema,
            identifier=identifier,
            quote_policy=self.config.quoting,
        )
        return self.get_columns_in_relation(relation)

    @abc.abstractmethod
    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        """Expand the current table's types to match the goal table. (passable)

        :param self.Relation goal: A relation that currently exists in the
            database with columns of the desired types.
        :param self.Relation current: A relation that currently exists in the
            database with columns of unspecified types.
        """
        raise NotImplementedException(
            "`expand_target_column_types` is not implemented for this adapter!"
        )

    @abc.abstractmethod
    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        """List relations in the given schema, bypassing the cache.

        This is used as the underlying behavior to fill the cache.

        :param schema_relation: A relation containing the database and schema
            as appropraite for the underlying data warehouse
        :return: The relations in schema
        :rtype: List[self.Relation]
        """
        raise NotImplementedException(
            "`list_relations_without_caching` is not implemented for this " "adapter!"
        )

    ###
    # Provided methods about relations
    ###
    @available.parse_list
    def get_missing_columns(
        self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> List[BaseColumn]:
        """Returns a list of Columns in from_relation that are missing from
        to_relation.
        """
        if not isinstance(from_relation, self.Relation):
            invalid_type_error(
                method_name="get_missing_columns",
                arg_name="from_relation",
                got_value=from_relation,
                expected_type=self.Relation,
            )

        if not isinstance(to_relation, self.Relation):
            invalid_type_error(
                method_name="get_missing_columns",
                arg_name="to_relation",
                got_value=to_relation,
                expected_type=self.Relation,
            )

        from_columns = {col.name: col for col in self.get_columns_in_relation(from_relation)}

        to_columns = {col.name: col for col in self.get_columns_in_relation(to_relation)}

        missing_columns = set(from_columns.keys()) - set(to_columns.keys())

        return [col for (col_name, col) in from_columns.items() if col_name in missing_columns]

    @available.parse_none
    def valid_snapshot_target(self, relation: BaseRelation) -> None:
        """Ensure that the target relation is valid, by making sure it has the
        expected columns.

        :param Relation relation: The relation to check
        :raises CompilationException: If the columns are
            incorrect.
        """
        if not isinstance(relation, self.Relation):
            invalid_type_error(
                method_name="valid_snapshot_target",
                arg_name="relation",
                got_value=relation,
                expected_type=self.Relation,
            )

        columns = self.get_columns_in_relation(relation)
        names = set(c.name.lower() for c in columns)
        expanded_keys = ("scd_id", "valid_from", "valid_to")
        extra = []
        missing = []
        for legacy in expanded_keys:
            desired = "dbt_" + legacy
            if desired not in names:
                missing.append(desired)
                if legacy in names:
                    extra.append(legacy)

        if missing:
            if extra:
                msg = (
                    'Snapshot target has ("{}") but not ("{}") - is it an '
                    "unmigrated previous version archive?".format(
                        '", "'.join(extra), '", "'.join(missing)
                    )
                )
            else:
                msg = 'Snapshot target is not a snapshot table (missing "{}")'.format(
                    '", "'.join(missing)
                )
            raise_compiler_error(msg)

    @available.parse_none
    def expand_target_column_types(
        self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> None:
        if not isinstance(from_relation, self.Relation):
            invalid_type_error(
                method_name="expand_target_column_types",
                arg_name="from_relation",
                got_value=from_relation,
                expected_type=self.Relation,
            )

        if not isinstance(to_relation, self.Relation):
            invalid_type_error(
                method_name="expand_target_column_types",
                arg_name="to_relation",
                got_value=to_relation,
                expected_type=self.Relation,
            )

        self.expand_column_types(from_relation, to_relation)

    def list_relations(self, database: Optional[str], schema: str) -> List[BaseRelation]:
        if self._schema_is_cached(database, schema):
            return self.cache.get_relations(database, schema)

        schema_relation = self.Relation.create(
            database=database, schema=schema, identifier="", quote_policy=self.config.quoting
        ).without_identifier()

        # we can't build the relations cache because we don't have a
        # manifest so we can't run any operations.
        relations = self.list_relations_without_caching(schema_relation)
        fire_event(
            ListRelations(
                database=database, schema=schema, relations=[_make_key(x) for x in relations]
            )
        )

        return relations

    def _make_match_kwargs(self, database: str, schema: str, identifier: str) -> Dict[str, str]:
        quoting = self.config.quoting
        if identifier is not None and quoting["identifier"] is False:
            identifier = identifier.lower()

        if schema is not None and quoting["schema"] is False:
            schema = schema.lower()

        if database is not None and quoting["database"] is False:
            database = database.lower()

        return filter_null_values(
            {
                "database": database,
                "identifier": identifier,
                "schema": schema,
            }
        )

    def _make_match(
        self,
        relations_list: List[BaseRelation],
        database: str,
        schema: str,
        identifier: str,
    ) -> List[BaseRelation]:

        matches = []

        search = self._make_match_kwargs(database, schema, identifier)

        for relation in relations_list:
            if relation.matches(**search):
                matches.append(relation)

        return matches

    @available.parse_none
    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        relations_list = self.list_relations(database, schema)

        matches = self._make_match(relations_list, database, schema, identifier)

        if len(matches) > 1:
            kwargs = {
                "identifier": identifier,
                "schema": schema,
                "database": database,
            }
            get_relation_returned_multiple_results(kwargs, matches)

        elif matches:
            return matches[0]

        return None

    @available.deprecated("get_relation", lambda *a, **k: False)
    def already_exists(self, schema: str, name: str) -> bool:
        """DEPRECATED: Return if a model already exists in the database"""
        database = self.config.credentials.database
        relation = self.get_relation(database, schema, name)
        return relation is not None

    ###
    # ODBC FUNCTIONS -- these should not need to change for every adapter,
    #                   although some adapters may override them
    ###
    @abc.abstractmethod
    @available.parse_none
    def create_schema(self, relation: BaseRelation):
        """Create the given schema if it does not exist."""
        raise NotImplementedException("`create_schema` is not implemented for this adapter!")

    @abc.abstractmethod
    @available.parse_none
    def drop_schema(self, relation: BaseRelation):
        """Drop the given schema (and everything in it) if it exists."""
        raise NotImplementedException("`drop_schema` is not implemented for this adapter!")

    @available
    @abc.abstractclassmethod
    def quote(cls, identifier: str) -> str:
        """Quote the given identifier, as appropriate for the database."""
        raise NotImplementedException("`quote` is not implemented for this adapter!")

    @available
    def quote_as_configured(self, identifier: str, quote_key: str) -> str:
        """Quote or do not quote the given identifer as configured in the
        project config for the quote key.

        The quote key should be one of 'database' (on bigquery, 'profile'),
        'identifier', or 'schema', or it will be treated as if you set `True`.
        """
        try:
            key = ComponentName(quote_key)
        except ValueError:
            return identifier

        default = self.Relation.get_default_quote_policy().get_part(key)
        if self.config.quoting.get(key, default):
            return self.quote(identifier)
        else:
            return identifier

    @available
    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:
        quote_columns: bool = True
        if isinstance(quote_config, bool):
            quote_columns = quote_config
        elif quote_config is None:
            pass
        else:
            raise_compiler_error(
                f'The seed configuration value of "quote_columns" has an '
                f"invalid type {type(quote_config)}"
            )

        if quote_columns:
            return self.quote(column)
        else:
            return column

    ###
    # Conversions: These must be implemented by concrete implementations, for
    # converting agate types into their sql equivalents.
    ###
    @abc.abstractclassmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.Text
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedException("`convert_text_type` is not implemented for this adapter!")

    @abc.abstractclassmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.Number
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedException("`convert_number_type` is not implemented for this adapter!")

    @abc.abstractclassmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.Boolean
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedException(
            "`convert_boolean_type` is not implemented for this adapter!"
        )

    @abc.abstractclassmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.DateTime
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedException(
            "`convert_datetime_type` is not implemented for this adapter!"
        )

    @abc.abstractclassmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        """Return the type in the database that best maps to the agate.Date
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedException("`convert_date_type` is not implemented for this adapter!")

    @abc.abstractclassmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        """Return the type in the database that best maps to the
        agate.TimeDelta type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database
        """
        raise NotImplementedException("`convert_time_type` is not implemented for this adapter!")

    @available
    @classmethod
    def convert_type(cls, agate_table: agate.Table, col_idx: int) -> Optional[str]:
        return cls.convert_agate_type(agate_table, col_idx)

    @classmethod
    def convert_agate_type(cls, agate_table: agate.Table, col_idx: int) -> Optional[str]:
        agate_type: Type = agate_table.column_types[col_idx]
        conversions: List[Tuple[Type, Callable[..., str]]] = [
            (agate.Text, cls.convert_text_type),
            (agate.Number, cls.convert_number_type),
            (agate.Boolean, cls.convert_boolean_type),
            (agate.DateTime, cls.convert_datetime_type),
            (agate.Date, cls.convert_date_type),
            (agate.TimeDelta, cls.convert_time_type),
        ]
        for agate_cls, func in conversions:
            if isinstance(agate_type, agate_cls):
                return func(agate_table, col_idx)

        return None

    ###
    # Operations involving the manifest
    ###
    def execute_macro(
        self,
        macro_name: str,
        manifest: Optional[Manifest] = None,
        project: Optional[str] = None,
        context_override: Optional[Dict[str, Any]] = None,
        kwargs: Dict[str, Any] = None,
        text_only_columns: Optional[Iterable[str]] = None,
    ) -> agate.Table:
        """Look macro_name up in the manifest and execute its results.

        :param macro_name: The name of the macro to execute.
        :param manifest: The manifest to use for generating the base macro
            execution context. If none is provided, use the internal manifest.
        :param project: The name of the project to search in, or None for the
            first match.
        :param context_override: An optional dict to update() the macro
            execution context.
        :param kwargs: An optional dict of keyword args used to pass to the
            macro.
        """

        if kwargs is None:
            kwargs = {}
        if context_override is None:
            context_override = {}

        if manifest is None:
            # TODO CT-211
            manifest = self._macro_manifest  # type: ignore[assignment]
        # TODO CT-211
        macro = manifest.find_macro_by_name(  # type: ignore[union-attr]
            macro_name, self.config.project_name, project
        )
        if macro is None:
            if project is None:
                package_name = "any package"
            else:
                package_name = 'the "{}" package'.format(project)

            raise RuntimeException(
                'dbt could not find a macro with the name "{}" in {}'.format(
                    macro_name, package_name
                )
            )
        # This causes a reference cycle, as generate_runtime_macro_context()
        # ends up calling get_adapter, so the import has to be here.
        from dbt.context.providers import generate_runtime_macro_context

        macro_context = generate_runtime_macro_context(
            # TODO CT-211
            macro=macro,
            config=self.config,
            manifest=manifest,  # type: ignore[arg-type]
            package_name=project,
        )
        macro_context.update(context_override)

        macro_function = MacroGenerator(macro, macro_context)

        with self.connections.exception_handler(f"macro {macro_name}"):
            result = macro_function(**kwargs)
        return result

    @classmethod
    def _catalog_filter_table(cls, table: agate.Table, manifest: Manifest) -> agate.Table:
        """Filter the table as appropriate for catalog entries. Subclasses can
        override this to change filtering rules on a per-adapter basis.
        """
        # force database + schema to be strings
        table = table_from_rows(
            table.rows,
            table.column_names,
            text_only_columns=["table_database", "table_schema", "table_name"],
        )
        return table.where(_catalog_filter_schemas(manifest))

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:

        kwargs = {"information_schema": information_schema, "schemas": schemas}
        table = self.execute_macro(
            GET_CATALOG_MACRO_NAME,
            kwargs=kwargs,
            # pass in the full manifest so we get any local project
            # overrides
            manifest=manifest,
        )

        results = self._catalog_filter_table(table, manifest)
        return results

    def get_catalog(self, manifest: Manifest) -> Tuple[agate.Table, List[Exception]]:
        schema_map = self._get_catalog_schemas(manifest)

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                if len(schemas) == 0:
                    continue
                name = ".".join([str(info.database), "information_schema"])

                fut = tpe.submit_connected(
                    self, name, self._get_one_catalog, info, schemas, manifest
                )
                futures.append(fut)

            catalogs, exceptions = catch_as_completed(futures)

        return catalogs, exceptions

    def cancel_open_connections(self):
        """Cancel all open connections."""
        return self.connections.cancel_open()

    def calculate_freshness(
        self,
        source: BaseRelation,
        loaded_at_field: str,
        filter: Optional[str],
        manifest: Optional[Manifest] = None,
    ) -> Dict[str, Any]:
        """Calculate the freshness of sources in dbt, and return it"""
        kwargs: Dict[str, Any] = {
            "source": source,
            "loaded_at_field": loaded_at_field,
            "filter": filter,
        }

        # run the macro
        table = self.execute_macro(FRESHNESS_MACRO_NAME, kwargs=kwargs, manifest=manifest)
        # now we have a 1-row table of the maximum `loaded_at_field` value and
        # the current time according to the db.
        if len(table) != 1 or len(table[0]) != 2:
            raise_compiler_error(
                'Got an invalid result from "{}" macro: {}'.format(
                    FRESHNESS_MACRO_NAME, [tuple(r) for r in table]
                )
            )
        if table[0][0] is None:
            # no records in the table, so really the max_loaded_at was
            # infinitely long ago. Just call it 0:00 January 1 year UTC
            max_loaded_at = datetime(1, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
        else:
            max_loaded_at = _utc(table[0][0], source, loaded_at_field)

        snapshotted_at = _utc(table[0][1], source, loaded_at_field)
        age = (snapshotted_at - max_loaded_at).total_seconds()
        return {
            "max_loaded_at": max_loaded_at,
            "snapshotted_at": snapshotted_at,
            "age": age,
        }

    def pre_model_hook(self, config: Mapping[str, Any]) -> Any:
        """A hook for running some operation before the model materialization
        runs. The hook can assume it has a connection available.

        The only parameter is a configuration dictionary (the same one
        available in the materialization context). It should be considered
        read-only.

        The pre-model hook may return anything as a context, which will be
        passed to the post-model hook.
        """
        pass

    def post_model_hook(self, config: Mapping[str, Any], context: Any) -> None:
        """A hook for running some operation after the model materialization
        runs. The hook can assume it has a connection available.

        The first parameter is a configuration dictionary (the same one
        available in the materialization context). It should be considered
        read-only.

        The second parameter is the value returned by pre_mdoel_hook.
        """
        pass

    def get_compiler(self):
        from dbt.compilation import Compiler

        return Compiler(self.config)

    # Methods used in adapter tests
    def update_column_sql(
        self,
        dst_name: str,
        dst_column: str,
        clause: str,
        where_clause: Optional[str] = None,
    ) -> str:
        clause = f"update {dst_name} set {dst_column} = {clause}"
        if where_clause is not None:
            clause += f" where {where_clause}"
        return clause

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        # for backwards compatibility, we're compelled to set some sort of
        # default. A lot of searching has lead me to believe that the
        # '+ interval' syntax used in postgres/redshift is relatively common
        # and might even be the SQL standard's intention.
        return f"{add_to} + interval '{number} {interval}'"

    def string_add_sql(
        self,
        add_to: str,
        value: str,
        location="append",
    ) -> str:
        if location == "append":
            return f"{add_to} || '{value}'"
        elif location == "prepend":
            return f"'{value}' || {add_to}"
        else:
            raise RuntimeException(f'Got an unexpected location value of "{location}"')

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql


COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} {except_op}
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} {except_op}
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a, table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count using (id)
""".strip()


def catch_as_completed(
    futures,  # typing: List[Future[agate.Table]]
) -> Tuple[agate.Table, List[Exception]]:

    # catalogs: agate.Table = agate.Table(rows=[])
    tables: List[agate.Table] = []
    exceptions: List[Exception] = []

    for future in as_completed(futures):
        exc = future.exception()
        # we want to re-raise on ctrl+c and BaseException
        if exc is None:
            catalog = future.result()
            tables.append(catalog)
        elif isinstance(exc, KeyboardInterrupt) or not isinstance(exc, Exception):
            raise exc
        else:
            warn_or_error(f"Encountered an error while generating catalog: {str(exc)}")
            # exc is not None, derives from Exception, and isn't ctrl+c
            exceptions.append(exc)
    return merge_tables(tables), exceptions
