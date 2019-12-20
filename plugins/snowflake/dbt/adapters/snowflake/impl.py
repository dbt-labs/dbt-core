from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Mapping, Any, Optional, Set

import agate

from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.snowflake import SnowflakeConnectionManager
from dbt.adapters.snowflake import SnowflakeRelation
from dbt.adapters.snowflake import SnowflakeColumn
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import RuntimeException
from dbt.utils import filter_null_values


GET_CATALOG_MACRO_NAME = 'snowflake_get_catalog'


class SnowflakeAdapter(SQLAdapter):
    Relation = SnowflakeRelation
    Column = SnowflakeColumn
    ConnectionManager = SnowflakeConnectionManager

    AdapterSpecificConfigs = frozenset(
        {"transient", "cluster_by", "automatic_clustering", "secure",
         "copy_grants", "snowflake_warehouse"}
    )

    @classmethod
    def date_function(cls):
        return "CURRENT_TIMESTAMP()"

    @classmethod
    def _catalog_filter_table(cls, table, manifest):
        # On snowflake, users can set QUOTED_IDENTIFIERS_IGNORE_CASE, so force
        # the column names to their lowercased forms.
        lowered = table.rename(
            column_names=[c.lower() for c in table.column_names]
        )
        return super()._catalog_filter_table(lowered, manifest)

    def _make_match_kwargs(self, database, schema, identifier):
        quoting = self.config.quoting
        if identifier is not None and quoting["identifier"] is False:
            identifier = identifier.upper()

        if schema is not None and quoting["schema"] is False:
            schema = schema.upper()

        if database is not None and quoting["database"] is False:
            database = database.upper()

        return filter_null_values(
            {"identifier": identifier, "schema": schema, "database": database}
        )

    def _get_warehouse(self) -> str:
        _, table = self.execute(
            'select current_warehouse() as warehouse',
            fetch=True
        )
        if len(table) == 0 or len(table[0]) == 0:
            # can this happen?
            raise RuntimeException(
                'Could not get current warehouse: no results'
            )
        return str(table[0][0])

    def _use_warehouse(self, warehouse: str):
        """Use the given warehouse. Quotes are never applied."""
        self.execute('use warehouse {}'.format(warehouse))

    def pre_model_hook(self, config: Mapping[str, Any]) -> Optional[str]:
        default_warehouse = self.config.credentials.warehouse
        warehouse = config.get('snowflake_warehouse', default_warehouse)
        if warehouse == default_warehouse or warehouse is None:
            return None
        previous = self._get_warehouse()
        self._use_warehouse(warehouse)
        return previous

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:

        name = '.'.join([
            str(information_schema.database),
            'information_schema'
        ])

        # calculate the possible schemas for a given schema name
        all_schema_names: Set[str] = set()
        for schema in schemas:
            all_schema_names.update({schema, schema.lower(), schema.upper()})

        with self.connection_named(name):
            kwargs = {
                'information_schema': information_schema,
                'schemas': all_schema_names
            }
            table = self.execute_macro(GET_CATALOG_MACRO_NAME,
                                       kwargs=kwargs,
                                       release=True)

        results = self._catalog_filter_table(table, manifest)
        return results

    def get_catalog(self, manifest: Manifest) -> agate.Table:
        # snowflake is super slow. split it out into the specified threads
        num_threads = self.config.threads
        schema_map = self._get_cache_schemas(manifest)
        catalogs: agate.Table = agate.Table(rows=[])

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(self._get_one_catalog, info, schemas, manifest)
                for info, schemas in schema_map.items() if len(schemas) > 0
            ]
            for future in as_completed(futures):
                catalog = future.result()
                catalogs = agate.Table.merge([catalogs, catalog])

        return catalogs

    def post_model_hook(
        self, config: Mapping[str, Any], context: Optional[str]
    ) -> None:
        if context is not None:
            self._use_warehouse(context)
