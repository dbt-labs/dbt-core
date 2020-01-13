from typing import Mapping, Any, Optional

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.snowflake import SnowflakeConnectionManager
from dbt.adapters.snowflake import SnowflakeRelation
from dbt.adapters.snowflake import SnowflakeColumn
from dbt.utils import filter_null_values
from dbt.exceptions import RuntimeException


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

    def post_model_hook(
        self, config: Mapping[str, Any], context: Optional[str]
    ) -> None:
        if context is not None:
            self._use_warehouse(context)
