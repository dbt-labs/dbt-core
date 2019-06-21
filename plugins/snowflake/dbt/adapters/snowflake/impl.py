from __future__ import absolute_import

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.snowflake import SnowflakeConnectionManager
from dbt.adapters.snowflake import SnowflakeRelation
from dbt.utils import filter_null_values


class SnowflakeAdapter(SQLAdapter):
    Relation = SnowflakeRelation
    ConnectionManager = SnowflakeConnectionManager

    AdapterSpecificConfigs = frozenset({"transient"})

    @classmethod
    def date_function(cls):
        return 'CURRENT_TIMESTAMP()'

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
        if identifier is not None and quoting['identifier'] is False:
            identifier = identifier.upper()

        if schema is not None and quoting['schema'] is False:
            schema = schema.upper()

        if database is not None and quoting['database'] is False:
            database = database.upper()

        return filter_null_values({'identifier': identifier,
                                   'schema': schema,
                                   'database': database})
