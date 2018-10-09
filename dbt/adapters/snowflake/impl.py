from __future__ import absolute_import

import dbt.compat
import dbt.exceptions

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.snowflake import SnowflakeConnectionManager
from dbt.adapters.snowflake import SnowflakeRelation
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import filter_null_values


class SnowflakeAdapter(SQLAdapter):
    Relation = SnowflakeRelation
    ConnectionManager = SnowflakeConnectionManager

    @classmethod
    def date_function(cls):
        return 'CURRENT_TIMESTAMP()'

    def list_relations_without_caching(self, schema, model_name=None):
        sql = """
        select
          table_name as name, table_schema as schema, table_type as type
        from information_schema.tables
        where table_schema ilike '{schema}'
        """.format(schema=schema).strip()  # noqa

        _, cursor = self.add_query(sql, model_name, auto_begin=False)

        results = cursor.fetchall()

        relation_type_lookup = {
            'BASE TABLE': 'table',
            'VIEW': 'view'

        }
        return [self.Relation.create(
            database=self.config.credentials.database,
            schema=_schema,
            identifier=name,
            quote_policy={
                'identifier': True,
                'schema': True,
            },
            type=relation_type_lookup.get(type))
                for (name, _schema, type) in results]

    def get_existing_schemas(self, model_name=None):
        sql = "select distinct schema_name from information_schema.schemata"

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)
        results = cursor.fetchall()

        return [row[0] for row in results]

    def check_schema_exists(self, schema, model_name=None):
        sql = """
        select count(*)
        from information_schema.schemata
        where upper(schema_name) = upper('{schema}')
        """.format(schema=schema).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)
        results = cursor.fetchone()

        return results[0] > 0

    @classmethod
    def _catalog_filter_table(cls, table, manifest):
        # On snowflake, users can set QUOTED_IDENTIFIERS_IGNORE_CASE, so force
        # the column names to their lowercased forms.
        lowered = table.rename(
            column_names=[c.lower() for c in table.column_names]
        )
        return super(SnowflakeAdapter, cls)._catalog_filter_table(lowered,
                                                                  manifest)

    def _make_match_kwargs(self, schema, identifier):
        quoting = self.config.quoting
        if identifier is not None and quoting['identifier'] is False:
            identifier = identifier.upper()

        if schema is not None and quoting['schema'] is False:
            schema = schema.upper()

        return filter_null_values({'identifier': identifier,
                                   'schema': schema})

    @classmethod
    def get_columns_in_relation_sql(cls, relation):
        schema_filter = '1=1'
        if relation.schema:
            schema_filter = "table_schema ilike '{}'".format(relation.schema)

        db_prefix = ''
        if relation.database:
            db_prefix = '{}.'.format(relation.database)

        sql = """
        select
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision || ',' || numeric_scale as numeric_size

        from {db_prefix}information_schema.columns
        where table_name ilike '{table_name}'
          and {schema_filter}
        order by ordinal_position
        """.format(db_prefix=db_prefix,
                   table_name=relation.identifier,
                   schema_filter=schema_filter).strip()

        return sql
