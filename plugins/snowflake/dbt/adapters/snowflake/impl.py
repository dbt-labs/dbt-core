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

    def list_relations_without_caching(self, database, schema,
                                       model_name=None):
        sql = """
        select
          table_catalog as database, table_name as name,
          table_schema as schema, table_type as type
        from information_schema.tables
        where
            table_schema ilike '{schema}' and
            table_catalog ilike '{database}'
        """.format(database=database, schema=schema).strip()  # noqa

        _, cursor = self.add_query(sql, model_name, auto_begin=False)

        results = cursor.fetchall()

        relation_type_lookup = {
            'BASE TABLE': 'table',
            'VIEW': 'view'

        }
        return [self.Relation.create(
            database=_database,
            schema=_schema,
            identifier=name,
            quote_policy={
                'identifier': True,
                'schema': True,
            },
            type=relation_type_lookup.get(_type))
                for (_database, name, _schema, _type) in results]

    def list_schemas(self, database, model_name=None):
        sql = """
            select distinct schema_name
            from "{database}".information_schema.schemata
            where catalog_name ilike '{database}'
        """.format(database=database).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)
        results = cursor.fetchall()

        return [row[0] for row in results]

    def check_schema_exists(self, database, schema, model_name=None):
        sql = """
        select count(*)
        from information_schema.schemata
        where upper(schema_name) = upper('{schema}')
            and upper(catalog_name) = upper('{database}')
        """.format(database=database, schema=schema).strip()  # noqa

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

    @classmethod
    def get_columns_in_relation_sql(cls, relation):
        source_name = 'information_schema.columns'
        db_filter = '1=1'
        if relation.database:
            db_filter = "table_catalog ilike '{}'".format(relation.database)
            source_name = '{}.{}'.format(relation.database, source_name)

        schema_filter = '1=1'
        if relation.schema:
            schema_filter = "table_schema ilike '{}'".format(relation.schema)

        sql = """
        select
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision || ',' || numeric_scale as numeric_size

        from {source_name}
        where table_name ilike '{table_name}'
          and {schema_filter}
          and {db_filter}
        order by ordinal_position
        """.format(source_name=source_name,
                   table_name=relation.identifier,
                   schema_filter=schema_filter,
                   db_filter=db_filter).strip()

        return sql
