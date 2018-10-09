import psycopg2

import time

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.postgres import PostgresConnectionManager
import dbt.compat
import dbt.exceptions
import agate

from dbt.logger import GLOBAL_LOGGER as logger


GET_RELATIONS_OPERATION_NAME = 'get_relations_data'


class PostgresAdapter(SQLAdapter):
    ConnectionManager = PostgresConnectionManager

    @classmethod
    def date_function(cls):
        return 'datenow()'

    def _link_cached_relations(self, manifest):
        schemas = manifest.get_used_schemas()
        try:
            table = self.run_operation(manifest, GET_RELATIONS_OPERATION_NAME)
        finally:
            self.release_connection(GET_RELATIONS_OPERATION_NAME)
        table = self._relations_filter_table(table, schemas)

        for (refed_schema, refed_name, dep_schema, dep_name) in table:
            referenced = self.Relation.create(schema=refed_schema,
                                              identifier=refed_name)
            dependent = self.Relation.create(schema=dep_schema,
                                             identifier=dep_name)
            self.cache.add_link(dependent, referenced)

    def _relations_cache_for_schemas(self, manifest):
        super(PostgresAdapter, self)._relations_cache_for_schemas(manifest)
        self._link_cached_relations(manifest)

    def list_relations_without_caching(self, schema, model_name=None):
        sql = """
        select tablename as name, schemaname as schema, 'table' as type from pg_tables
        where schemaname ilike '{schema}'
        union all
        select viewname as name, schemaname as schema, 'view' as type from pg_views
        where schemaname ilike '{schema}'
        """.format(schema=schema).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)

        results = cursor.fetchall()

        return [self.Relation.create(
            database=self.config.credentials.dbname,
            schema=_schema,
            identifier=name,
            quote_policy={
                'schema': True,
                'identifier': True
            },
            type=type)
                for (name, _schema, type) in results]

    def get_existing_schemas(self, model_name=None):
        sql = "select distinct nspname from pg_namespace"

        connection, cursor = self.add_query(sql, model_name, auto_begin=False)
        results = cursor.fetchall()

        return [row[0] for row in results]

    def check_schema_exists(self, schema, model_name=None):
        sql = """
        select count(*) from pg_namespace where nspname = '{schema}'
        """.format(schema=schema).strip()  # noqa

        connection, cursor = self.add_query(sql, model_name,
                                            auto_begin=False)
        results = cursor.fetchone()

        return results[0] > 0

    @classmethod
    def get_columns_in_relation_sql(cls, relation):
        schema_filter = '1=1'
        if relation.schema:
            schema_filter = "table_schema = '{}'".format(relation.schema)

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
        where table_name = '{table_name}'
          and {schema_filter}
        order by ordinal_position
        """.format(db_prefix=db_prefix,
                   table_name=relation.identifier,
                   schema_filter=schema_filter).strip()

        return sql
